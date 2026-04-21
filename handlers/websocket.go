package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"backend/models"
	"backend/repositories"
	"backend/services"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for development
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// WebSocketHandler manages WebSocket connections and routes incoming messages
// to the appropriate handler. It uses the event-driven Matchmaker for
// instant pairing and supports both text chat and video call signaling.
type WebSocketHandler struct {
	matchmaker *services.Matchmaker
	users      map[string]*models.User
	chatRepo   *repositories.ChatRepository
	mu         sync.Mutex
}

func NewWebSocketHandler() *WebSocketHandler {
	return &WebSocketHandler{
		matchmaker: services.GetMatchmaker(),
		users:      make(map[string]*models.User),
		chatRepo:   repositories.NewChatRepository(),
	}
}

// HandleWebSocket upgrades an HTTP connection to WebSocket, waits for the
// initial find_match message with tags and optional mode, registers the user,
// and enqueues them for event-driven matchmaking.
func (h *WebSocketHandler) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WS] Failed to upgrade connection: %v", err)
		return
	}
	defer conn.Close()

	// Set up ping/pong handlers to keep connection alive
	conn.SetPingHandler(func(appData string) error {
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		return conn.WriteMessage(websocket.PongMessage, []byte{})
	})

	conn.SetPongHandler(func(appData string) error {
		conn.SetReadDeadline(time.Now().Add(120 * time.Second))
		return nil
	})

	// Set initial read deadline
	conn.SetReadDeadline(time.Now().Add(120 * time.Second))

	// Wait for initial message with tags
	var initMsg models.WebSocketMessage
	err = conn.ReadJSON(&initMsg)
	if err != nil {
		log.Printf("[WS] Failed to read initial message: %v", err)
		return
	}

	if initMsg.Type != models.MsgTypeFindMatch {
		h.sendError(conn, "Expected find_match message as first message")
		return
	}

	// Parse tags and mode from data payload
	data, ok := initMsg.Data.(map[string]interface{})
	if !ok {
		h.sendError(conn, "Invalid data format — expected JSON object with 'tags'")
		return
	}

	tagsInterface, ok := data["tags"].([]interface{})
	if !ok {
		h.sendError(conn, "Tags array expected in 'tags' field")
		return
	}

	tags := make([]string, 0, len(tagsInterface))
	for _, tag := range tagsInterface {
		if s, ok := tag.(string); ok {
			tags = append(tags, s)
		}
	}

	if len(tags) == 0 {
		h.sendError(conn, "At least one tag is required for matching")
		return
	}

	// Parse optional mode (chat or video)
	mode := "chat"
	if modeVal, ok := data["mode"].(string); ok {
		if modeVal == "video" {
			mode = "video"
		}
	}

	// Create user and register with matchmaker
	user := models.NewUser(conn, tags)
	user.VideoEnabled = mode == "video"

	h.mu.Lock()
	h.users[user.ID] = user
	h.mu.Unlock()

	h.matchmaker.AddUser(user)

	log.Printf("[WS] User %s connected with tags: %v, mode: %s", user.ID, tags, mode)

	// Enqueue user for event-driven matching
	h.matchmaker.EnqueueUser(user)

	// Notify the user they are now searching
	searchingMsg := models.WebSocketMessage{
		Type:      models.MsgTypeSearching,
		Data:      map[string]interface{}{"message": "Searching for a match...", "user_id": user.ID, "mode": mode},
		Timestamp: time.Now(),
	}
	user.SendMessage(searchingMsg)

	// Start a lightweight goroutine to watch for match results
	go h.watchForMatch(user)

	// Handle incoming messages (this blocks until disconnect)
	h.handleMessages(user)
}

// watchForMatch polls the user's match state and sends a match_found
// WebSocket message when a match is created by the event processor.
func (h *WebSocketHandler) watchForMatch(user *models.User) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if user.State == models.StateMatched {
				match := h.matchmaker.GetMatch(user.ID)
				if match != nil {
					strangerID := match.PartnerID(user.ID)

					// Send match_found to this user
					matchMsg := models.WebSocketMessage{
						Type: models.MsgTypeMatchFound,
						Data: map[string]interface{}{
							"match_id":      match.ID,
							"shared_tags":   match.SharedTags,
							"similarity":    match.Similarity,
							"stranger_id":   strangerID,
							"mode":          match.Mode,
							"initiator":     match.Initiator == user.ID,
							"video_quality": match.VideoQuality,
						},
						Timestamp: time.Now(),
					}

					if err := user.SendMessage(matchMsg); err != nil {
						log.Printf("[WS] Failed to send match_found to %s: %v", user.ID, err)
					}

					// Also notify the partner
					h.mu.Lock()
					partner, partnerExists := h.users[strangerID]
					h.mu.Unlock()

					if partnerExists {
						partnerMsg := models.WebSocketMessage{
							Type: models.MsgTypeMatchFound,
							Data: map[string]interface{}{
								"match_id":      match.ID,
								"shared_tags":   match.SharedTags,
								"similarity":    match.Similarity,
								"stranger_id":   user.ID,
								"mode":          match.Mode,
								"initiator":     match.Initiator == partner.ID,
								"video_quality": match.VideoQuality,
							},
							Timestamp: time.Now(),
						}
						if err := partner.SendMessage(partnerMsg); err != nil {
							log.Printf("[WS] Failed to send match_found to %s: %v", strangerID, err)
						}
					}
				}
				return
			}

			if user.State == models.StateDisconnected {
				return
			}

			// Send queue update while still searching
			if user.State == models.StateSearching {
				stats := h.matchmaker.GetQueueStats()
				queueMsg := models.WebSocketMessage{
					Type: models.MsgTypeQueueUpdate,
					Data: map[string]interface{}{
						"searching_count": stats["searching_count"],
						"wait_time_ms":    time.Since(user.QueuedAt).Milliseconds(),
					},
					Timestamp: time.Now(),
				}
				user.SendMessage(queueMsg)
			}

		case <-time.After(10 * time.Minute):
			// Safety timeout
			return
		}
	}
}

// handleMessages reads messages from the user's WebSocket connection and
// dispatches them to the appropriate handler based on message type.
func (h *WebSocketHandler) handleMessages(user *models.User) {
	user.Conn.SetReadDeadline(time.Now().Add(120 * time.Second))

	for {
		var msg models.WebSocketMessage
		err := user.Conn.ReadJSON(&msg)
		if err != nil {
			log.Printf("[WS] User %s disconnected: %v", user.ID, err)
			h.handleDisconnect(user)
			break
		}

		// Reset read deadline on successful message
		user.Conn.SetReadDeadline(time.Now().Add(120 * time.Second))

		switch msg.Type {
		// Chat & matchmaking
		case models.MsgTypeChatMessage:
			h.handleChatMessage(user, msg)
		case models.MsgTypeTyping:
			h.handleTyping(user, msg)
		case models.MsgTypeSkip:
			h.handleSkip(user)
		case models.MsgTypeReport:
			h.handleReport(user, msg)
		case models.MsgTypeFindMatch:
			h.handleReSearch(user, msg)
		case models.MsgTypePong:
			// Heartbeat acknowledgment

		// WebRTC video call signaling
		case models.MsgTypeOffer:
			h.handleSDPOffer(user, msg)
		case models.MsgTypeAnswer:
			h.handleSDPAnswer(user, msg)
		case models.MsgTypeICECandidate:
			h.handleICECandidate(user, msg)
		case models.MsgTypeVideoReady:
			h.handleVideoReady(user, msg)
		case models.MsgTypeVideoToggle:
			h.handleVideoToggle(user, msg)
		case models.MsgTypeAudioToggle:
			h.handleAudioToggle(user, msg)
		case models.MsgTypeEndCall:
			h.handleEndCall(user, msg)

		default:
			log.Printf("[WS] Unknown message type '%s' from user %s", msg.Type, user.ID)
		}
	}
}

// ============================================
// CHAT HANDLERS
// ============================================

func (h *WebSocketHandler) handleChatMessage(user *models.User, msg models.WebSocketMessage) {
	match := h.matchmaker.GetMatch(user.ID)
	if match == nil {
		h.sendError(user.Conn, "No active match — cannot send message")
		return
	}

	recipientID := match.PartnerID(user.ID)

	h.mu.Lock()
	recipient, exists := h.users[recipientID]
	h.mu.Unlock()

	if !exists {
		h.sendError(user.Conn, "Recipient not found — they may have disconnected")
		return
	}

	chatMsg := models.WebSocketMessage{
		Type:      models.MsgTypeChatMessage,
		FromID:    user.ID,
		Data:      msg.Data,
		Timestamp: time.Now(),
	}

	if err := recipient.SendMessage(chatMsg); err != nil {
		log.Printf("[WS] Failed to deliver chat message to %s: %v", recipientID, err)
		h.sendError(user.Conn, "Failed to deliver message")
		return
	}

	// MongoDB persistence (non-blocking)
	go h.persistChatMessage(match.ID, user.ID, recipientID, msg)
}

func (h *WebSocketHandler) persistChatMessage(matchID, fromID, toID string, msg models.WebSocketMessage) {
	data, ok := msg.Data.(map[string]interface{})
	if !ok {
		return
	}
	content, _ := data["content"].(string)
	if content == "" {
		return
	}

	dbMsg := &models.DBMessage{
		MatchID:   matchID,
		FromID:    fromID,
		ToID:      toID,
		Content:   content,
		Timestamp: time.Now(),
	}

	if err := h.chatRepo.SaveMessage(dbMsg); err != nil {
		log.Printf("[DB] Failed to save chat message: %v", err)
	}
}

func (h *WebSocketHandler) handleTyping(user *models.User, msg models.WebSocketMessage) {
	match := h.matchmaker.GetMatch(user.ID)
	if match == nil {
		return
	}

	recipientID := match.PartnerID(user.ID)

	h.mu.Lock()
	recipient, exists := h.users[recipientID]
	h.mu.Unlock()

	if !exists {
		return
	}

	typingMsg := models.WebSocketMessage{
		Type:      models.MsgTypeTyping,
		FromID:    user.ID,
		Data:      msg.Data,
		Timestamp: time.Now(),
	}
	recipient.SendMessage(typingMsg)
}

// ============================================
// MATCH CONTROL HANDLERS
// ============================================

func (h *WebSocketHandler) handleSkip(user *models.User) {
	log.Printf("[WS] User %s skipped current match", user.ID)

	// Dissolve the match and notify partner
	match := h.matchmaker.RemoveMatch(user.ID)
	if match != nil {
		partnerID := match.PartnerID(user.ID)

		h.mu.Lock()
		partner, partnerExists := h.users[partnerID]
		h.mu.Unlock()

		if partnerExists {
			disconnectMsg := models.WebSocketMessage{
				Type:      models.MsgTypeDisconnected,
				Data:      map[string]interface{}{"message": "Stranger skipped the conversation", "reason": "skip"},
				Timestamp: time.Now(),
			}
			partner.SendMessage(disconnectMsg)

			partner.MatchedWith = ""
			h.matchmaker.EnqueueUser(partner)
			go h.watchForMatch(partner)
		}
	}

	user.MatchedWith = ""
	h.matchmaker.EnqueueUser(user)

	skipMsg := models.WebSocketMessage{
		Type:      models.MsgTypeSkipped,
		Data:      map[string]interface{}{"message": "Searching for a new match..."},
		Timestamp: time.Now(),
	}
	user.SendMessage(skipMsg)

	go h.watchForMatch(user)
}

func (h *WebSocketHandler) handleReSearch(user *models.User, msg models.WebSocketMessage) {
	data, ok := msg.Data.(map[string]interface{})
	if !ok {
		return
	}

	tagsInterface, ok := data["tags"].([]interface{})
	if !ok {
		return
	}

	tags := make([]string, 0, len(tagsInterface))
	for _, tag := range tagsInterface {
		if s, ok := tag.(string); ok {
			tags = append(tags, s)
		}
	}

	if len(tags) == 0 {
		return
	}

	// Parse optional mode
	mode := "chat"
	if modeVal, ok := data["mode"].(string); ok && modeVal == "video" {
		mode = "video"
		user.VideoEnabled = true
	}

	// If currently matched, dissolve first
	if user.State == models.StateMatched {
		match := h.matchmaker.RemoveMatch(user.ID)
		if match != nil {
			partnerID := match.PartnerID(user.ID)

			h.mu.Lock()
			partner, partnerExists := h.users[partnerID]
			h.mu.Unlock()

			if partnerExists {
				disconnectMsg := models.WebSocketMessage{
					Type:      models.MsgTypeDisconnected,
					Data:      map[string]interface{}{"message": "Stranger disconnected", "reason": "reconnect"},
					Timestamp: time.Now(),
				}
				partner.SendMessage(disconnectMsg)
				partner.MatchedWith = ""
				h.matchmaker.EnqueueUser(partner)
				go h.watchForMatch(partner)
			}
		}
	}

	user.Tags = tags
	h.matchmaker.EnqueueUser(user)

	searchMsg := models.WebSocketMessage{
		Type:      models.MsgTypeSearching,
		Data:      map[string]interface{}{"message": "Searching with updated tags...", "tags": tags, "mode": mode},
		Timestamp: time.Now(),
	}
	user.SendMessage(searchMsg)

	go h.watchForMatch(user)
}

func (h *WebSocketHandler) handleReport(user *models.User, msg models.WebSocketMessage) {
	var reportReq models.ReportRequest
	dataBytes, _ := json.Marshal(msg.Data)
	json.Unmarshal(dataBytes, &reportReq)

	log.Printf("[WS] User %s reported match — reason: %s", user.ID, reportReq.Reason)
	h.handleSkip(user)
}

// ============================================
// WEBRTC VIDEO CALL SIGNALING HANDLERS
// ============================================

// forwardSignal sends a WebRTC signaling message to the user's matched partner.
// This is the core relay for offer/answer/ICE candidates.
func (h *WebSocketHandler) forwardSignal(user *models.User, msg models.WebSocketMessage) error {
	match := h.matchmaker.GetMatch(user.ID)
	if match == nil {
		return h.sendErrorReturn(user.Conn, "No active match — cannot send signal")
	}

	recipientID := match.PartnerID(user.ID)

	h.mu.Lock()
	recipient, exists := h.users[recipientID]
	h.mu.Unlock()

	if !exists {
		return h.sendErrorReturn(user.Conn, "Partner disconnected")
	}

	// Set FromID and forward
	msg.FromID = user.ID
	msg.Timestamp = time.Now()

	if err := recipient.SendMessage(msg); err != nil {
		log.Printf("[WS] Failed to forward %s to %s: %v", msg.Type, recipientID, err)
		return h.sendErrorReturn(user.Conn, "Failed to forward signal")
	}

	return nil
}

func (h *WebSocketHandler) handleSDPOffer(user *models.User, msg models.WebSocketMessage) {
	log.Printf("[WS] User %s sent SDP offer", user.ID)
	if err := h.forwardSignal(user, msg); err != nil {
		log.Printf("[WS] Error forwarding offer: %v", err)
	}
}

func (h *WebSocketHandler) handleSDPAnswer(user *models.User, msg models.WebSocketMessage) {
	log.Printf("[WS] User %s sent SDP answer", user.ID)
	if err := h.forwardSignal(user, msg); err != nil {
		log.Printf("[WS] Error forwarding answer: %v", err)
	}
}

func (h *WebSocketHandler) handleICECandidate(user *models.User, msg models.WebSocketMessage) {
	if err := h.forwardSignal(user, msg); err != nil {
		log.Printf("[WS] Error forwarding ICE candidate: %v", err)
	}
}

// handleVideoReady is sent by a client after they've initialized their local
// media and are ready to receive the peer's stream. We forward this to the
// partner so they know it's safe to start the WebRTC negotiation.
func (h *WebSocketHandler) handleVideoReady(user *models.User, msg models.WebSocketMessage) {
	log.Printf("[WS] User %s is video ready", user.ID)

	match := h.matchmaker.GetMatch(user.ID)
	if match == nil {
		h.sendError(user.Conn, "No active match")
		return
	}

	// Only relevant for video matches
	if match.Mode != "video" {
		return
	}

	recipientID := match.PartnerID(user.ID)

	h.mu.Lock()
	recipient, exists := h.users[recipientID]
	h.mu.Unlock()

	if !exists {
		return
	}

	// Notify partner that this user is ready
	readyMsg := models.WebSocketMessage{
		Type:      models.MsgTypePeerJoined,
		FromID:    user.ID,
		Data:      map[string]interface{}{"message": "Peer is ready for video"},
		Timestamp: time.Now(),
	}
	recipient.SendMessage(readyMsg)
}

func (h *WebSocketHandler) handleVideoToggle(user *models.User, msg models.WebSocketMessage) {
	log.Printf("[WS] User %s toggled video", user.ID)

	var payload models.VideoTogglePayload
	dataBytes, _ := json.Marshal(msg.Data)
	json.Unmarshal(dataBytes, &payload)

	// Update user state
	user.VideoEnabled = payload.Enabled

	// Forward to partner
	h.forwardSignal(user, models.WebSocketMessage{
		Type: models.MsgTypeVideoToggle,
		Data: msg.Data,
	})
}

func (h *WebSocketHandler) handleAudioToggle(user *models.User, msg models.WebSocketMessage) {
	log.Printf("[WS] User %s toggled audio", user.ID)
	// Forward to partner
	h.forwardSignal(user, models.WebSocketMessage{
		Type: models.MsgTypeAudioToggle,
		Data: msg.Data,
	})
}

func (h *WebSocketHandler) handleEndCall(user *models.User, msg models.WebSocketMessage) {
	log.Printf("[WS] User %s ended video call", user.ID)

	// Forward end call to partner first
	h.forwardSignal(user, models.WebSocketMessage{
		Type: models.MsgTypeEndCall,
		Data: map[string]interface{}{"message": "Call ended"},
	})

	// Then treat as skip — re-enqueue both
	h.handleSkip(user)
}

// ============================================
// DISCONNECT HANDLER
// ============================================

func (h *WebSocketHandler) handleDisconnect(user *models.User) {
	log.Printf("[WS] Handling disconnect for user %s", user.ID)

	user.State = models.StateDisconnected

	var partnerID string
	if user.MatchedWith != "" {
		partnerID = user.MatchedWith
	}

	h.matchmaker.RemoveUser(user.ID)

	if partnerID != "" {
		h.mu.Lock()
		partner, partnerExists := h.users[partnerID]
		h.mu.Unlock()

		if partnerExists {
			disconnectMsg := models.WebSocketMessage{
				Type:      models.MsgTypeDisconnected,
				Data:      map[string]interface{}{"message": "Stranger disconnected", "reason": "disconnect"},
				Timestamp: time.Now(),
			}
			partner.SendMessage(disconnectMsg)

			partner.MatchedWith = ""
			h.matchmaker.EnqueueUser(partner)
			go h.watchForMatch(partner)
		}
	}

	h.mu.Lock()
	delete(h.users, user.ID)
	h.mu.Unlock()

	log.Printf("[WS] User %s fully cleaned up", user.ID)
}

// ============================================
// UTILITY METHODS
// ============================================

func (h *WebSocketHandler) sendError(conn *websocket.Conn, message string) {
	errMsg := models.WebSocketMessage{
		Type:      models.MsgTypeError,
		Data:      map[string]interface{}{"message": message},
		Timestamp: time.Now(),
	}
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := conn.WriteJSON(errMsg); err != nil {
		log.Printf("[WS] Failed to send error message: %v", err)
	}
}

func (h *WebSocketHandler) sendErrorReturn(conn *websocket.Conn, message string) error {
	h.sendError(conn, message)
	return nil // Return nil to satisfy error interface; actual error is sent over WS
}
