// websocket.go — Fixed: watchForMatch race, handleVideoReady forwarding,
// and added connection state validation for WebRTC signaling

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
		return true
	},
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
}

type WebSocketHandler struct {
	matchmaker *services.Matchmaker
	users      map[string]*models.User
	chatRepo   *repositories.ChatRepository // ← was *models.ChatRepository
	mu         sync.RWMutex
}

func NewWebSocketHandler() *WebSocketHandler {
	return &WebSocketHandler{
		matchmaker: services.GetMatchmaker(),
		users:      make(map[string]*models.User),
		chatRepo:   repositories.NewChatRepository(), // ← was models.NewChatRepository()
	}
}

func (h *WebSocketHandler) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WS] Failed to upgrade connection: %v", err)
		return
	}
	defer conn.Close()

	// FIX: Larger write deadline for slow networks
	conn.SetPingHandler(func(appData string) error {
		conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		return conn.WriteMessage(websocket.PongMessage, []byte{})
	})

	conn.SetPongHandler(func(appData string) error {
		conn.SetReadDeadline(time.Now().Add(120 * time.Second))
		return nil
	})

	conn.SetReadDeadline(time.Now().Add(120 * time.Second))

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

	data, ok := initMsg.Data.(map[string]interface{})
	if !ok {
		h.sendError(conn, "Invalid data format")
		return
	}

	tagsInterface, ok := data["tags"].([]interface{})
	if !ok {
		h.sendError(conn, "Tags array expected")
		return
	}

	tags := make([]string, 0, len(tagsInterface))
	for _, tag := range tagsInterface {
		if s, ok := tag.(string); ok {
			tags = append(tags, s)
		}
	}

	if len(tags) == 0 {
		h.sendError(conn, "At least one tag is required")
		return
	}

	mode := "chat"
	if modeVal, ok := data["mode"].(string); ok && modeVal == "video" {
		mode = "video"
	}

	user := models.NewUser(conn, tags)
	user.VideoEnabled = mode == "video"

	h.mu.Lock()
	h.users[user.ID] = user
	h.mu.Unlock()

	h.matchmaker.AddUser(user)

	log.Printf("[WS] User %s connected (tags: %v, mode: %s)", user.ID, tags, mode)

	h.matchmaker.EnqueueUser(user)

	searchingMsg := models.WebSocketMessage{
		Type:      models.MsgTypeSearching,
		Data:      map[string]interface{}{"message": "Searching for a match...", "user_id": user.ID, "mode": mode},
		Timestamp: time.Now(),
	}
	user.SendMessage(searchingMsg)

	go h.watchForMatch(user)

	h.handleMessages(user)
}

// watchForMatch polls for match state.
// CRITICAL FIX: Only User1 sends match_found to both participants.
// User2 waits — prevents race where both goroutines send duplicate messages.
func (h *WebSocketHandler) watchForMatch(user *models.User) {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if user.State == models.StateDisconnected {
				return
			}

			if user.State == models.StateMatched {
				match := h.matchmaker.GetMatch(user.ID)
				if match == nil {
					return
				}

				// CRITICAL: Only User1 sends match_found to both sides.
				if match.User1ID != user.ID {
					// We are User2 — wait for User1's goroutine to notify us, then exit
					time.Sleep(3 * time.Second)
					return
				}

				user2ID := match.User2ID

				h.mu.RLock() // FIX: Use RLock for reads
				user2, user2Exists := h.users[user2ID]
				h.mu.RUnlock()

				// Send to User2 FIRST — gives non-initiator time to set up PC before offer
				if user2Exists {
					user2Msg := models.WebSocketMessage{
						Type: models.MsgTypeMatchFound,
						Data: map[string]interface{}{
							"match_id":      match.ID,
							"shared_tags":   match.SharedTags,
							"similarity":    match.Similarity,
							"stranger_id":   user.ID,
							"mode":          match.Mode,
							"initiator":     match.Initiator == user2ID,
							"video_quality": match.VideoQuality,
						},
						Timestamp: time.Now(),
					}
					if err := user2.SendMessage(user2Msg); err != nil {
						log.Printf("[WS] Failed to send match_found to user2 %s: %v", user2ID, err)
					}
					// Give user2 time to initialize RTCPeerConnection
					time.Sleep(500 * time.Millisecond)
				}

				// Then send to User1 (this user)
				user1Msg := models.WebSocketMessage{
					Type: models.MsgTypeMatchFound,
					Data: map[string]interface{}{
						"match_id":      match.ID,
						"shared_tags":   match.SharedTags,
						"similarity":    match.Similarity,
						"stranger_id":   user2ID,
						"mode":          match.Mode,
						"initiator":     match.Initiator == user.ID,
						"video_quality": match.VideoQuality,
					},
					Timestamp: time.Now(),
				}

				if err := user.SendMessage(user1Msg); err != nil {
					log.Printf("[WS] Failed to send match_found to user1 %s: %v", user.ID, err)
				}

				return
			}

			// Queue update while searching
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
			return
		}
	}
}

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

		user.Conn.SetReadDeadline(time.Now().Add(120 * time.Second))

		// FIX: Validate user is in a match before allowing WebRTC signaling
		switch msg.Type {
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
			// heartbeat

		// WebRTC signaling — validate match exists
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

// ─── Chat Handlers ────────────────────────────────────────────────────────────

func (h *WebSocketHandler) handleChatMessage(user *models.User, msg models.WebSocketMessage) {
	match := h.matchmaker.GetMatch(user.ID)
	if match == nil {
		h.sendError(user.Conn, "No active match")
		return
	}

	recipientID := match.PartnerID(user.ID)

	h.mu.RLock()
	recipient, exists := h.users[recipientID]
	h.mu.RUnlock()

	if !exists {
		h.sendError(user.Conn, "Recipient not found")
		return
	}

	chatMsg := models.WebSocketMessage{
		Type:      models.MsgTypeChatMessage,
		FromID:    user.ID,
		Data:      msg.Data,
		Timestamp: time.Now(),
	}

	if err := recipient.SendMessage(chatMsg); err != nil {
		log.Printf("[WS] Failed to deliver message to %s: %v", recipientID, err)
	}

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
		log.Printf("[DB] Failed to save message: %v", err)
	}
}

func (h *WebSocketHandler) handleTyping(user *models.User, msg models.WebSocketMessage) {
	match := h.matchmaker.GetMatch(user.ID)
	if match == nil {
		return
	}

	h.mu.RLock()
	recipient, exists := h.users[match.PartnerID(user.ID)]
	h.mu.RUnlock()

	if !exists {
		return
	}

	recipient.SendMessage(models.WebSocketMessage{
		Type:      models.MsgTypeTyping,
		FromID:    user.ID,
		Data:      msg.Data,
		Timestamp: time.Now(),
	})
}

// ─── Match Control ────────────────────────────────────────────────────────────

func (h *WebSocketHandler) handleSkip(user *models.User) {
	log.Printf("[WS] User %s skipped", user.ID)

	match := h.matchmaker.RemoveMatch(user.ID)
	if match != nil {
		partnerID := match.PartnerID(user.ID)

		h.mu.RLock()
		partner, partnerExists := h.users[partnerID]
		h.mu.RUnlock()

		if partnerExists {
			partner.SendMessage(models.WebSocketMessage{
				Type:      models.MsgTypeDisconnected,
				Data:      map[string]interface{}{"message": "Stranger skipped", "reason": "skip"},
				Timestamp: time.Now(),
			})
			partner.MatchedWith = ""
			h.matchmaker.EnqueueUser(partner)
			go h.watchForMatch(partner)
		}
	}

	user.MatchedWith = ""
	h.matchmaker.EnqueueUser(user)
	user.SendMessage(models.WebSocketMessage{
		Type:      models.MsgTypeSkipped,
		Data:      map[string]interface{}{"message": "Searching for new match..."},
		Timestamp: time.Now(),
	})
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

	tags := make([]string, 0)
	for _, tag := range tagsInterface {
		if s, ok := tag.(string); ok {
			tags = append(tags, s)
		}
	}

	if len(tags) == 0 {
		return
	}

	mode := "chat"
	if modeVal, ok := data["mode"].(string); ok && modeVal == "video" {
		mode = "video"
		user.VideoEnabled = true
	} else {
		user.VideoEnabled = false
	}

	if user.State == models.StateMatched {
		match := h.matchmaker.RemoveMatch(user.ID)
		if match != nil {
			h.mu.RLock()
			partner, partnerExists := h.users[match.PartnerID(user.ID)]
			h.mu.RUnlock()

			if partnerExists {
				partner.SendMessage(models.WebSocketMessage{
					Type:      models.MsgTypeDisconnected,
					Data:      map[string]interface{}{"message": "Stranger disconnected", "reason": "reconnect"},
					Timestamp: time.Now(),
				})
				partner.MatchedWith = ""
				h.matchmaker.EnqueueUser(partner)
				go h.watchForMatch(partner)
			}
		}
	}

	user.Tags = tags
	h.matchmaker.EnqueueUser(user)
	user.SendMessage(models.WebSocketMessage{
		Type:      models.MsgTypeSearching,
		Data:      map[string]interface{}{"message": "Searching...", "tags": tags, "mode": mode},
		Timestamp: time.Now(),
	})
	go h.watchForMatch(user)
}

func (h *WebSocketHandler) handleReport(user *models.User, msg models.WebSocketMessage) {
	var reportReq models.ReportRequest
	dataBytes, _ := json.Marshal(msg.Data)
	json.Unmarshal(dataBytes, &reportReq)
	log.Printf("[WS] User %s reported — reason: %s", user.ID, reportReq.Reason)
	h.handleSkip(user)
}

// ─── WebRTC Signaling ─────────────────────────────────────────────────────────

// forwardSignal forwards WebRTC signaling messages to the partner.
// FIX: Added detailed logging and connection state checks.
func (h *WebSocketHandler) forwardSignal(user *models.User, msg models.WebSocketMessage) error {
	match := h.matchmaker.GetMatch(user.ID)
	if match == nil {
		log.Printf("[WS] forwardSignal: no active match for user %s", user.ID)
		return h.sendErrorReturn(user.Conn, "No active match")
	}

	h.mu.RLock()
	recipient, exists := h.users[match.PartnerID(user.ID)]
	h.mu.RUnlock()

	if !exists {
		log.Printf("[WS] forwardSignal: partner disconnected for user %s", user.ID)
		return h.sendErrorReturn(user.Conn, "Partner disconnected")
	}

	msg.FromID = user.ID
	msg.Timestamp = time.Now()

	log.Printf("[WS] Forwarding %s from %s to %s", msg.Type, user.ID, match.PartnerID(user.ID))

	if err := recipient.SendMessage(msg); err != nil {
		log.Printf("[WS] Failed to forward %s: %v", msg.Type, err)
		return err
	}
	return nil
}

func (h *WebSocketHandler) handleSDPOffer(user *models.User, msg models.WebSocketMessage) {
	log.Printf("[WS] User %s sent SDP offer (len: %d)", user.ID, len(msg.Data.(map[string]interface{})["sdp"].(string)))
	h.forwardSignal(user, msg)
}

func (h *WebSocketHandler) handleSDPAnswer(user *models.User, msg models.WebSocketMessage) {
	log.Printf("[WS] User %s sent SDP answer (len: %d)", user.ID, len(msg.Data.(map[string]interface{})["sdp"].(string)))
	h.forwardSignal(user, msg)
}

func (h *WebSocketHandler) handleICECandidate(user *models.User, msg models.WebSocketMessage) {
	h.forwardSignal(user, msg)
}

// FIX: handleVideoReady now properly forwards to partner AND logs state
func (h *WebSocketHandler) handleVideoReady(user *models.User, msg models.WebSocketMessage) {
	log.Printf("[WS] User %s video ready", user.ID)

	match := h.matchmaker.GetMatch(user.ID)
	if match == nil {
		log.Printf("[WS] handleVideoReady: no match for user %s", user.ID)
		return
	}

	if match.Mode != "video" {
		log.Printf("[WS] handleVideoReady: match mode is %s, not video", match.Mode)
		return
	}

	h.mu.RLock()
	recipient, exists := h.users[match.PartnerID(user.ID)]
	h.mu.RUnlock()

	if !exists {
		log.Printf("[WS] handleVideoReady: partner %s not found", match.PartnerID(user.ID))
		return
	}

	// Forward video_ready to partner so they know peer is ready
	peerMsg := models.WebSocketMessage{
		Type:      models.MsgTypePeerJoined,
		FromID:    user.ID,
		Data:      map[string]interface{}{"message": "Peer is ready for video"},
		Timestamp: time.Now(),
	}

	if err := recipient.SendMessage(peerMsg); err != nil {
		log.Printf("[WS] Failed to send peer_joined to %s: %v", recipient.ID, err)
	}

	// Also forward the original video_ready message for compatibility
	h.forwardSignal(user, msg)
}

func (h *WebSocketHandler) handleVideoToggle(user *models.User, msg models.WebSocketMessage) {
	var payload models.VideoTogglePayload
	dataBytes, _ := json.Marshal(msg.Data)
	json.Unmarshal(dataBytes, &payload)
	user.VideoEnabled = payload.Enabled
	h.forwardSignal(user, models.WebSocketMessage{Type: models.MsgTypeVideoToggle, Data: msg.Data})
}

func (h *WebSocketHandler) handleAudioToggle(user *models.User, msg models.WebSocketMessage) {
	h.forwardSignal(user, models.WebSocketMessage{Type: models.MsgTypeAudioToggle, Data: msg.Data})
}

func (h *WebSocketHandler) handleEndCall(user *models.User, msg models.WebSocketMessage) {
	log.Printf("[WS] User %s ended call", user.ID)
	h.forwardSignal(user, models.WebSocketMessage{
		Type: models.MsgTypeEndCall,
		Data: map[string]interface{}{"message": "Call ended"},
	})
	h.handleSkip(user)
}

// ─── Disconnect ───────────────────────────────────────────────────────────────

func (h *WebSocketHandler) handleDisconnect(user *models.User) {
	user.State = models.StateDisconnected

	var partnerID string
	if user.MatchedWith != "" {
		partnerID = user.MatchedWith
	}

	h.matchmaker.RemoveUser(user.ID)

	if partnerID != "" {
		h.mu.RLock()
		partner, partnerExists := h.users[partnerID]
		h.mu.RUnlock()

		if partnerExists {
			partner.SendMessage(models.WebSocketMessage{
				Type:      models.MsgTypeDisconnected,
				Data:      map[string]interface{}{"message": "Stranger disconnected", "reason": "disconnect"},
				Timestamp: time.Now(),
			})
			partner.MatchedWith = ""
			h.matchmaker.EnqueueUser(partner)
			go h.watchForMatch(partner)
		}
	}

	h.mu.Lock()
	delete(h.users, user.ID)
	h.mu.Unlock()

	log.Printf("[WS] User %s cleaned up", user.ID)
}

// ─── Utilities ────────────────────────────────────────────────────────────────

func (h *WebSocketHandler) sendError(conn *websocket.Conn, message string) {
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	conn.WriteJSON(models.WebSocketMessage{
		Type:      models.MsgTypeError,
		Data:      map[string]interface{}{"message": message},
		Timestamp: time.Now(),
	})
}

func (h *WebSocketHandler) sendErrorReturn(conn *websocket.Conn, message string) error {
	h.sendError(conn, message)
	return nil
}
