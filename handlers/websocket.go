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
// instant pairing — no polling is involved.
type WebSocketHandler struct {
	matchmaker *services.Matchmaker
	users      map[string]*models.User
	chatRepo   *repositories.ChatRepository // Added for DB persistence
	mu         sync.Mutex
}

func NewWebSocketHandler() *WebSocketHandler {
	return &WebSocketHandler{
		matchmaker: services.GetMatchmaker(),
		users:      make(map[string]*models.User),
		chatRepo:   repositories.NewChatRepository(), // Initialize repo
	}
}

// HandleWebSocket upgrades an HTTP connection to WebSocket, waits for the
// initial find_match message with tags, registers the user, and enqueues
// them for event-driven matchmaking.
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

	if initMsg.Type != "find_match" {
		h.sendError(conn, "Expected find_match message as first message")
		return
	}

	// Parse tags from data payload
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

	// Create user and register with matchmaker
	user := models.NewUser(conn, tags)

	h.mu.Lock()
	h.users[user.ID] = user
	h.mu.Unlock()

	h.matchmaker.AddUser(user)

	log.Printf("[WS] User %s connected with tags: %v", user.ID, tags)

	// Enqueue user for event-driven matching
	h.matchmaker.EnqueueUser(user)

	// Notify the user they are now searching
	searchingMsg := models.WebSocketMessage{
		Type:      "searching",
		Data:      map[string]interface{}{"message": "Searching for a match...", "user_id": user.ID},
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
					// Determine the stranger's ID
					strangerID := match.User2ID
					if user.ID == match.User1ID {
						strangerID = match.User2ID
					}

					// Send match_found to this user
					matchMsg := models.WebSocketMessage{
						Type: "match_found",
						Data: map[string]interface{}{
							"match_id":    match.ID,
							"shared_tags": match.SharedTags,
							"similarity":  match.Similarity,
							"stranger_id": strangerID,
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
							Type: "match_found",
							Data: map[string]interface{}{
								"match_id":    match.ID,
								"shared_tags": match.SharedTags,
								"similarity":  match.Similarity,
								"stranger_id": user.ID,
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
					Type: "queue_update",
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
		case "chat_message":
			h.handleChatMessage(user, msg)
		case "typing":
			h.handleTyping(user, msg)
		case "skip":
			h.handleSkip(user)
		case "report":
			h.handleReport(user, msg)
		case "find_match":
			// Client wants to search again with (optionally) new tags
			h.handleReSearch(user, msg)
		case "pong":
			// Heartbeat acknowledgment — connection is alive
		default:
			log.Printf("[WS] Unknown message type '%s' from user %s", msg.Type, user.ID)
		}
	}
}

// handleChatMessage forwards a chat message to the user's matched partner
// AND saves it to MongoDB in the background.
func (h *WebSocketHandler) handleChatMessage(user *models.User, msg models.WebSocketMessage) {
	match := h.matchmaker.GetMatch(user.ID)
	if match == nil {
		h.sendError(user.Conn, "No active match — cannot send message")
		return
	}

	recipientID := match.User1ID
	if user.ID == match.User1ID {
		recipientID = match.User2ID
	}

	h.mu.Lock()
	recipient, exists := h.users[recipientID]
	h.mu.Unlock()

	if !exists {
		h.sendError(user.Conn, "Recipient not found — they may have disconnected")
		return
	}

	chatMsg := models.WebSocketMessage{
		Type:      "chat_message",
		FromID:    user.ID,
		Data:      msg.Data,
		Timestamp: time.Now(),
	}

	if err := recipient.SendMessage(chatMsg); err != nil {
		log.Printf("[WS] Failed to deliver chat message to %s: %v", recipientID, err)
		h.sendError(user.Conn, "Failed to deliver message")
		return
	}

	// --- MONGODB CHAT PERSISTENCE (Non-blocking) ---
	go func() {
		data, ok := msg.Data.(map[string]interface{})
		if !ok {
			return
		}
		content, _ := data["content"].(string)
		if content == "" {
			return
		}

		dbMsg := &models.DBMessage{
			MatchID:   match.ID,
			FromID:    user.ID,
			ToID:      recipientID,
			Content:   content,
			Timestamp: time.Now(),
		}

		if err := h.chatRepo.SaveMessage(dbMsg); err != nil {
			log.Printf("[DB] Failed to save chat message to MongoDB: %v", err)
		}
	}()
}

// handleTyping forwards a typing indicator to the matched partner.
func (h *WebSocketHandler) handleTyping(user *models.User, msg models.WebSocketMessage) {
	match := h.matchmaker.GetMatch(user.ID)
	if match == nil {
		return
	}

	recipientID := match.User1ID
	if user.ID == match.User1ID {
		recipientID = match.User2ID
	}

	h.mu.Lock()
	recipient, exists := h.users[recipientID]
	h.mu.Unlock()

	if !exists {
		return
	}

	typingMsg := models.WebSocketMessage{
		Type:      "typing",
		FromID:    user.ID,
		Data:      msg.Data,
		Timestamp: time.Now(),
	}
	recipient.SendMessage(typingMsg)
}

// handleSkip dissolves the current match and re-enqueues the user.
func (h *WebSocketHandler) handleSkip(user *models.User) {
	log.Printf("[WS] User %s skipped current match", user.ID)

	// Dissolve the match and notify partner
	match := h.matchmaker.RemoveMatch(user.ID)
	if match != nil {
		partnerID := match.User1ID
		if user.ID == match.User1ID {
			partnerID = match.User2ID
		}

		h.mu.Lock()
		partner, partnerExists := h.users[partnerID]
		h.mu.Unlock()

		if partnerExists {
			disconnectMsg := models.WebSocketMessage{
				Type:      "disconnected",
				Data:      map[string]interface{}{"message": "Stranger skipped the conversation"},
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
		Type:      "skipped",
		Data:      map[string]interface{}{"message": "Searching for a new match..."},
		Timestamp: time.Now(),
	}
	user.SendMessage(skipMsg)

	go h.watchForMatch(user)
}

// handleReSearch allows a user to update their tags and re-enter the search queue.
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

	if user.State == models.StateMatched {
		match := h.matchmaker.RemoveMatch(user.ID)
		if match != nil {
			partnerID := match.User1ID
			if user.ID == match.User1ID {
				partnerID = match.User2ID
			}

			h.mu.Lock()
			partner, partnerExists := h.users[partnerID]
			h.mu.Unlock()

			if partnerExists {
				disconnectMsg := models.WebSocketMessage{
					Type:      "disconnected",
					Data:      map[string]interface{}{"message": "Stranger disconnected"},
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
		Type:      "searching",
		Data:      map[string]interface{}{"message": "Searching with updated tags...", "tags": tags},
		Timestamp: time.Now(),
	}
	user.SendMessage(searchMsg)

	go h.watchForMatch(user)
}

// handleReport logs a report and then skips the user.
func (h *WebSocketHandler) handleReport(user *models.User, msg models.WebSocketMessage) {
	var reportReq models.ReportRequest
	dataBytes, _ := json.Marshal(msg.Data)
	json.Unmarshal(dataBytes, &reportReq)

	log.Printf("[WS] User %s reported match — reason: %s", user.ID, reportReq.Reason)

	// TODO: In production, save the report to MongoDB here
	h.handleSkip(user)
}

// handleDisconnect cleans up a disconnected user.
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
				Type:      "disconnected",
				Data:      map[string]interface{}{"message": "Stranger disconnected"},
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

// sendError sends an error WebSocket message to a connection.
func (h *WebSocketHandler) sendError(conn *websocket.Conn, message string) {
	errMsg := models.WebSocketMessage{
		Type:      "error",
		Data:      map[string]interface{}{"message": message},
		Timestamp: time.Now(),
	}
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := conn.WriteJSON(errMsg); err != nil {
		log.Printf("[WS] Failed to send error message: %v", err)
	}
}
