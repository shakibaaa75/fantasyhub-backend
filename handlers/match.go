package handlers

import (
	"encoding/json"
	"net/http"

	"backend/models"
	"backend/services"
)

// MatchHandler provides HTTP API endpoints for querying matchmaking state
// and configuration. These complement the WebSocket real-time channel
// for cases where REST access is more convenient (e.g., monitoring dashboards).
type MatchHandler struct {
	matchmaker *services.Matchmaker
}

func NewMatchHandler() *MatchHandler {
	return &MatchHandler{
		matchmaker: services.GetMatchmaker(),
	}
}

// GetMatchStatus returns whether a user is currently matched, and if so,
// the match details including shared tags and similarity score.
// GET /api/match/status?user_id=xxx
func (h *MatchHandler) GetMatchStatus(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id query parameter required", http.StatusBadRequest)
		return
	}

	match := h.matchmaker.GetMatch(userID)
	if match == nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"matched": false,
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"matched":     true,
		"match_id":    match.ID,
		"shared_tags": match.SharedTags,
		"similarity":  match.Similarity,
	})
}

// GetOnlineCount returns the number of currently connected users.
// GET /api/online/count
func (h *MatchHandler) GetOnlineCount(w http.ResponseWriter, r *http.Request) {
	count := h.matchmaker.GetOnlineCount()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"count": count,
	})
}

// GetTags returns the list of available interest tags that users can select.
// GET /api/tags
func (h *MatchHandler) GetTags(w http.ResponseWriter, r *http.Request) {
	tags := []models.Tag{
		{Name: "Gaming", Icon: "gamepad-2", Category: "fun"},
		{Name: "Anime", Icon: "tv", Category: "fun"},
		{Name: "Music", Icon: "music", Category: "creative"},
		{Name: "Movies", Icon: "film", Category: "fun"},
		{Name: "Tech", Icon: "cpu", Category: "tech"},
		{Name: "Coding", Icon: "code", Category: "tech"},
		{Name: "Crypto", Icon: "bitcoin", Category: "tech"},
		{Name: "Fitness", Icon: "dumbbell", Category: "life"},
		{Name: "Art", Icon: "palette", Category: "creative"},
		{Name: "Photography", Icon: "camera", Category: "creative"},
		{Name: "Reading", Icon: "book-open", Category: "life"},
		{Name: "Cooking", Icon: "chef-hat", Category: "life"},
		{Name: "Travel", Icon: "plane", Category: "life"},
		{Name: "Sports", Icon: "trophy", Category: "life"},
		{Name: "K-Pop", Icon: "mic", Category: "fun"},
		{Name: "Memes", Icon: "image", Category: "fun"},
		{Name: "Design", Icon: "pen-tool", Category: "creative"},
		{Name: "AI", Icon: "brain", Category: "tech"},
		{Name: "Startups", Icon: "rocket", Category: "tech"},
		{Name: "Meditation", Icon: "leaf", Category: "life"},
		{Name: "Writing", Icon: "pencil", Category: "creative"},
		{Name: "Fashion", Icon: "shirt", Category: "life"},
		{Name: "RPGs", Icon: "swords", Category: "fun"},
		{Name: "FPS", Icon: "crosshair", Category: "fun"},
		{Name: "Board Games", Icon: "dice-5", Category: "fun"},
		{Name: "Podcasts", Icon: "headphones", Category: "fun"},
		{Name: "Science", Icon: "atom", Category: "tech"},
		{Name: "Space", Icon: "orbit", Category: "tech"},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tags)
}

// GetQueueStats returns detailed statistics about the search queue,
// including searching count, tag distribution, and current configuration.
// GET /api/queue/stats
func (h *MatchHandler) GetQueueStats(w http.ResponseWriter, r *http.Request) {
	stats := h.matchmaker.GetQueueStats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// GetConfig returns the current matchmaking configuration parameters.
// GET /api/config
func (h *MatchHandler) GetConfig(w http.ResponseWriter, r *http.Request) {
	config := h.matchmaker.GetConfig()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"min_similarity":       config.MinSimilarity,
		"min_shared_tags":      config.MinSharedTags,
		"queue_timeout_s":      config.QueueTimeout.Seconds(),
		"cleanup_interval_s":   config.CleanupInterval.Seconds(),
		"heartbeat_interval_s": config.HeartbeatInterval.Seconds(),
	})
}

// UpdateConfig allows runtime reconfiguration of matchmaking parameters.
// POST /api/config
// Body: {"min_similarity": 0.3, "min_shared_tags": 2, ...}
func (h *MatchHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST method required", http.StatusMethodNotAllowed)
		return
	}

	var newConfig models.MatchConfig
	if err := json.NewDecoder(r.Body).Decode(&newConfig); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	// Validate
	if newConfig.MinSimilarity < 0 || newConfig.MinSimilarity > 1 {
		http.Error(w, "min_similarity must be between 0.0 and 1.0", http.StatusBadRequest)
		return
	}
	if newConfig.MinSharedTags < 1 {
		newConfig.MinSharedTags = 1
	}

	// Preserve defaults for zero-valued fields
	current := h.matchmaker.GetConfig()
	if newConfig.QueueTimeout == 0 {
		newConfig.QueueTimeout = current.QueueTimeout
	}
	if newConfig.CleanupInterval == 0 {
		newConfig.CleanupInterval = current.CleanupInterval
	}
	if newConfig.HeartbeatInterval == 0 {
		newConfig.HeartbeatInterval = current.HeartbeatInterval
	}

	h.matchmaker.UpdateConfig(newConfig)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message": "Configuration updated",
		"config": map[string]interface{}{
			"min_similarity":  newConfig.MinSimilarity,
			"min_shared_tags": newConfig.MinSharedTags,
			"queue_timeout_s": newConfig.QueueTimeout.Seconds(),
		},
	})
}
