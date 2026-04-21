package services

import (
	"log"
	"sort"
	"sync"
	"time"

	"backend/models"
	"backend/utils"
)

// MatchEvent represents an event that triggers the matchmaker to attempt pairing.
// This is the foundation of the event-driven architecture: instead of polling,
// the matchmaker reacts instantly to each join event.
type MatchEvent struct {
	UserID string    // The user who triggered the event
	Type   EventType // What kind of event
}

// EventType distinguishes between different matchmaking triggers.
type EventType int

const (
	EventJoin    EventType = iota // A new user joined the search queue
	EventRequeue                  // A user re-entered the queue (e.g., after skip)
	EventLeave                    // A user left the queue (cleanup triggered)
)

// CandidateScore holds a candidate user along with their computed similarity score
// and shared tags, used for ranking during match selection.
type CandidateScore struct {
	UserID     string
	Similarity float64
	SharedTags []string
	QueuedAt   time.Time // Earlier = higher priority (waited longer)
}

// Matchmaker is the core matchmaking engine. It uses an event-driven architecture
// with Go channels instead of periodic polling. When a user joins the search queue,
// an event is pushed to the event channel, and a dedicated goroutine processes it
// immediately, scanning for compatible partners.
//
// Key design decisions:
//   - Event channel for instant, non-blocking match triggers (no polling)
//   - Tag reverse index (tag -> set of user IDs) for O(k) candidate lookup
//   - Jaccard similarity scoring with configurable threshold
//   - Fine-grained locking with no nested acquisitions (deadlock-free)
//   - FIFO priority: users waiting longer get matched first among equal scores
//   - Video mode matching: users with VideoEnabled=true are matched for video calls
type Matchmaker struct {
	// Configuration
	config models.MatchConfig

	// Core data structures
	users          map[string]*models.User    // All connected users (userID -> User)
	searchingUsers map[string]*models.User    // Users currently searching (userID -> User)
	activeMatches  map[string]*models.Match   // Active matches indexed by BOTH user IDs
	tagIndex       map[string]map[string]bool // Reverse index: tag -> set of user IDs in queue

	// Event-driven matching
	eventCh chan MatchEvent // Buffered channel for match triggers
	stopCh  chan struct{}   // Signal to stop the event processor

	// Synchronization
	mu sync.RWMutex // Protects all mutable state above
}

var instance *Matchmaker
var once sync.Once

// GetMatchmaker returns the singleton Matchmaker instance, initializing it on first call.
// It starts the event processing goroutine immediately.
func GetMatchmaker() *Matchmaker {
	once.Do(func() {
		config := models.DefaultMatchConfig()
		instance = &Matchmaker{
			config:         config,
			users:          make(map[string]*models.User),
			searchingUsers: make(map[string]*models.User),
			activeMatches:  make(map[string]*models.Match),
			tagIndex:       make(map[string]map[string]bool),
			eventCh:        make(chan MatchEvent, 256), // Buffered for burst tolerance
			stopCh:         make(chan struct{}),
		}
		go instance.processEvents() // Event-driven match processor
		go instance.cleanupLoop()   // Periodic stale user cleanup
		go instance.heartbeatLoop() // Periodic connection liveness check
	})
	return instance
}

// GetMatchmakerWithConfig returns a singleton Matchmaker with custom configuration.
// Must be called before GetMatchmaker() for the config to take effect.
func GetMatchmakerWithConfig(config models.MatchConfig) *Matchmaker {
	once.Do(func() {
		instance = &Matchmaker{
			config:         config,
			users:          make(map[string]*models.User),
			searchingUsers: make(map[string]*models.User),
			activeMatches:  make(map[string]*models.Match),
			tagIndex:       make(map[string]map[string]bool),
			eventCh:        make(chan MatchEvent, 256),
			stopCh:         make(chan struct{}),
		}
		go instance.processEvents()
		go instance.cleanupLoop()
		go instance.heartbeatLoop()
	})
	return instance
}

// processEvents is the heart of the event-driven architecture. It listens on the
// event channel and processes each join/requeue event by immediately attempting
// to find a match for the triggering user. This eliminates the need for polling —
// matchmaking happens instantly when a user enters the queue.
func (m *Matchmaker) processEvents() {
	for {
		select {
		case event := <-m.eventCh:
			switch event.Type {
			case EventJoin, EventRequeue:
				m.tryMatchUser(event.UserID)
			case EventLeave:
				// No action needed — user already removed from data structures
				// by the caller. We just drain the event.
			}
		case <-m.stopCh:
			log.Println("Matchmaker event processor stopped")
			return
		}
	}
}

// tryMatchUser attempts to find the best compatible partner for a given user
// in the search queue. It uses the tag reverse index for efficient candidate
// lookup, then ranks candidates by (similarity score, wait time) and pairs
// with the best match if the similarity threshold is met.
//
// Video mode: If the requesting user has VideoEnabled=true, they will only be
// matched with other video-enabled users. Chat-only users are filtered out.
func (m *Matchmaker) tryMatchUser(userID string) {
	m.mu.RLock()
	user, exists := m.searchingUsers[userID]
	if !exists || user.State != models.StateSearching {
		m.mu.RUnlock()
		return
	}
	userTags := user.Tags
	userWantsVideo := user.VideoEnabled
	m.mu.RUnlock()

	// Step 1: Use the tag reverse index to find candidate user IDs efficiently.
	candidateIDs := m.findCandidatesByTags(userTags, userID)

	if len(candidateIDs) == 0 {
		log.Printf("No candidates found for user %s (tags: %v)", userID, userTags)
		return
	}

	// Step 2: Score each candidate and rank them.
	// For video mode, filter out non-video candidates.
	var candidates []CandidateScore
	m.mu.RLock()
	for _, cid := range candidateIDs {
		candidate, exists := m.searchingUsers[cid]
		if !exists || candidate.State != models.StateSearching {
			continue
		}

		// Video mode compatibility: both must want video, or both must want chat
		if userWantsVideo != candidate.VideoEnabled {
			continue
		}

		compatible, similarity, shared := utils.IsMatchCompatible(
			userTags, candidate.Tags,
			m.config.MinSimilarity,
			m.config.MinSharedTags,
		)

		if compatible {
			candidates = append(candidates, CandidateScore{
				UserID:     cid,
				Similarity: similarity,
				SharedTags: shared,
				QueuedAt:   candidate.QueuedAt,
			})
		}
	}
	m.mu.RUnlock()

	if len(candidates) == 0 {
		log.Printf("No compatible match for user %s above threshold %.2f (video=%v)", userID, m.config.MinSimilarity, userWantsVideo)
		return
	}

	// Step 3: Rank candidates by similarity (desc), then by queue wait time (asc = longer wait first).
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Similarity != candidates[j].Similarity {
			return candidates[i].Similarity > candidates[j].Similarity
		}
		return candidates[i].QueuedAt.Before(candidates[j].QueuedAt)
	})

	best := candidates[0]

	// Step 4: Atomically create the match.
	created := m.atomicCreateMatch(userID, best.UserID, best.SharedTags, best.Similarity)
	if created {
		log.Printf("Match created: %s <-> %s (similarity: %.2f, shared: %v)",
			userID, best.UserID, best.Similarity, best.SharedTags)
	}
}

// findCandidatesByTags uses the tag reverse index to collect all user IDs
// that share at least one tag with the given user. The requesting user's own
// ID is excluded from the results.
func (m *Matchmaker) findCandidatesByTags(tags []string, excludeUserID string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	candidateSet := make(map[string]bool)
	for _, tag := range tags {
		if userIDs, exists := m.tagIndex[tag]; exists {
			for uid := range userIDs {
				if uid != excludeUserID {
					candidateSet[uid] = true
				}
			}
		}
	}

	candidates := make([]string, 0, len(candidateSet))
	for uid := range candidateSet {
		candidates = append(candidates, uid)
	}
	return candidates
}

// atomicCreateMatch safely creates a match between two users, ensuring no race
// conditions. It acquires the write lock, verifies both users are still searching,
// creates the match with video mode detection, and removes both users from the
// search queue and tag index.
// Returns true if the match was successfully created, false if either user was
// no longer available.
func (m *Matchmaker) atomicCreateMatch(user1ID, user2ID string, sharedTags []string, similarity float64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Re-verify both users are still searching
	user1, exists1 := m.searchingUsers[user1ID]
	user2, exists2 := m.searchingUsers[user2ID]

	if !exists1 || !exists2 {
		log.Printf("Atomic match failed: user1=%s exists=%v, user2=%s exists=%v",
			user1ID, exists1, user2ID, exists2)
		return false
	}

	if user1.State != models.StateSearching || user2.State != models.StateSearching {
		log.Printf("Atomic match failed: user1 state=%v, user2 state=%v",
			user1.State, user2.State)
		return false
	}

	// Create the match object
	match := models.NewMatch(user1ID, user2ID, sharedTags, similarity)
	match.User1QueuedAt = user1.QueuedAt
	match.User2QueuedAt = user2.QueuedAt

	// Determine mode: if both users want video, use video mode
	if user1.VideoEnabled && user2.VideoEnabled {
		match.Mode = "video"
		// Set video quality tier based on similarity
		if similarity >= 0.8 {
			match.VideoQuality = "hd"
		} else if similarity >= 0.5 {
			match.VideoQuality = "sd"
		} else {
			match.VideoQuality = "low"
		}
		// Randomize initiator to distribute WebRTC offer creation load
		if time.Now().UnixNano()%2 == 0 {
			match.Initiator = user1ID
		} else {
			match.Initiator = user2ID
		}
	} else {
		match.Mode = "chat"
		match.Initiator = ""
		match.VideoQuality = ""
	}

	// Store match indexed by both user IDs for O(1) lookup
	m.activeMatches[user1ID] = match
	m.activeMatches[user2ID] = match

	// Remove both users from the search queue
	delete(m.searchingUsers, user1ID)
	delete(m.searchingUsers, user2ID)

	// Remove both users from the tag reverse index
	m.removeFromTagIndexLocked(user1ID, user1.Tags)
	m.removeFromTagIndexLocked(user2ID, user2.Tags)

	// Update user states
	user1.State = models.StateMatched
	user1.MatchedWith = user2ID
	user2.State = models.StateMatched
	user2.MatchedWith = user1ID

	return true
}

// --- Public API Methods ---

// AddUser registers a connected user in the global users map.
// This does NOT add them to the search queue — call EnqueueUser for that.
func (m *Matchmaker) AddUser(user *models.User) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.users[user.ID] = user
	log.Printf("[Matchmaker] User %s registered (tags: %v, video=%v)", user.ID, user.Tags, user.VideoEnabled)
}

// RemoveUser completely removes a user from the system: the global users map,
// the search queue, the tag index, and any active matches. This is deadlock-free
// because it performs all removals under a single lock acquisition with no
// nested lock calls.
func (m *Matchmaker) RemoveUser(userID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	user, exists := m.users[userID]
	if !exists {
		return
	}

	// Remove from global users map
	delete(m.users, userID)

	// Remove from search queue and tag index if they were searching
	if _, inQueue := m.searchingUsers[userID]; inQueue {
		delete(m.searchingUsers, userID)
		m.removeFromTagIndexLocked(userID, user.Tags)
	}

	// Remove any active match
	if match, hasMatch := m.activeMatches[userID]; hasMatch {
		delete(m.activeMatches, match.User1ID)
		delete(m.activeMatches, match.User2ID)
	}

	log.Printf("[Matchmaker] User %s removed from all data structures", userID)
}

// EnqueueUser adds a user to the search queue and immediately triggers
// an event-driven match attempt. This is the primary entry point for
// matchmaking — it's called when a user joins or re-enters the queue.
//
// The flow is:
//  1. Add user to searchingUsers map
//  2. Add user's tags to the reverse index
//  3. Push a MatchEvent to the event channel
//  4. The processEvents goroutine picks it up instantly and calls tryMatchUser
//
// This ensures matchmaking happens immediately on join, without polling.
func (m *Matchmaker) EnqueueUser(user *models.User) {
	m.mu.Lock()
	user.State = models.StateSearching
	user.QueuedAt = time.Now()
	m.searchingUsers[user.ID] = user
	m.addToTagIndexLocked(user.ID, user.Tags)
	m.mu.Unlock()

	log.Printf("[Matchmaker] User %s enqueued (mode=%v, tags=%v) — triggering match event",
		user.ID, map[bool]string{true: "video", false: "chat"}[user.VideoEnabled], user.Tags)

	// Non-blocking send to event channel. If the channel is full (burst scenario),
	// we skip the event since the periodic cleanup and heartbeat will eventually
	// trigger re-evaluation. This prevents goroutine blocking.
	select {
	case m.eventCh <- MatchEvent{UserID: user.ID, Type: EventJoin}:
	default:
		log.Printf("[Matchmaker] Event channel full for user %s — match will be attempted on next event", user.ID)
	}
}

// DequeueUser removes a user from the search queue without removing them
// from the system entirely (e.g., when they cancel their search).
func (m *Matchmaker) DequeueUser(userID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	user, exists := m.searchingUsers[userID]
	if !exists {
		return
	}

	delete(m.searchingUsers, userID)
	m.removeFromTagIndexLocked(userID, user.Tags)
	user.State = models.StateConnected
	user.QueuedAt = time.Time{}

	log.Printf("[Matchmaker] User %s dequeued from search", userID)
}

// GetOnlineCount returns the total number of connected users.
func (m *Matchmaker) GetOnlineCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.users)
}

// GetSearchingCount returns the number of users currently in the search queue.
func (m *Matchmaker) GetSearchingCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.searchingUsers)
}

// GetUser retrieves a user by ID.
func (m *Matchmaker) GetUser(userID string) *models.User {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.users[userID]
}

// GetMatch retrieves an active match for a given user ID.
func (m *Matchmaker) GetMatch(userID string) *models.Match {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.activeMatches[userID]
}

// RemoveMatch dissolves an active match for a given user. It removes the match
// from both user indexes and resets the partner's state back to connected (but
// does NOT re-enqueue the partner — the caller is responsible for that via
// EnqueueUser or the skip handler).
func (m *Matchmaker) RemoveMatch(userID string) *models.Match {
	m.mu.Lock()
	defer m.mu.Unlock()

	match, exists := m.activeMatches[userID]
	if !exists {
		return nil
	}

	// Remove match from both user indexes
	delete(m.activeMatches, match.User1ID)
	delete(m.activeMatches, match.User2ID)

	// Reset both users' match state
	if user1, ok := m.users[match.User1ID]; ok {
		user1.State = models.StateConnected
		user1.MatchedWith = ""
	}
	if user2, ok := m.users[match.User2ID]; ok {
		user2.State = models.StateConnected
		user2.MatchedWith = ""
	}

	log.Printf("[Matchmaker] Match %s dissolved", match.ID)
	return match
}

// GetQueueStats returns statistics about the current search queue.
func (m *Matchmaker) GetQueueStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Count users per tag in the queue
	tagCounts := make(map[string]int)
	for tag := range m.tagIndex {
		tagCounts[tag] = len(m.tagIndex[tag])
	}

	// Count video vs chat users in queue
	videoSearching := 0
	chatSearching := 0
	for _, user := range m.searchingUsers {
		if user.VideoEnabled {
			videoSearching++
		} else {
			chatSearching++
		}
	}

	// Find longest wait time
	var longestWait time.Duration
	now := time.Now()
	for _, user := range m.searchingUsers {
		wait := now.Sub(user.QueuedAt)
		if wait > longestWait {
			longestWait = wait
		}
	}

	return map[string]interface{}{
		"searching_count":  len(m.searchingUsers),
		"video_searching":  videoSearching,
		"chat_searching":   chatSearching,
		"online_count":     len(m.users),
		"active_matches":   len(m.activeMatches) / 2, // Each match is stored twice
		"tag_distribution": tagCounts,
		"longest_wait_ms":  longestWait.Milliseconds(),
		"config": map[string]interface{}{
			"min_similarity":  m.config.MinSimilarity,
			"min_shared_tags": m.config.MinSharedTags,
			"queue_timeout_s": m.config.QueueTimeout.Seconds(),
		},
	}
}

// UpdateConfig allows runtime reconfiguration of matchmaking parameters.
func (m *Matchmaker) UpdateConfig(config models.MatchConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config = config
	log.Printf("[Matchmaker] Config updated: similarity=%.2f, min_shared=%d, timeout=%v",
		config.MinSimilarity, config.MinSharedTags, config.QueueTimeout)
}

// GetConfig returns the current matchmaking configuration.
func (m *Matchmaker) GetConfig() models.MatchConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

// --- Tag Index Operations (must be called with m.mu held) ---

// addToTagIndexLocked adds a user's tags to the reverse index.
// Caller must hold m.mu write lock.
func (m *Matchmaker) addToTagIndexLocked(userID string, tags []string) {
	for _, tag := range tags {
		if m.tagIndex[tag] == nil {
			m.tagIndex[tag] = make(map[string]bool)
		}
		m.tagIndex[tag][userID] = true
	}
}

// removeFromTagIndexLocked removes a user's tags from the reverse index.
// Caller must hold m.mu write lock.
func (m *Matchmaker) removeFromTagIndexLocked(userID string, tags []string) {
	for _, tag := range tags {
		if users, exists := m.tagIndex[tag]; exists {
			delete(users, userID)
			// Clean up empty tag entries to prevent memory leaks
			if len(users) == 0 {
				delete(m.tagIndex, tag)
			}
		}
	}
}

// --- Background Goroutines ---

// cleanupLoop periodically removes users who have been in the search queue
// beyond the configured timeout. This prevents indefinite waiting and
// memory leaks from abandoned connections.
func (m *Matchmaker) cleanupLoop() {
	ticker := time.NewTicker(m.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanupStaleUsers()
		case <-m.stopCh:
			return
		}
	}
}

// cleanupStaleUsers removes users from the search queue who have exceeded
// the queue timeout. These users may have disconnected without sending a
// proper close frame, or their client may have frozen.
func (m *Matchmaker) cleanupStaleUsers() {
	m.mu.Lock()
	now := time.Now()
	var expired []string

	for id, user := range m.searchingUsers {
		if now.Sub(user.QueuedAt) > m.config.QueueTimeout {
			expired = append(expired, id)
		}
	}

	for _, id := range expired {
		user := m.searchingUsers[id]
		m.removeFromTagIndexLocked(id, user.Tags)
		delete(m.searchingUsers, id)
		user.State = models.StateConnected
		log.Printf("[Matchmaker] User %s expired from queue after timeout", id)
	}
	m.mu.Unlock()

	if len(expired) > 0 {
		log.Printf("[Matchmaker] Cleaned up %d stale users from queue", len(expired))
	}
}

// heartbeatLoop periodically checks if user WebSocket connections are still
// alive by sending ping frames. Dead connections are removed.
func (m *Matchmaker) heartbeatLoop() {
	ticker := time.NewTicker(m.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.checkConnections()
		case <-m.stopCh:
			return
		}
	}
}

// checkConnections sends pings to all connected users and removes those
// whose connections are no longer responsive.
func (m *Matchmaker) checkConnections() {
	m.mu.RLock()
	users := make([]*models.User, 0, len(m.users))
	for _, user := range m.users {
		users = append(users, user)
	}
	m.mu.RUnlock()

	for _, user := range users {
		if err := user.SendPing(); err != nil {
			log.Printf("[Matchmaker] User %s connection dead (ping failed), removing", user.ID)
			go m.RemoveUser(user.ID)
		}
	}
}
