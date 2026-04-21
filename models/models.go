package models

import (
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// ============================================
// AUTH TYPES
// ============================================

type AuthProvider string

const (
	ProviderEmail    AuthProvider = "email"
	ProviderGoogle   AuthProvider = "google"
	ProviderFacebook AuthProvider = "facebook"
)

type RegisterRequest struct {
	Email       string   `json:"email"`
	Username    string   `json:"username"`
	DisplayName string   `json:"display_name"`
	Password    string   `json:"password"`
	Tags        []string `json:"tags"`
}

type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type AuthResponse struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	ExpiresAt    time.Time `json:"expires_at"`
	TokenType    string    `json:"token_type"`
	User         DBUser    `json:"user"`
}

type JWTClaims struct {
	UserID   string       `json:"user_id"`
	Email    string       `json:"email"`
	Provider AuthProvider `json:"provider"`
}

type OAuthUserInfo struct {
	ID        string `json:"id"`
	Email     string `json:"email"`
	Name      string `json:"name"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	AvatarURL string `json:"avatar_url"`
}

type OAuthCallbackRequest struct {
	Code  string `json:"code"`
	State string `json:"state"`
}

type RefreshTokenRequest struct {
	RefreshToken string `json:"refresh_token"`
}

// ============================================
// DATABASE MODELS (MongoDB)
// ============================================

type DBUser struct {
	ID            bson.ObjectID `json:"id" bson:"_id,omitempty"`
	Email         string        `json:"email" bson:"email"`
	PasswordHash  string        `json:"-" bson:"password_hash"`
	Username      string        `json:"username" bson:"username"`
	DisplayName   string        `json:"display_name" bson:"display_name"`
	Avatar        string        `json:"avatar" bson:"avatar,omitempty"`
	Tags          []string      `json:"tags" bson:"tags"`
	Provider      string        `json:"provider" bson:"provider"`
	ProviderID    string        `json:"provider_id" bson:"provider_id"`
	EmailVerified bool          `json:"email_verified" bson:"email_verified"`
	IsOnline      bool          `json:"is_online" bson:"is_online"`
	LastSeen      time.Time     `json:"last_seen" bson:"last_seen"`
	CreatedAt     time.Time     `json:"created_at" bson:"created_at"`
	UpdatedAt     time.Time     `json:"updated_at" bson:"updated_at"`
}

type StoredRefreshToken struct {
	ID        bson.ObjectID `json:"id" bson:"_id,omitempty"`
	UserID    string        `json:"user_id" bson:"user_id"`
	Token     string        `json:"token" bson:"token"`
	ExpiresAt time.Time     `json:"expires_at" bson:"expires_at"`
	Revoked   bool          `json:"revoked" bson:"revoked"`
	CreatedAt time.Time     `json:"created_at" bson:"created_at"`
}

type DBMessage struct {
	ID        bson.ObjectID `json:"id" bson:"_id,omitempty"`
	MatchID   string        `json:"match_id" bson:"match_id"`
	FromID    string        `json:"from_id" bson:"from_id"`
	ToID      string        `json:"to_id" bson:"to_id"`
	Content   string        `json:"content" bson:"content"`
	Timestamp time.Time     `json:"timestamp" bson:"timestamp"`
}

func NewDBUser(email, username, displayName string, provider AuthProvider) *DBUser {
	return &DBUser{
		Email:       strings.ToLower(email),
		Username:    username,
		DisplayName: displayName,
		Provider:    string(provider),
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
}

// ============================================
// WEBSOCKET & MATCHMAKING MODELS (In-Memory)
// ============================================

type UserState int

const (
	StateConnected UserState = iota
	StateSearching
	StateMatched
	StateDisconnected
)

func (s UserState) String() string {
	switch s {
	case StateConnected:
		return "connected"
	case StateSearching:
		return "searching"
	case StateMatched:
		return "matched"
	case StateDisconnected:
		return "disconnected"
	default:
		return "unknown"
	}
}

type Tag struct {
	Name     string `json:"name"`
	Icon     string `json:"icon"`
	Category string `json:"category"`
}

type User struct {
	ID          string          `json:"id"`
	Conn        *websocket.Conn `json:"-"`
	Tags        []string        `json:"tags"`
	MatchedWith string          `json:"matched_with,omitempty"`
	State       UserState       `json:"state"`
	JoinedAt    time.Time       `json:"joined_at"`
	QueuedAt    time.Time       `json:"queued_at,omitempty"`
	// Video call state
	VideoEnabled bool `json:"video_enabled"`
	mu           sync.Mutex
}

type Match struct {
	ID            string    `json:"id"`
	User1ID       string    `json:"user1_id"`
	User2ID       string    `json:"user2_id"`
	SharedTags    []string  `json:"shared_tags"`
	Similarity    float64   `json:"similarity"`
	CreatedAt     time.Time `json:"created_at"`
	User1QueuedAt time.Time `json:"user1_queued_at"`
	User2QueuedAt time.Time `json:"user2_queued_at"`
	// Video call metadata
	Mode         string `json:"mode"`      // "chat" or "video"
	Initiator    string `json:"initiator"` // UserID who creates the WebRTC offer
	VideoQuality string `json:"video_quality,omitempty"`
}

// PartnerID returns the other user's ID in the match
func (m *Match) PartnerID(userID string) string {
	if userID == m.User1ID {
		return m.User2ID
	}
	return m.User1ID
}

type Message struct {
	ID        string    `json:"id"`
	FromID    string    `json:"from_id"`
	ToID      string    `json:"to_id"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

// ============================================
// WEBSOCKET MESSAGE TYPES
// ============================================

// WebSocket message type constants
const (
	MsgTypeFindMatch    = "find_match"
	MsgTypeSearching    = "searching"
	MsgTypeMatchFound   = "match_found"
	MsgTypeChatMessage  = "chat_message"
	MsgTypeTyping       = "typing"
	MsgTypeSkip         = "skip"
	MsgTypeSkipped      = "skipped"
	MsgTypeDisconnected = "disconnected"
	MsgTypeReport       = "report"
	MsgTypeQueueUpdate  = "queue_update"
	MsgTypeError        = "error"
	MsgTypePong         = "pong"

	// Video call signaling types
	MsgTypeOffer        = "offer"
	MsgTypeAnswer       = "answer"
	MsgTypeICECandidate = "ice_candidate"
	MsgTypeVideoReady   = "video_ready"
	MsgTypePeerJoined   = "peer_joined"
	MsgTypeVideoToggle  = "video_toggle"
	MsgTypeAudioToggle  = "audio_toggle"
	MsgTypeEndCall      = "end_call"
)

type WebSocketMessage struct {
	Type      string      `json:"type"`
	Data      interface{} `json:"data"`
	ToID      string      `json:"to_id,omitempty"`
	FromID    string      `json:"from_id,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
}

type FindMatchRequest struct {
	Tags           []string `json:"tags"`
	MinSimilarity  float64  `json:"min_similarity,omitempty"`
	MaxWaitSeconds int      `json:"max_wait_seconds,omitempty"`
	Mode           string   `json:"mode,omitempty"` // "chat" or "video"
}

type SendMessageRequest struct {
	Content string `json:"content"`
	ToID    string `json:"to_id"`
}

type ReportRequest struct {
	Reason string `json:"reason"`
}

// WebRTC signaling payloads
type SDPOffer struct {
	SDP  string `json:"sdp"`
	Type string `json:"type"`
}

type SDPAnswer struct {
	SDP  string `json:"sdp"`
	Type string `json:"type"`
}

type ICECandidate struct {
	Candidate        string `json:"candidate"`
	SDPMid           string `json:"sdpMid"`
	SDPMLineIndex    int    `json:"sdpMLineIndex"`
	UsernameFragment string `json:"usernameFragment,omitempty"`
}

type VideoTogglePayload struct {
	Enabled bool `json:"enabled"`
}

type AudioTogglePayload struct {
	Enabled bool `json:"enabled"`
}

type MatchConfig struct {
	MinSimilarity     float64       `json:"min_similarity"`
	MinSharedTags     int           `json:"min_shared_tags"`
	QueueTimeout      time.Duration `json:"queue_timeout"`
	CleanupInterval   time.Duration `json:"cleanup_interval"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
}

func DefaultMatchConfig() MatchConfig {
	return MatchConfig{
		MinSimilarity:     0.25,
		MinSharedTags:     1,
		QueueTimeout:      5 * time.Minute,
		CleanupInterval:   30 * time.Second,
		HeartbeatInterval: 30 * time.Second,
	}
}

func (u *User) SendMessage(msg WebSocketMessage) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return u.Conn.WriteJSON(msg)
}

func (u *User) SendPing() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return u.Conn.WriteMessage(websocket.PingMessage, []byte{})
}

func NewUser(conn *websocket.Conn, tags []string) *User {
	return &User{
		ID:       uuid.New().String(),
		Conn:     conn,
		Tags:     tags,
		State:    StateConnected,
		JoinedAt: time.Now(),
	}
}

func NewMatch(user1ID, user2ID string, sharedTags []string, similarity float64) *Match {
	return &Match{
		ID:         uuid.New().String(),
		User1ID:    user1ID,
		User2ID:    user2ID,
		SharedTags: sharedTags,
		Similarity: similarity,
		CreatedAt:  time.Now(),
		Mode:       "chat", // Default to chat, can be overridden
		Initiator:  user1ID,
	}
}

func (dbu *DBUser) ToWSUser(conn *websocket.Conn) *User {
	return &User{
		ID:   dbu.ID.Hex(),
		Conn: conn,
		Tags: dbu.Tags,
	}
}
