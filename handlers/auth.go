package handlers

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"backend/middleware"
	"backend/models"
	"backend/services"
)

// AuthHandler provides HTTP endpoints for user authentication:
// registration, login, OAuth2 flows, token refresh, and profile access.
type AuthHandler struct {
	authService *services.AuthService
}

// NewAuthHandler creates a new AuthHandler with the singleton AuthService.
func NewAuthHandler() *AuthHandler {
	return &AuthHandler{
		authService: services.GetAuthService(),
	}
}

// ==================== EMAIL/PASSWORD AUTH ====================

// Register handles POST /api/auth/register
// Creates a new user account with email and password.
func (h *AuthHandler) Register(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST method required", http.StatusMethodNotAllowed)
		return
	}

	var req models.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	resp, err := h.authService.Register(req)
	if err != nil {
		log.Printf("[Auth] Registration failed: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	log.Printf("[Auth] Registration successful for %s", req.Email)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

// Login handles POST /api/auth/login
// Authenticates a user with email and password, returns JWT tokens.
func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST method required", http.StatusMethodNotAllowed)
		return
	}

	var req models.LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	resp, err := h.authService.Login(req)
	if err != nil {
		log.Printf("[Auth] Login failed for %s: %v", req.Email, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	log.Printf("[Auth] Login successful for %s", req.Email)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ==================== OAUTH2 ====================

// GoogleAuthURL handles GET /api/auth/google
// Returns the Google OAuth consent screen URL for the frontend to redirect to.
func (h *AuthHandler) GoogleAuthURL(w http.ResponseWriter, r *http.Request) {
	// Generate a random state token for CSRF protection
	state, err := generateStateToken()
	if err != nil {
		http.Error(w, "Failed to generate state token", http.StatusInternalServerError)
		return
	}

	authURL, err := h.authService.GetGoogleAuthURL(state)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"auth_url": authURL,
		"state":    state,
		"provider": "google",
	})
}

// GoogleCallback handles POST /api/auth/google/callback
// Exchanges the OAuth authorization code for user info and creates/logins the user.
func (h *AuthHandler) GoogleCallback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST method required", http.StatusMethodNotAllowed)
		return
	}

	var req models.OAuthCallbackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	if req.Code == "" {
		http.Error(w, "Authorization code is required", http.StatusBadRequest)
		return
	}

	resp, err := h.authService.HandleGoogleCallback(r.Context(), req.Code)
	if err != nil {
		log.Printf("[Auth] Google OAuth callback failed: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":   "Google authentication failed",
			"details": err.Error(),
		})
		return
	}

	log.Printf("[Auth] Google OAuth login successful for %s", resp.User.Email)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// FacebookAuthURL handles GET /api/auth/facebook
// Returns the Facebook OAuth consent screen URL for the frontend to redirect to.
func (h *AuthHandler) FacebookAuthURL(w http.ResponseWriter, r *http.Request) {
	state, err := generateStateToken()
	if err != nil {
		http.Error(w, "Failed to generate state token", http.StatusInternalServerError)
		return
	}

	authURL, err := h.authService.GetFacebookAuthURL(state)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"auth_url": authURL,
		"state":    state,
		"provider": "facebook",
	})
}

// FacebookCallback handles POST /api/auth/facebook/callback
// Exchanges the OAuth authorization code for user info and creates/logins the user.
func (h *AuthHandler) FacebookCallback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST method required", http.StatusMethodNotAllowed)
		return
	}

	var req models.OAuthCallbackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	if req.Code == "" {
		http.Error(w, "Authorization code is required", http.StatusBadRequest)
		return
	}

	resp, err := h.authService.HandleFacebookCallback(r.Context(), req.Code)
	if err != nil {
		log.Printf("[Auth] Facebook OAuth callback failed: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":   "Facebook authentication failed",
			"details": err.Error(),
		})
		return
	}

	log.Printf("[Auth] Facebook OAuth login successful for %s", resp.User.Email)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ==================== TOKEN MANAGEMENT ====================

// RefreshToken handles POST /api/auth/refresh
// Validates a refresh token and issues a new access/refresh token pair.
func (h *AuthHandler) RefreshToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST method required", http.StatusMethodNotAllowed)
		return
	}

	var req models.RefreshTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	if req.RefreshToken == "" {
		http.Error(w, "Refresh token is required", http.StatusBadRequest)
		return
	}

	resp, err := h.authService.RefreshToken(req.RefreshToken)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// Logout handles POST /api/auth/logout
// Revokes the provided refresh token.
func (h *AuthHandler) Logout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST method required", http.StatusMethodNotAllowed)
		return
	}

	var req models.RefreshTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// No body is fine — just revoke via Authorization header if present
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"message": "Logged out",
		})
		return
	}

	if req.RefreshToken != "" {
		h.authService.RevokeRefreshToken(req.RefreshToken)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message": "Logged out successfully",
	})
}

// ==================== PROTECTED ROUTES ====================

// GetMe handles GET /api/auth/me
// Returns the authenticated user's profile. Requires a valid JWT.
func (h *AuthHandler) GetMe(w http.ResponseWriter, r *http.Request) {
	userID := middleware.GetUserIDFromContext(r.Context())
	if userID == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	user, err := h.authService.GetUserByID(userID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(user)
}

// UpdateProfile handles PUT /api/auth/profile
// Updates the authenticated user's profile (display name, avatar, tags).
func (h *AuthHandler) UpdateProfile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "PUT method required", http.StatusMethodNotAllowed)
		return
	}

	userID := middleware.GetUserIDFromContext(r.Context())
	if userID == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var update struct {
		DisplayName string   `json:"display_name"`
		AvatarURL   string   `json:"avatar_url"`
		Tags        []string `json:"tags"`
	}
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	user, err := h.authService.UpdateUserProfile(userID, update.DisplayName, update.AvatarURL, update.Tags)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(user)
}

// GetOAuthStatus handles GET /api/auth/oauth/status
// Returns which OAuth providers are configured and available.
func (h *AuthHandler) GetOAuthStatus(w http.ResponseWriter, r *http.Request) {
	configs := h.authService.GetOAuthConfigs()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(configs)
}

// generateStateToken creates a cryptographically random state token for CSRF protection.
func generateStateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}

// writeJSONError is a helper to write a JSON error response.
func writeJSONError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error":     message,
		"code":      statusCode,
		"timestamp": time.Now(),
	})
}
