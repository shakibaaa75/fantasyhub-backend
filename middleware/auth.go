package middleware

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"backend/services"
)

// contextKey is an unexported type for context keys defined in this package.
// This prevents collisions with context keys defined in other packages.
type contextKey string

const (
	contextKeyUserID   contextKey = "user_id"
	contextKeyEmail    contextKey = "email"
	contextKeyProvider contextKey = "provider"
)

// GetUserIDFromContext extracts the authenticated user's ID from the request context.
// This is the recommended way for handlers to access the user ID set by JWTAuth.
func GetUserIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(contextKeyUserID).(string); ok {
		return v
	}
	return ""
}

// GetEmailFromContext extracts the authenticated user's email from the request context.
func GetEmailFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(contextKeyEmail).(string); ok {
		return v
	}
	return ""
}

// GetProviderFromContext extracts the auth provider from the request context.
func GetProviderFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(contextKeyProvider).(string); ok {
		return v
	}
	return ""
}

// JWTAuth is an HTTP middleware that validates JWT access tokens from the
// Authorization header. If the token is valid, user information is injected
// into the request context for downstream handlers to access.
//
// Usage:
//
//	protectedRoute := middleware.JWTAuth(myHandler)
//
// The downstream handler can retrieve user info via:
//
//	userID := middleware.GetUserIDFromContext(r.Context())
func JWTAuth(next http.Handler) http.Handler {
	authService := services.GetAuthService()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract Authorization header
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			writeAuthError(w, "Missing Authorization header", http.StatusUnauthorized)
			return
		}

		// Parse "Bearer <token>" format
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			writeAuthError(w, "Invalid Authorization header format — expected: Bearer <token>", http.StatusUnauthorized)
			return
		}

		tokenString := strings.TrimSpace(parts[1])
		if tokenString == "" {
			writeAuthError(w, "Empty access token", http.StatusUnauthorized)
			return
		}

		// Validate the token
		claims, err := authService.ValidateAccessToken(tokenString)
		if err != nil {
			log.Printf("[Auth] Token validation failed: %v", err)
			writeAuthError(w, "Invalid or expired access token", http.StatusUnauthorized)
			return
		}

		// Verify the user still exists
		user, err := authService.GetUserByID(claims.UserID)
		if err != nil {
			writeAuthError(w, "User account not found", http.StatusUnauthorized)
			return
		}

		// Inject user information into the request context
		ctx := context.WithValue(r.Context(), contextKeyUserID, user.ID)
		ctx = context.WithValue(ctx, contextKeyEmail, user.Email)
		ctx = context.WithValue(ctx, contextKeyProvider, string(user.Provider))

		// Call the next handler with the enriched context
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// OptionalJWTAuth is like JWTAuth but does not reject requests without a token.
// If a valid token is present, user info is injected into the context.
// If no token or an invalid token is present, the request continues without auth.
// This is useful for endpoints that behave differently for authenticated vs.
// anonymous users (e.g., the matchmaking WebSocket connection).
func OptionalJWTAuth(next http.Handler) http.Handler {
	authService := services.GetAuthService()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			next.ServeHTTP(w, r)
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			next.ServeHTTP(w, r)
			return
		}

		tokenString := strings.TrimSpace(parts[1])
		if tokenString == "" {
			next.ServeHTTP(w, r)
			return
		}

		claims, err := authService.ValidateAccessToken(tokenString)
		if err != nil {
			next.ServeHTTP(w, r)
			return
		}

		user, err := authService.GetUserByID(claims.UserID)
		if err != nil {
			next.ServeHTTP(w, r)
			return
		}

		ctx := context.WithValue(r.Context(), contextKeyUserID, user.ID)
		ctx = context.WithValue(ctx, contextKeyEmail, user.Email)
		ctx = context.WithValue(ctx, contextKeyProvider, string(user.Provider))

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// writeAuthError writes a JSON-formatted authentication error response.
func writeAuthError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error":     message,
		"code":      statusCode,
		"timestamp": time.Now(),
	})
}
