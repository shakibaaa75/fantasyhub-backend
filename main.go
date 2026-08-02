package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/rs/cors"

	"backend/config"
	"backend/handlers"
)

func main() {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Println("⚠️  No .env file found, using system environment variables")
	}

	// Connect to MongoDB
	if err := config.ConnectDB(); err != nil {
		log.Fatalf("❌ Database connection failed: %v", err)
	}

	// Initialize handlers
	wsHandler := handlers.NewWebSocketHandler()
	matchHandler := handlers.NewMatchHandler()
	authHandler := handlers.NewAuthHandler()

	mux := http.NewServeMux()

	// WebSocket endpoint (handles chat + video call signaling)
	mux.HandleFunc("/ws", wsHandler.HandleWebSocket)

	// REST API endpoints
	mux.HandleFunc("/api/match/status", matchHandler.GetMatchStatus)
	mux.HandleFunc("/api/online/count", matchHandler.GetOnlineCount)
	mux.HandleFunc("/api/tags", matchHandler.GetTags)
	mux.HandleFunc("/api/queue/stats", matchHandler.GetQueueStats)
	mux.HandleFunc("/api/config", matchHandler.GetConfig)
	mux.HandleFunc("/api/config/update", matchHandler.UpdateConfig)

	// Auth endpoints
	mux.HandleFunc("/api/auth/register", authHandler.Register)
	mux.HandleFunc("/api/auth/login", authHandler.Login)
	mux.HandleFunc("/api/auth/google", authHandler.GoogleAuthURL)
	mux.HandleFunc("/api/auth/google/callback", authHandler.GoogleCallback)
	mux.HandleFunc("/api/auth/facebook", authHandler.FacebookAuthURL)
	mux.HandleFunc("/api/auth/facebook/callback", authHandler.FacebookCallback)
	mux.HandleFunc("/api/auth/refresh", authHandler.RefreshToken)
	mux.HandleFunc("/api/auth/logout", authHandler.Logout)
	mux.HandleFunc("/api/auth/me", authHandler.GetMe)
	mux.HandleFunc("/api/auth/profile", authHandler.UpdateProfile)
	mux.HandleFunc("/api/auth/oauth/status", authHandler.GetOAuthStatus)

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Setup CORS (Dynamic for deployment)
	allowedOrigins := os.Getenv("ALLOWED_ORIGINS")
	if allowedOrigins == "" {
		// Fallback for local development
		allowedOrigins = "http://localhost:3000,http://localhost:3001"
	}

	corsHandler := cors.New(cors.Options{
		AllowedOrigins:   strings.Split(allowedOrigins, ","),
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Content-Type", "X-CSRF-Token", "Authorization"},
		AllowCredentials: true,
	})

	handler := corsHandler.Handler(mux)

	// Render (and most PaaS providers) assign the port dynamically via $PORT.
	// Falls back to 8080 for local development.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Create HTTP server with timeouts
	server := &http.Server{
		Addr:         ":" + port,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in goroutine
	go func() {
		printBanner(port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("❌ Server failed to start: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("\n🛑 Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("❌ Server forced to shutdown: %v", err)
	}

	// Disconnect from database
	if err := config.DisconnectDB(); err != nil {
		log.Printf("❌ Database disconnect error: %v", err)
	}

	log.Println("✅ Server stopped gracefully")
}

func printBanner(port string) {
	log.Println("========================================")
	log.Println("  Event-Driven Matchmaking Server")
	log.Println("========================================")
	log.Printf("Server starting on :%s\n", port)
	log.Println("")
	log.Println("WebSocket endpoint:")
	log.Printf("  ws://localhost:%s/ws  (chat + video call signaling)\n", port)
	log.Println("")
	log.Println("REST API endpoints:")
	log.Println("  GET  /api/tags?mode=video - Available interest tags (+ video tags)")
	log.Println("  GET  /api/online/count     - Connected user count")
	log.Println("  GET  /api/match/status     - Match status (query: user_id)")
	log.Println("  GET  /api/queue/stats      - Queue statistics & config")
	log.Println("  GET  /api/config           - Current matchmaking config")
	log.Println("  POST /api/config/update    - Update config at runtime")
	log.Println("")
	log.Println("Auth endpoints:")
	log.Println("  POST /api/auth/register          - Email/password registration")
	log.Println("  POST /api/auth/login             - Email/password login")
	log.Println("  GET  /api/auth/google            - Google OAuth URL")
	log.Println("  POST /api/auth/google/callback    - Google OAuth callback")
	log.Println("  GET  /api/auth/facebook          - Facebook OAuth URL")
	log.Println("  POST /api/auth/facebook/callback - Facebook OAuth callback")
	log.Println("  POST /api/auth/refresh           - Refresh access token")
	log.Println("  POST /api/auth/logout            - Revoke refresh token")
	log.Println("  GET  /api/auth/me                - Get current user profile")
	log.Println("  PUT  /api/auth/profile           - Update user profile")
	log.Println("  GET  /api/auth/oauth/status      - OAuth provider status")
	log.Println("")
	log.Println("Health check:")
	log.Println("  GET  /health              - Health check")
	log.Println("")
	log.Println("Architecture: Event-driven (channel-based, no polling)")
	log.Println("Matching: Jaccard similarity with configurable threshold")
	log.Println("Video: WebRTC P2P signaling via WebSocket (STUN/TURN ready)")
	log.Println("Database: MongoDB (Fantisy cluster)")
	log.Println("========================================")
}
