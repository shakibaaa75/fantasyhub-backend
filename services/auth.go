package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/facebook"
	"golang.org/x/oauth2/google"

	"backend/models"
	"backend/repositories"
)

type AuthConfig struct {
	JWTSecret           string
	JWTExpiry           time.Duration
	RefreshExpiry       time.Duration
	GoogleClientID      string
	GoogleClientSecret  string
	GoogleRedirectURL   string
	FacebookAppID       string
	FacebookAppSecret   string
	FacebookRedirectURL string
}

func DefaultAuthConfig() AuthConfig {
	return AuthConfig{
		JWTSecret:           getEnv("JWT_SECRET", "vibematch-dev-jwt-secret-change-in-production"),
		JWTExpiry:           15 * time.Minute,
		RefreshExpiry:       7 * 24 * time.Hour,
		GoogleClientID:      getEnv("GOOGLE_CLIENT_ID", ""),
		GoogleClientSecret:  getEnv("GOOGLE_CLIENT_SECRET", ""),
		GoogleRedirectURL:   getEnv("GOOGLE_REDIRECT_URL", "http://localhost:3000/auth/google/callback"),
		FacebookAppID:       getEnv("FACEBOOK_APP_ID", ""),
		FacebookAppSecret:   getEnv("FACEBOOK_APP_SECRET", ""),
		FacebookRedirectURL: getEnv("FACEBOOK_REDIRECT_URL", "http://localhost:3000/auth/facebook/callback"),
	}
}

var envCache sync.Map

func getEnv(key, fallback string) string {
	if v, ok := envCache.Load(key); ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return fallback
}

func SetEnv(key, value string) {
	envCache.Store(key, value)
}

type AuthService struct {
	config              AuthConfig
	userRepo            *repositories.UserRepository
	tokenRepo           *repositories.TokenRepository
	googleOAuthConfig   *oauth2.Config
	facebookOAuthConfig *oauth2.Config
	mu                  sync.RWMutex
}

var authInstance *AuthService
var authOnce sync.Once

func GetAuthService() *AuthService {
	authOnce.Do(func() {
		config := DefaultAuthConfig()
		authInstance = newAuthService(config)
	})
	return authInstance
}

func GetAuthServiceWithConfig(config AuthConfig) *AuthService {
	authOnce.Do(func() {
		authInstance = newAuthService(config)
	})
	return authInstance
}

func newAuthService(config AuthConfig) *AuthService {
	svc := &AuthService{
		config:    config,
		userRepo:  repositories.NewUserRepository(),
		tokenRepo: repositories.NewTokenRepository(),
	}

	if config.GoogleClientID != "" && config.GoogleClientSecret != "" {
		svc.googleOAuthConfig = &oauth2.Config{
			ClientID:     config.GoogleClientID,
			ClientSecret: config.GoogleClientSecret,
			RedirectURL:  config.GoogleRedirectURL,
			Scopes:       []string{"https://www.googleapis.com/auth/userinfo.email", "https://www.googleapis.com/auth/userinfo.profile"},
			Endpoint:     google.Endpoint,
		}
		log.Println("[Auth] Google OAuth2 configured")
	}

	if config.FacebookAppID != "" && config.FacebookAppSecret != "" {
		svc.facebookOAuthConfig = &oauth2.Config{
			ClientID:     config.FacebookAppID,
			ClientSecret: config.FacebookAppSecret,
			RedirectURL:  config.FacebookRedirectURL,
			Scopes:       []string{"email", "public_profile"},
			Endpoint:     facebook.Endpoint,
		}
		log.Println("[Auth] Facebook OAuth2 configured")
	}

	go func() {
		if err := svc.userRepo.CreateIndex(); err != nil {
			log.Printf("[Auth] Failed to create user indexes: %v", err)
		}
		if err := svc.tokenRepo.CreateIndex(); err != nil {
			log.Printf("[Auth] Failed to create token indexes: %v", err)
		}
	}()

	log.Println("[Auth] Service initialized (MongoDB)")
	return svc
}

// ==================== REGISTRATION ====================

func (s *AuthService) Register(req models.RegisterRequest) (*models.AuthResponse, error) {
	if req.Email == "" || req.Username == "" {
		return nil, errors.New("email and username are required")
	}
	if len(req.Password) < 8 {
		return nil, errors.New("password must be at least 8 characters")
	}

	req.Email = strings.ToLower(strings.TrimSpace(req.Email))
	req.Username = strings.TrimSpace(req.Username)

	if _, err := s.userRepo.FindByEmail(req.Email); err == nil {
		return nil, errors.New("email already registered")
	}
	if _, err := s.userRepo.FindByUsername(req.Username); err == nil {
		return nil, errors.New("username already taken")
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("failed to hash password: %w", err)
	}

	displayName := req.DisplayName
	if displayName == "" {
		displayName = req.Username
	}

	user := models.NewDBUser(req.Email, req.Username, displayName, models.ProviderEmail)
	user.PasswordHash = string(hash)
	if len(req.Tags) > 0 {
		user.Tags = req.Tags
	}

	if err := s.userRepo.Create(user); err != nil {
		return nil, fmt.Errorf("failed to create user: %w", err)
	}

	log.Printf("[Auth] User registered: %s (%s)", user.ID.Hex(), user.Email)
	return s.generateTokenPair(user)
}

// ==================== LOGIN ====================

func (s *AuthService) Login(req models.LoginRequest) (*models.AuthResponse, error) {
	if req.Email == "" || req.Password == "" {
		return nil, errors.New("email and password are required")
	}

	user, err := s.userRepo.FindByEmail(strings.ToLower(req.Email))
	if err != nil {
		return nil, errors.New("invalid email or password")
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		return nil, errors.New("invalid email or password")
	}

	s.userRepo.SetOnlineStatus(user.ID.Hex(), true)
	user.IsOnline = true

	log.Printf("[Auth] User logged in: %s (%s)", user.ID.Hex(), user.Email)
	return s.generateTokenPair(user)
}

// ==================== OAUTH2 ====================

func (s *AuthService) GetGoogleAuthURL(state string) (string, error) {
	if s.googleOAuthConfig == nil {
		return "", errors.New("Google OAuth is not configured")
	}
	return s.googleOAuthConfig.AuthCodeURL(state, oauth2.AccessTypeOffline), nil
}

func (s *AuthService) HandleGoogleCallback(ctx context.Context, code string) (*models.AuthResponse, error) {
	if s.googleOAuthConfig == nil {
		return nil, errors.New("Google OAuth is not configured")
	}
	token, err := s.googleOAuthConfig.Exchange(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("failed to exchange Google OAuth code: %w", err)
	}
	userInfo, err := s.fetchGoogleUserInfo(ctx, token.AccessToken)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch Google user info: %w", err)
	}
	return s.findOrCreateOAuthUser(userInfo, models.ProviderGoogle)
}

func (s *AuthService) GetFacebookAuthURL(state string) (string, error) {
	if s.facebookOAuthConfig == nil {
		return "", errors.New("Facebook OAuth is not configured")
	}
	return s.facebookOAuthConfig.AuthCodeURL(state), nil
}

func (s *AuthService) HandleFacebookCallback(ctx context.Context, code string) (*models.AuthResponse, error) {
	if s.facebookOAuthConfig == nil {
		return nil, errors.New("Facebook OAuth is not configured")
	}
	token, err := s.facebookOAuthConfig.Exchange(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("failed to exchange Facebook OAuth code: %w", err)
	}
	userInfo, err := s.fetchFacebookUserInfo(ctx, token.AccessToken)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch Facebook user info: %w", err)
	}
	return s.findOrCreateOAuthUser(userInfo, models.ProviderFacebook)
}

func (s *AuthService) fetchGoogleUserInfo(ctx context.Context, accessToken string) (*models.OAuthUserInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://www.googleapis.com/oauth2/v2/userinfo", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Google API returned status %d: %s", resp.StatusCode, string(body))
	}

	var gUser struct {
		ID      string `json:"id"`
		Email   string `json:"email"`
		Name    string `json:"name"`
		Picture string `json:"picture"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&gUser); err != nil {
		return nil, fmt.Errorf("failed to decode Google user info: %w", err)
	}

	return &models.OAuthUserInfo{ID: gUser.ID, Email: gUser.Email, Name: gUser.Name, AvatarURL: gUser.Picture}, nil
}

func (s *AuthService) fetchFacebookUserInfo(ctx context.Context, accessToken string) (*models.OAuthUserInfo, error) {
	url := fmt.Sprintf("https://graph.facebook.com/v18.0/me?fields=id,email,name,first_name,last_name,picture.width(200).height(200)&access_token=%s", accessToken)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Facebook API returned status %d: %s", resp.StatusCode, string(body))
	}

	var fbUser struct {
		ID        string `json:"id"`
		Email     string `json:"email"`
		Name      string `json:"name"`
		FirstName string `json:"first_name"`
		LastName  string `json:"last_name"`
		Picture   struct {
			Data struct {
				URL string `json:"url"`
			} `json:"data"`
		} `json:"picture"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&fbUser); err != nil {
		return nil, fmt.Errorf("failed to decode Facebook user info: %w", err)
	}

	return &models.OAuthUserInfo{
		ID: fbUser.ID, Email: fbUser.Email, Name: fbUser.Name,
		FirstName: fbUser.FirstName, LastName: fbUser.LastName, AvatarURL: fbUser.Picture.Data.URL,
	}, nil
}

func (s *AuthService) findOrCreateOAuthUser(info *models.OAuthUserInfo, provider models.AuthProvider) (*models.AuthResponse, error) {
	email := strings.ToLower(strings.TrimSpace(info.Email))
	providerStr := string(provider)

	// 1. Check if user exists with this provider + provider ID
	user, err := s.userRepo.FindByOAuthProvider(providerStr, info.ID)
	if err == nil {
		s.userRepo.SetOnlineStatus(user.ID.Hex(), true)
		log.Printf("[Auth] OAuth user re-login: %s (%s)", user.ID.Hex(), email)
		return s.generateTokenPair(user)
	}

	// 2. Check if user exists with the same email (link accounts)
	if email != "" {
		user, err = s.userRepo.FindByEmail(email)
		if err == nil {
			s.userRepo.UpdateProvider(user.ID.Hex(), providerStr, info.ID)
			if user.Avatar == "" && info.AvatarURL != "" {
				s.userRepo.UpdateProfile(user.ID.Hex(), bson.M{"avatar": info.AvatarURL})
			}
			s.userRepo.SetOnlineStatus(user.ID.Hex(), true)
			log.Printf("[Auth] OAuth account linked for user: %s (%s)", user.ID.Hex(), email)
			user, _ = s.userRepo.FindByID(user.ID.Hex())
			return s.generateTokenPair(user)
		}
	}

	// 3. Create a brand new user
	username := s.generateUniqueUsername(info.Name, info.FirstName, info.LastName)
	displayName := info.Name
	if displayName == "" {
		displayName = username
	}

	newUser := models.NewDBUser(email, username, displayName, provider)
	newUser.ProviderID = info.ID
	newUser.Avatar = info.AvatarURL
	newUser.EmailVerified = email != ""
	newUser.IsOnline = true

	if err := s.userRepo.Create(newUser); err != nil {
		return nil, fmt.Errorf("failed to create OAuth user: %w", err)
	}

	log.Printf("[Auth] New OAuth user created: %s (%s via %s)", newUser.ID.Hex(), email, provider)
	return s.generateTokenPair(newUser)
}

func (s *AuthService) generateUniqueUsername(name, firstName, lastName string) string {
	base := name
	if base == "" {
		base = firstName
	}
	if base == "" {
		base = "user"
	}

	base = strings.ToLower(base)
	base = strings.ReplaceAll(base, " ", "_")
	base = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, base)

	if len(base) > 20 {
		base = base[:20]
	}

	candidate := base
	for i := 0; i < 100; i++ {
		if _, err := s.userRepo.FindByUsername(candidate); err != nil {
			return candidate
		}
		candidate = fmt.Sprintf("%s_%d", base, i+1)
	}

	return base + "_" + uuid.New().String()[:8]
}

// ==================== JWT TOKEN MANAGEMENT ====================

func (s *AuthService) generateTokenPair(user *models.DBUser) (*models.AuthResponse, error) {
	now := time.Now()
	expiresAt := now.Add(s.config.JWTExpiry)

	claims := jwt.MapClaims{
		"user_id":  user.ID.Hex(),
		"email":    user.Email,
		"provider": user.Provider,
		"iat":      now.Unix(),
		"exp":      expiresAt.Unix(),
		"jti":      uuid.New().String(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	accessToken, err := token.SignedString([]byte(s.config.JWTSecret))
	if err != nil {
		return nil, fmt.Errorf("failed to sign access token: %w", err)
	}

	refreshTokenStr := uuid.New().String() + uuid.New().String()
	storedRefresh := &models.StoredRefreshToken{
		UserID:    user.ID.Hex(),
		Token:     refreshTokenStr,
		ExpiresAt: now.Add(s.config.RefreshExpiry),
	}

	if err := s.tokenRepo.Store(storedRefresh); err != nil {
		return nil, fmt.Errorf("failed to store refresh token: %w", err)
	}

	return &models.AuthResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshTokenStr,
		ExpiresAt:    expiresAt,
		TokenType:    "Bearer",
		User:         *user,
	}, nil
}

func (s *AuthService) ValidateAccessToken(tokenString string) (*models.JWTClaims, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(s.config.JWTSecret), nil
	})

	if err != nil {
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token claims")
	}

	userID, _ := claims["user_id"].(string)
	email, _ := claims["email"].(string)
	provider, _ := claims["provider"].(string)

	if userID == "" {
		return nil, errors.New("token missing user_id claim")
	}

	return &models.JWTClaims{
		UserID:   userID,
		Email:    email,
		Provider: models.AuthProvider(provider),
	}, nil
}

func (s *AuthService) RefreshToken(refreshToken string) (*models.AuthResponse, error) {
	stored, err := s.tokenRepo.FindByToken(refreshToken)
	if err != nil {
		return nil, errors.New("invalid refresh token")
	}
	if stored.Revoked {
		return nil, errors.New("refresh token has been revoked")
	}
	if time.Now().After(stored.ExpiresAt) {
		return nil, errors.New("refresh token has expired")
	}

	s.tokenRepo.RevokeToken(refreshToken)

	user, err := s.userRepo.FindByID(stored.UserID)
	if err != nil {
		return nil, errors.New("user not found")
	}

	return s.generateTokenPair(user)
}

func (s *AuthService) RevokeRefreshToken(refreshToken string) {
	if err := s.tokenRepo.RevokeToken(refreshToken); err != nil {
		log.Printf("[Auth] Failed to revoke refresh token: %v", err)
	}
}

// ==================== USER OPERATIONS ====================

func (s *AuthService) GetUserByID(userID string) (*models.DBUser, error) {
	return s.userRepo.FindByID(userID)
}

func (s *AuthService) UpdateUserProfile(userID string, displayName string, avatarURL string, tags []string) (*models.DBUser, error) {
	updates := bson.M{}
	if displayName != "" {
		updates["display_name"] = displayName
	}
	if avatarURL != "" {
		updates["avatar"] = avatarURL
	}
	if tags != nil {
		updates["tags"] = tags
	}

	if len(updates) > 0 {
		if err := s.userRepo.UpdateProfile(userID, updates); err != nil {
			return nil, err
		}
	}

	return s.userRepo.FindByID(userID)
}

func (s *AuthService) SetUserOnline(userID string, online bool) {
	s.userRepo.SetOnlineStatus(userID, online)
}

func (s *AuthService) GetOAuthConfigs() map[string]interface{} {
	config := map[string]interface{}{
		"google_configured":   s.googleOAuthConfig != nil,
		"facebook_configured": s.facebookOAuthConfig != nil,
	}
	if s.googleOAuthConfig != nil {
		config["google_redirect_url"] = s.googleOAuthConfig.RedirectURL
	}
	if s.facebookOAuthConfig != nil {
		config["facebook_redirect_url"] = s.facebookOAuthConfig.RedirectURL
	}
	return config
}

func (s *AuthService) CleanupExpiredTokens() {
	if err := s.tokenRepo.DeleteExpired(); err != nil {
		log.Printf("[Auth] Failed to cleanup expired tokens: %v", err)
	} else {
		log.Println("[Auth] Cleaned up expired/revoked refresh tokens")
	}
}

func (s *AuthService) StartCleanupLoop() {
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			s.CleanupExpiredTokens()
		}
	}()
	log.Println("[Auth] Refresh token cleanup loop started")
}
