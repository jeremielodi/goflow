// pkg/authentication/jwt.go
package authentication

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

const (
	Issuer = "weave.io"
)

type CustomClaims struct {
	UserId    string `json:"user_id"`
	Email     string `json:"email,omitempty"`
	TokenType string `json:"token_type"` // "access" or "refresh"
	jwt.RegisteredClaims
}

type TokenPair struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
}

type JWTService struct {
	db                    *sqlx.DB
	userRepo              *repository.UserRepository
	signKey               []byte
	refreshSignKey        []byte
	issuer                string
	accessExpireDuration  time.Duration
	refreshExpireDuration time.Duration
}

// NewJWTService creates a new JWT service with database access
func NewJWTService(db *sqlx.DB, secret string) *JWTService {
	return &JWTService{
		db:                    db,
		userRepo:              repository.NewUserRepository(db),
		signKey:               []byte(secret),
		refreshSignKey:        []byte(secret + "_refresh"),
		issuer:                Issuer,
		accessExpireDuration:  15 * time.Minute,   // Access token short-lived
		refreshExpireDuration: 7 * 24 * time.Hour, // Refresh token long-lived
	}
}

// CreateTokenPair creates both access and refresh tokens
func (s *JWTService) CreateTokenPair(user models.User) (*TokenPair, error) {
	accessToken, err := s.CreateAccessToken(user)
	if err != nil {
		return nil, err
	}

	refreshToken, err := s.CreateRefreshToken(user)
	if err != nil {
		return nil, err
	}

	return &TokenPair{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		ExpiresIn:    int64(s.accessExpireDuration.Seconds()),
	}, nil
}

// CreateAccessToken creates a short-lived access token
func (s *JWTService) CreateAccessToken(user models.User) (string, error) {
	now := time.Now()
	token := jwt.NewWithClaims(
		jwt.SigningMethodHS256,
		CustomClaims{
			UserId:    user.ID.String(),
			Email:     user.Email,
			TokenType: "access",
			RegisteredClaims: jwt.RegisteredClaims{
				ExpiresAt: jwt.NewNumericDate(now.Add(s.accessExpireDuration)), // Fixed: removed 3x multiplier
				NotBefore: jwt.NewNumericDate(now),
				ID:        uuid.New().String(),
				Issuer:    s.issuer,
			},
		},
	)

	return token.SignedString(s.signKey)
}

// CreateRefreshToken creates a long-lived refresh token
func (s *JWTService) CreateRefreshToken(user models.User) (string, error) {
	now := time.Now()
	token := jwt.NewWithClaims(
		jwt.SigningMethodHS256,
		CustomClaims{
			UserId:    user.ID.String(),
			TokenType: "refresh",
			RegisteredClaims: jwt.RegisteredClaims{
				ExpiresAt: jwt.NewNumericDate(now.Add(s.refreshExpireDuration)),
				NotBefore: jwt.NewNumericDate(now),
				ID:        uuid.New().String(),
				Issuer:    s.issuer,
			},
		},
	)

	return token.SignedString(s.refreshSignKey)
}

// ParseAccessToken parses and validates access token
func (s *JWTService) ParseAccessToken(tokenString string) (string, error) {
	// Parse token
	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		// Validate signing method
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return s.signKey, nil
	})

	if err != nil {
		return "", fmt.Errorf("failed to parse token: %w", err)
	}

	// Validate claims
	claims, ok := token.Claims.(*CustomClaims)
	if !ok {
		return "", fmt.Errorf("invalid token claims")
	}

	if !token.Valid {
		return "", fmt.Errorf("token is invalid")
	}

	// Check token type
	if claims.TokenType != "access" {
		return "", fmt.Errorf("invalid token type, expected access token, got: %s", claims.TokenType)
	}

	// Check expiration
	if claims.ExpiresAt != nil && claims.ExpiresAt.Time.Before(time.Now()) {
		return "", fmt.Errorf("token has expired")
	}

	return claims.UserId, nil
}

// ParseRefreshToken parses and validates refresh token
func (s *JWTService) ParseRefreshToken(tokenString string) (string, error) {
	// Parse token
	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		// Validate signing method
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return s.refreshSignKey, nil
	})

	if err != nil {
		return "", fmt.Errorf("failed to parse refresh token: %w", err)
	}

	// Validate claims
	claims, ok := token.Claims.(*CustomClaims)
	if !ok {
		return "", fmt.Errorf("invalid refresh token claims")
	}

	if !token.Valid {
		return "", fmt.Errorf("refresh token is invalid")
	}

	// Check token type
	if claims.TokenType != "refresh" {
		return "", fmt.Errorf("invalid token type, expected refresh token, got: %s", claims.TokenType)
	}

	// Check expiration
	if claims.ExpiresAt != nil && claims.ExpiresAt.Time.Before(time.Now()) {
		return "", fmt.Errorf("refresh token has expired")
	}

	return claims.UserId, nil
}

// RefreshAccessToken creates a new access token from a valid refresh token
func (s *JWTService) RefreshAccessToken(refreshToken string) (*TokenPair, error) {
	// Parse and validate refresh token
	userID, err := s.ParseRefreshToken(refreshToken)
	if err != nil {
		return nil, fmt.Errorf("invalid refresh token: %w", err)
	}

	// Parse user ID to UUID
	userUUID, err := uuid.Parse(userID)
	if err != nil {
		return nil, fmt.Errorf("invalid user ID in token: %w", err)
	}

	// Get user from database
	user, err := s.userRepo.FindByID(userUUID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch user: %w", err)
	}

	// Check if user is empty (not found)
	if user.ID == uuid.Nil {
		return nil, fmt.Errorf("user not found")
	}

	// Check if user is active
	if !user.IsActive {
		return nil, fmt.Errorf("user account is inactive")
	}

	// Generate new token pair
	return s.CreateTokenPair(user)
}

// ValidateAndGetUser validates a token and returns the user
func (s *JWTService) ValidateAndGetUser(tokenString string) (*models.User, error) {
	// Parse token
	userID, err := s.ParseAccessToken(tokenString)
	if err != nil {
		return nil, err
	}

	// Parse user ID to UUID
	userUUID, err := uuid.Parse(userID)
	if err != nil {
		return nil, fmt.Errorf("invalid user ID in token: %w", err)
	}

	// Get user from database
	user, err := s.userRepo.FindByID(userUUID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch user: %w", err)
	}

	// Check if user is empty (not found)
	if user.ID == uuid.Nil {
		return nil, fmt.Errorf("user not found")
	}

	// Check if user is active
	if !user.IsActive {
		return nil, fmt.Errorf("user account is inactive")
	}

	return &user, nil
}

// GetUserIDFromToken extracts user ID from token without database lookup
func (s *JWTService) GetUserIDFromToken(tokenString string) (string, error) {
	return s.ParseAccessToken(tokenString)
}
