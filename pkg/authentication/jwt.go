package authentication

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/jeremielodi/goflow/internal/models"
)

const (
	Issuer = "weave.io"
)

type CustomClaims struct {
	UserId string
	jwt.RegisteredClaims
}

type JWTService struct {
	signKey        []byte
	issuer         string
	expireDuration time.Duration
}

func NewJWTService(secret string) *JWTService {
	return &JWTService{
		signKey:        []byte(secret),
		issuer:         Issuer,
		expireDuration: 7 * 24 * time.Hour,
	}
}

func (s *JWTService) CreateToken(user models.User) (string, error) {

	now := time.Now()
	//fmt.Println("new use as token", user)
	token := jwt.NewWithClaims(
		jwt.SigningMethodHS256,
		CustomClaims{
			UserId: user.ID.String(),
			RegisteredClaims: jwt.RegisteredClaims{
				ExpiresAt: jwt.NewNumericDate(now.Add(s.expireDuration)),
				NotBefore: jwt.NewNumericDate(now.Add(-1000 * time.Second)),
				ID:        user.ID.String(),
				Issuer:    s.issuer,
			},
		},
	)

	tokens, err := token.SignedString(s.signKey)
	return tokens, err
}

func (s *JWTService) ParseToken(tokenString string) (string, error) {
	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(token *jwt.Token) (u interface{}, err error) {
		return s.signKey, nil
	})
	if err != nil {
		return "", err
	}

	claims, ok := token.Claims.(*CustomClaims)
	if !ok || !token.Valid {
		return "", fmt.Errorf("invaild token")
	}

	return claims.UserId, nil
}
