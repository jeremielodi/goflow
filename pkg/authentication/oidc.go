// pkg/authentication/oidc.go
//
// Minimal OIDC (OpenID Connect) bearer-token validator, used as an
// alternative to the internal JWT service so GoFlow can sit behind an
// external identity provider (Keycloak, Auth0, Azure AD, etc.) instead of
// (or alongside) its own password-based login. Only JWKS-based RS256 token
// validation is implemented here — no authorization-code/redirect flow,
// since that belongs to the frontend/IdP, not the resource server.
//
// Deliberately implemented with only the stdlib + the jwt package already
// used by the internal JWT service, rather than pulling in a dedicated OIDC
// client library, to keep this dependency-light.
package authentication

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

// jwk is a single entry of a JSON Web Key Set, as published at an OIDC
// provider's jwks_uri.
type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
}

type jwks struct {
	Keys []jwk `json:"keys"`
}

// OIDCClaims is the subset of ID/access token claims GoFlow cares about.
type OIDCClaims struct {
	Email string `json:"email,omitempty"`
	jwt.RegisteredClaims
}

// OIDCService validates bearer tokens issued by an external OIDC provider
// against that provider's published JWKS, caching keys for a short time to
// avoid fetching the JWKS on every request.
type OIDCService struct {
	issuer   string
	audience string
	jwksURL  string
	client   *http.Client

	mu        sync.RWMutex
	keys      map[string]*rsa.PublicKey
	fetchedAt time.Time
	cacheTTL  time.Duration
}

// NewOIDCService returns nil if issuer/jwksURL aren't configured — OIDC is
// entirely optional, and callers should treat a nil *OIDCService as "OIDC
// disabled" and skip it.
func NewOIDCService(issuer, audience, jwksURL string) *OIDCService {
	if issuer == "" || jwksURL == "" {
		return nil
	}
	return &OIDCService{
		issuer:   issuer,
		audience: audience,
		jwksURL:  jwksURL,
		client:   &http.Client{Timeout: 5 * time.Second},
		cacheTTL: 10 * time.Minute,
	}
}

// refreshKeys re-fetches the JWKS if the cache is empty or stale.
func (s *OIDCService) refreshKeys() error {
	s.mu.RLock()
	stale := time.Since(s.fetchedAt) > s.cacheTTL || len(s.keys) == 0
	s.mu.RUnlock()
	if !stale {
		return nil
	}

	resp, err := s.client.Get(s.jwksURL)
	if err != nil {
		return fmt.Errorf("failed to fetch JWKS: %w", err)
	}
	defer resp.Body.Close()

	var set jwks
	if err := json.NewDecoder(resp.Body).Decode(&set); err != nil {
		return fmt.Errorf("failed to decode JWKS: %w", err)
	}

	keys := make(map[string]*rsa.PublicKey, len(set.Keys))
	for _, k := range set.Keys {
		if k.Kty != "RSA" || k.N == "" || k.E == "" {
			continue
		}
		pubKey, err := jwkToRSAPublicKey(k)
		if err != nil {
			continue
		}
		keys[k.Kid] = pubKey
	}

	s.mu.Lock()
	s.keys = keys
	s.fetchedAt = time.Now()
	s.mu.Unlock()
	return nil
}

// jwkToRSAPublicKey decodes a JWK's base64url-encoded modulus/exponent into
// an *rsa.PublicKey.
func jwkToRSAPublicKey(k jwk) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(k.N)
	if err != nil {
		return nil, fmt.Errorf("invalid modulus: %w", err)
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(k.E)
	if err != nil {
		return nil, fmt.Errorf("invalid exponent: %w", err)
	}

	n := new(big.Int).SetBytes(nBytes)
	e := new(big.Int).SetBytes(eBytes)

	return &rsa.PublicKey{N: n, E: int(e.Int64())}, nil
}

func (s *OIDCService) getKey(kid string) (*rsa.PublicKey, error) {
	if err := s.refreshKeys(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	key, ok := s.keys[kid]
	if !ok {
		return nil, fmt.Errorf("no matching JWKS key for kid %q", kid)
	}
	return key, nil
}

// ValidateToken validates an RS256 bearer token against the configured
// provider's JWKS, and checks issuer/audience if configured.
func (s *OIDCService) ValidateToken(tokenString string) (*OIDCClaims, error) {
	claims := &OIDCClaims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		kid, _ := token.Header["kid"].(string)
		if kid == "" {
			return nil, fmt.Errorf("token header is missing kid")
		}
		return s.getKey(kid)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to validate OIDC token: %w", err)
	}
	if !token.Valid {
		return nil, fmt.Errorf("OIDC token is invalid")
	}

	if s.issuer != "" && claims.Issuer != s.issuer {
		return nil, fmt.Errorf("unexpected issuer: %s", claims.Issuer)
	}
	if s.audience != "" {
		matched := false
		for _, aud := range claims.Audience {
			if aud == s.audience {
				matched = true
				break
			}
		}
		if !matched {
			return nil, fmt.Errorf("token audience does not include %q", s.audience)
		}
	}
	if claims.Subject == "" {
		return nil, fmt.Errorf("token has no subject")
	}

	return claims, nil
}

// LooksLikeJWT is a cheap structural check (three dot-separated base64url
// segments) used to decide whether a bearer token is even worth attempting
// to validate — not a security check by itself.
func LooksLikeJWT(token string) bool {
	return len(strings.Split(token, ".")) == 3
}
