package authentication

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

// testIdP is a self-contained stand-in for a real OIDC provider: an RSA
// keypair plus an httptest server serving the public key as a JWKS, so
// OIDCService.ValidateToken can be exercised end-to-end without a real
// identity provider (Keycloak, Auth0, etc.) — those still need a manual
// smoke test per the roadmap, since standing one up isn't practical in an
// automated suite, but the actual token-validation logic is fully covered
// here against a real RS256 signature and a real (local) JWKS endpoint.
type testIdP struct {
	privateKey *rsa.PrivateKey
	server     *httptest.Server
	issuer     string
}

func newTestIdP(t *testing.T) *testIdP {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate RSA key: %v", err)
	}

	idp := &testIdP{privateKey: privateKey, issuer: "https://test-idp.example.com"}

	mux := http.NewServeMux()
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		n := base64.RawURLEncoding.EncodeToString(privateKey.PublicKey.N.Bytes())
		e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(privateKey.PublicKey.E)).Bytes())
		set := jwks{Keys: []jwk{{Kty: "RSA", Kid: "test-key-1", N: n, E: e}}}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(set)
	})
	idp.server = httptest.NewServer(mux)
	return idp
}

func (idp *testIdP) close() {
	idp.server.Close()
}

func (idp *testIdP) jwksURL() string {
	return idp.server.URL + "/jwks"
}

func (idp *testIdP) signToken(t *testing.T, claims OIDCClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "test-key-1"
	signed, err := token.SignedString(idp.privateKey)
	if err != nil {
		t.Fatalf("failed to sign test token: %v", err)
	}
	return signed
}

func validClaims(issuer, audience string) OIDCClaims {
	now := time.Now()
	return OIDCClaims{
		Email: "alice@example.com",
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "oidc-subject-alice",
			Issuer:    issuer,
			Audience:  jwt.ClaimStrings{audience},
			ExpiresAt: jwt.NewNumericDate(now.Add(5 * time.Minute)),
			IssuedAt:  jwt.NewNumericDate(now),
		},
	}
}

func TestOIDCService_ValidToken(t *testing.T) {
	idp := newTestIdP(t)
	defer idp.close()

	svc := NewOIDCService(idp.issuer, "goflow-api", idp.jwksURL())
	if svc == nil {
		t.Fatal("expected a non-nil OIDCService when issuer+jwksURL are set")
	}

	token := idp.signToken(t, validClaims(idp.issuer, "goflow-api"))

	claims, err := svc.ValidateToken(token)
	if err != nil {
		t.Fatalf("expected valid token to pass validation, got: %v", err)
	}
	if claims.Subject != "oidc-subject-alice" {
		t.Errorf("expected subject oidc-subject-alice, got %s", claims.Subject)
	}
	if claims.Email != "alice@example.com" {
		t.Errorf("expected email alice@example.com, got %s", claims.Email)
	}
}

func TestOIDCService_WrongIssuerRejected(t *testing.T) {
	idp := newTestIdP(t)
	defer idp.close()

	svc := NewOIDCService(idp.issuer, "goflow-api", idp.jwksURL())
	token := idp.signToken(t, validClaims("https://not-the-configured-issuer.example.com", "goflow-api"))

	if _, err := svc.ValidateToken(token); err == nil {
		t.Fatal("expected an error for a token with the wrong issuer")
	}
}

func TestOIDCService_WrongAudienceRejected(t *testing.T) {
	idp := newTestIdP(t)
	defer idp.close()

	svc := NewOIDCService(idp.issuer, "goflow-api", idp.jwksURL())
	token := idp.signToken(t, validClaims(idp.issuer, "some-other-api"))

	if _, err := svc.ValidateToken(token); err == nil {
		t.Fatal("expected an error for a token with the wrong audience")
	}
}

func TestOIDCService_ExpiredTokenRejected(t *testing.T) {
	idp := newTestIdP(t)
	defer idp.close()

	svc := NewOIDCService(idp.issuer, "goflow-api", idp.jwksURL())
	claims := validClaims(idp.issuer, "goflow-api")
	claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-1 * time.Minute))
	token := idp.signToken(t, claims)

	if _, err := svc.ValidateToken(token); err == nil {
		t.Fatal("expected an error for an expired token")
	}
}

func TestOIDCService_TamperedSignatureRejected(t *testing.T) {
	idp := newTestIdP(t)
	defer idp.close()

	svc := NewOIDCService(idp.issuer, "goflow-api", idp.jwksURL())
	token := idp.signToken(t, validClaims(idp.issuer, "goflow-api"))

	// Flip a character in the middle of the signature segment. Flipping the
	// very last base64url character is flaky: depending on byte alignment,
	// its low bits can be unused padding that doesn't change the decoded
	// signature bytes, so the "tampered" token intermittently verifies fine.
	sigStart := strings.LastIndex(token, ".") + 1
	mid := sigStart + (len(token)-sigStart)/2
	tokenBytes := []byte(token)
	if tokenBytes[mid] == 'A' {
		tokenBytes[mid] = 'B'
	} else {
		tokenBytes[mid] = 'A'
	}
	tampered := string(tokenBytes)
	if _, err := svc.ValidateToken(tampered); err == nil {
		t.Fatal("expected an error for a tampered signature")
	}
}

func TestOIDCService_UnknownKidRejected(t *testing.T) {
	idp := newTestIdP(t)
	defer idp.close()

	svc := NewOIDCService(idp.issuer, "goflow-api", idp.jwksURL())

	claims := validClaims(idp.issuer, "goflow-api")
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = "some-unknown-key-id"
	signed, err := tok.SignedString(idp.privateKey)
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}

	if _, err := svc.ValidateToken(signed); err == nil {
		t.Fatal("expected an error for a token signed with an unrecognized kid")
	}
}

func TestNewOIDCService_DisabledWhenUnconfigured(t *testing.T) {
	if svc := NewOIDCService("", "", ""); svc != nil {
		t.Fatal("expected NewOIDCService to return nil when issuer/jwksURL are empty")
	}
	if svc := NewOIDCService("https://issuer.example.com", "", ""); svc != nil {
		t.Fatal("expected NewOIDCService to return nil when jwksURL is empty")
	}
}
