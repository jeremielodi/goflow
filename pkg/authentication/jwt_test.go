package authentication

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

// A token issued on one server is checked on another: their clocks may be
// a little apart, within clockSkew.
func TestAccessTokenClockSkew(t *testing.T) {
	s := NewJWTService(nil, "test-secret-test-secret-test-secret")
	sign := func(notBefore, expires time.Time) string {
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, CustomClaims{
			UserId:    "u1",
			TokenType: "access",
			RegisteredClaims: jwt.RegisteredClaims{
				NotBefore: jwt.NewNumericDate(notBefore),
				ExpiresAt: jwt.NewNumericDate(expires),
			},
		})
		signed, err := token.SignedString(s.signKey)
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
	now := time.Now()
	cases := []struct {
		name               string
		notBefore, expires time.Time
		valid              bool
	}{
		{"current", now.Add(-time.Minute), now.Add(time.Minute), true},
		{"issued by a server a little ahead", now.Add(10 * time.Second), now.Add(15 * time.Minute), true},
		{"not valid yet", now.Add(time.Minute), now.Add(15 * time.Minute), false},
		{"just expired, clock a little behind", now.Add(-15 * time.Minute), now.Add(-10 * time.Second), true},
		{"expired", now.Add(-15 * time.Minute), now.Add(-time.Minute), false},
	}
	for _, c := range cases {
		_, err := s.ParseAccessTokenClaims(sign(c.notBefore, c.expires))
		if (err == nil) != c.valid {
			t.Errorf("%s: valid=%v, err=%v", c.name, c.valid, err)
		}
	}
	if _, err := s.ParseAccessTokenClaims(sign(now, now.Add(time.Minute)) + "x"); err == nil {
		t.Error("a tampered token must be refused")
	}
}
