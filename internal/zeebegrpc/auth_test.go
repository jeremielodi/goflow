package zeebegrpc

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/pkg/authentication"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const testSecret = "test-secret-test-secret-test-secret"

func callWith(header string) context.Context {
	if header == "" {
		return context.Background()
	}
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", header))
}

func TestAuthenticate(t *testing.T) {
	jwt := authentication.NewJWTService(nil, testSecret)
	user := models.User{ID: uuid.New(), Email: "worker@goflow.com"}
	valid, err := jwt.CreateAccessToken(user)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := authentication.NewJWTService(nil, "another-secret-another-secret-xx").CreateAccessToken(user)
	if err != nil {
		t.Fatal(err)
	}
	anonymous := uuid.New()

	cases := []struct {
		name      string
		anonymous uuid.UUID
		header    string
		want      uuid.UUID // uuid.Nil: refused with Unauthenticated
	}{
		{"valid GoFlow JWT", uuid.Nil, "Bearer " + valid, user.ID},
		{"valid JWT, anonymous allowed too", anonymous, "Bearer " + valid, user.ID},
		{"no credentials", uuid.Nil, "", uuid.Nil},
		{"no credentials, anonymous allowed", anonymous, "", anonymous},
		// The gateway used to run these as super_admin.
		{"garbage token", uuid.Nil, "Bearer not-a-token", uuid.Nil},
		{"garbage token, anonymous allowed", anonymous, "Bearer not-a-token", uuid.Nil},
		{"token signed with another key", anonymous, "Bearer " + foreign, uuid.Nil},
		{"empty bearer", anonymous, "Bearer ", uuid.Nil},
		{"unknown scheme", anonymous, "Token " + valid, uuid.Nil},
		{"undecodable basic", anonymous, "Basic %%%", uuid.Nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a := NewAuthenticator(nil, jwt, nil, c.anonymous)
			ctx, err := a.authenticate(callWith(c.header))
			if c.want == uuid.Nil {
				if status.Code(err) != codes.Unauthenticated {
					t.Fatalf("want Unauthenticated, got err=%v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("want user %s, got %v", c.want, err)
			}
			if got := UserIDFromContext(ctx); got != c.want {
				t.Fatalf("user = %s, want %s", got, c.want)
			}
		})
	}
}
