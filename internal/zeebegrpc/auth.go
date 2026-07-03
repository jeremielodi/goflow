// internal/zeebegrpc/auth.go
package zeebegrpc

import (
	"context"
	"strings"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/pkg/authentication"
	"github.com/jmoiron/sqlx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type contextKey string

const userIDContextKey contextKey = "zeebegrpc-user-id"

// UserIDFromContext returns the authenticated (or anonymous-fallback)
// caller identity set by the auth interceptors below.
func UserIDFromContext(ctx context.Context) uuid.UUID {
	if v, ok := ctx.Value(userIDContextKey).(uuid.UUID); ok {
		return v
	}
	return uuid.Nil
}

// resolveAnonymousUserID finds a super_admin user to act as the identity
// for gRPC callers that send no bearer token — real self-managed Zeebe
// gateways commonly run with no auth configured at all, and GoFlow's own
// CreateDefaultData (internal/service/user_service.go) guarantees at
// least one super_admin exists at every startup, so this makes the gRPC
// gateway usable out of the box while still enforcing the same
// per-resource ACL (CanAccessProcess) for callers that DO send a valid
// token.
func ResolveAnonymousUserID(db *sqlx.DB) (uuid.UUID, error) {
	var id uuid.UUID
	err := db.Get(&id, `
		SELECT u.id FROM public.users u
		JOIN public.user_roles ur ON ur.user_id = u.id
		JOIN public.roles r ON r.id = ur.roles_id
		WHERE r.label = 'super_admin'
		ORDER BY u.created_at ASC
		LIMIT 1
	`)
	return id, err
}

func authenticate(ctx context.Context, jwtService *authentication.JWTService, anonymousUserID uuid.UUID) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get("authorization"); len(values) > 0 {
			token := strings.TrimPrefix(values[0], "Bearer ")
			if claims, err := jwtService.ParseAccessTokenClaims(token); err == nil {
				if userID, err := uuid.Parse(claims.UserId); err == nil {
					return context.WithValue(ctx, userIDContextKey, userID)
				}
			}
		}
	}
	return context.WithValue(ctx, userIDContextKey, anonymousUserID)
}

// UnaryAuthInterceptor and StreamAuthInterceptor read an `authorization:
// Bearer <token>` gRPC metadata header if present (reusing GoFlow's
// existing JWT validation — the same tokens issued by POST /auth/login)
// and put the resulting user id on the request context; otherwise they
// fall back to the resolved super_admin identity.
func UnaryAuthInterceptor(jwtService *authentication.JWTService, anonymousUserID uuid.UUID) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return handler(authenticate(ctx, jwtService, anonymousUserID), req)
	}
}

func StreamAuthInterceptor(jwtService *authentication.JWTService, anonymousUserID uuid.UUID) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &authenticatedStream{
			ServerStream: ss,
			ctx:          authenticate(ss.Context(), jwtService, anonymousUserID),
		})
	}
}

type authenticatedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authenticatedStream) Context() context.Context { return s.ctx }
