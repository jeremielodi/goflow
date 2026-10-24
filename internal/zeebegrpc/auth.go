// internal/zeebegrpc/auth.go
package zeebegrpc

import (
	"context"
	"strings"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/pkg/authentication"
	"github.com/jeremielodi/goflow/pkg/middleware"
	"github.com/jmoiron/sqlx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type contextKey string

const userIDContextKey contextKey = "zeebegrpc-user-id"

// UserIDFromContext returns the caller identity set by the auth
// interceptors below.
func UserIDFromContext(ctx context.Context) uuid.UUID {
	if v, ok := ctx.Value(userIDContextKey).(uuid.UUID); ok {
		return v
	}
	return uuid.Nil
}

// ResolveAnonymousUserID finds the super_admin user that calls without
// credentials act as when the gateway allows them (GRPC_ALLOW_ANONYMOUS).
// CreateDefaultData (internal/service/user_service.go) guarantees at least
// one super_admin exists at every startup.
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

// Authenticator checks the `authorization` metadata of every gRPC call with
// the same credentials as the REST API: a GoFlow JWT or an OIDC token
// (`Bearer …`), or a GoFlow e-mail and password (`Basic …`, what
// zeebe-node's basicAuth and the Desktop Modeler send). A call whose
// credentials don't check out is refused, whatever the configuration. A
// call with no credentials at all is refused too, unless anonymous is set:
// it then runs as that user (the gateway's old, unauthenticated mode).
type Authenticator struct {
	db        *sqlx.DB
	jwt       *authentication.JWTService
	oidc      *authentication.OIDCService // nil when OIDC isn't configured
	anonymous uuid.UUID                   // uuid.Nil: no anonymous calls
}

func NewAuthenticator(db *sqlx.DB, jwt *authentication.JWTService, oidc *authentication.OIDCService, anonymous uuid.UUID) *Authenticator {
	return &Authenticator{db: db, jwt: jwt, oidc: oidc, anonymous: anonymous}
}

func (a *Authenticator) authenticate(ctx context.Context) (context.Context, error) {
	var header string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get("authorization"); len(values) > 0 {
			header = strings.TrimSpace(values[0])
		}
	}
	if header == "" {
		if a.anonymous != uuid.Nil {
			return context.WithValue(ctx, userIDContextKey, a.anonymous), nil
		}
		return nil, status.Error(codes.Unauthenticated,
			"credentials required: send authorization: Bearer <GoFlow or OIDC token> or Basic <e-mail:password>")
	}
	userID, ok := a.identify(header)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "invalid or expired credentials")
	}
	return context.WithValue(ctx, userIDContextKey, userID), nil
}

// identify returns the user named by an authorization header value.
func (a *Authenticator) identify(header string) (uuid.UUID, bool) {
	if strings.HasPrefix(header, "Basic ") {
		if user := middleware.UserFromBasicAuth(a.db, header); user != nil {
			return user.ID, true
		}
		return uuid.Nil, false
	}
	token, isBearer := strings.CutPrefix(header, "Bearer ")
	if !isBearer || token == "" {
		return uuid.Nil, false
	}
	if claims, err := a.jwt.ParseAccessTokenClaims(token); err == nil {
		if userID, err := uuid.Parse(claims.UserId); err == nil {
			return userID, true
		}
	}
	if a.oidc != nil {
		if claims, err := a.oidc.ValidateToken(token); err == nil {
			if user, err := middleware.ResolveOIDCUser(a.db, claims); err == nil && user != nil {
				return user.ID, true
			}
		}
	}
	return uuid.Nil, false
}

// UnaryInterceptor and StreamInterceptor authenticate every call (see
// Authenticator) and put the caller's user id on its context.
func (a *Authenticator) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx, err := a.authenticate(ctx)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func (a *Authenticator) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := a.authenticate(ss.Context())
		if err != nil {
			return err
		}
		return handler(srv, &authenticatedStream{ServerStream: ss, ctx: ctx})
	}
}

type authenticatedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authenticatedStream) Context() context.Context { return s.ctx }
