// pkg/middleware/oidc_user.go
package middleware

import (
	"crypto/rand"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/pkg/authentication"
	"github.com/jmoiron/sqlx"
	"golang.org/x/crypto/bcrypt"
)

// ResolveOIDCUser maps a validated OIDC token to a local user, provisioning
// one on first login. Resolution order:
//  1. An existing user already linked to this OIDC subject.
//  2. An existing local user with a matching email — linked to the subject
//     for next time (lets an existing password-based account "upgrade" to
//     OIDC without creating a duplicate).
//  3. A brand new user, auto-provisioned with an unusable local password
//     (only the OIDC token can ever authenticate as them) and the default
//     role.
func ResolveOIDCUser(db *sqlx.DB, claims *authentication.OIDCClaims) (*models.User, error) {
	userRepo := repository.NewUserRepository(db)

	if user, err := userRepo.FindByOIDCSubject(claims.Subject); err == nil {
		return &user, nil
	}

	if claims.Email != "" {
		if user, err := userRepo.FindByEmail(claims.Email); err == nil {
			_ = userRepo.SetOIDCSubject(user.ID, claims.Subject)
			return &user, nil
		}
	}

	email := claims.Email
	if email == "" {
		email = "oidc-" + claims.Subject + "@no-email.invalid"
	}

	unusablePasswordHash, err := randomPasswordHash()
	if err != nil {
		return nil, err
	}

	newUser := models.UserCreateModel{
		ID:           uuid.New(),
		Email:        email,
		FullName:     email,
		PasswordHash: unusablePasswordHash,
		IsActive:     true,
		OIDCSubject:  &claims.Subject,
	}
	if _, err := userRepo.Create(newUser); err != nil {
		return nil, err
	}

	roleRepo := repository.NewRoleRepository(db)
	_ = roleRepo.AssignDefaultRoleToUser(newUser.ID)

	created, err := userRepo.FindByID(newUser.ID)
	if err != nil {
		return nil, err
	}
	return &created, nil
}

// randomPasswordHash produces a bcrypt hash of random bytes that is never
// handed back to anyone — it exists only so OIDC-provisioned users have
// *some* password_hash (the column is NOT NULL) while remaining unusable
// for local password login.
func randomPasswordHash() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	hash, err := bcrypt.GenerateFromPassword(raw, bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}
