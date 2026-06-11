// internal/middleware/auth_middleware.go
package middleware

import (
	"encoding/base64"
	"net/http"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/pkg/authentication"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
	"golang.org/x/crypto/bcrypt"
)

var PublicRoutes = []string{
	"/frontend",
	"/_next",
	"/app",
	"/auth/login",
	"/auth/refresh",
	"/login",
	"/themes",
	"/layout",
	"/static/",
	"/users/auth",
	"/assets/",
	"/modules/",
	"/bin",
	"/users/checkAuth",
	"/enterprise/init",
	"/users/log/in",
	"/clients",
	"/drivers",
	"/ws",
	"/health",
}

func WithinPublicRoutes(value string, urls []string) bool {
	if value == "/" {
		return true
	}
	var found bool = false
	for _, item := range urls {
		startWith := strings.Contains(value, item)
		if startWith {
			found = true
			break
		}
	}
	return found
}

// BasicAuthMiddleware handles HTTP Basic Authentication
func BasicAuthMiddleware(db *sqlx.DB) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get the Authorization header
		auth := c.Get("Authorization")
		if auth == "" {
			return c.Status(http.StatusUnauthorized).JSON(fiber.Map{
				"error":   "Unauthorized",
				"message": "Missing Authorization header",
			})
		}

		// Check if it's Basic Auth
		if !strings.HasPrefix(auth, "Basic ") {
			// Not Basic Auth, continue to next middleware
			return c.Next()
		}

		// Decode Basic Auth credentials
		payload, err := base64.StdEncoding.DecodeString(auth[6:])
		if err != nil {
			return c.Status(http.StatusUnauthorized).JSON(fiber.Map{
				"error":   "Unauthorized",
				"message": "Invalid Basic Auth encoding",
			})
		}

		// Split into username and password
		parts := strings.SplitN(string(payload), ":", 2)
		if len(parts) != 2 {
			return c.Status(http.StatusUnauthorized).JSON(fiber.Map{
				"error":   "Unauthorized",
				"message": "Invalid Basic Auth format",
			})
		}

		email := parts[0]
		password := parts[1]

		// Find user by email
		userRepo := repository.NewUserRepository(db)
		user, err := userRepo.FindByEmail(email)
		if err != nil {
			return c.Status(http.StatusUnauthorized).JSON(fiber.Map{
				"error":   "Unauthorized",
				"message": "Invalid credentials",
			})
		}

		// Check if user is active
		if !user.IsActive {
			return c.Status(http.StatusForbidden).JSON(fiber.Map{
				"error":   "Forbidden",
				"message": "Account is inactive",
			})
		}

		// Get password hash
		userPassword, err := userRepo.GetPasswordHash(user.ID)
		if err != nil {
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
				"error":   "Internal Error",
				"message": "Failed to verify credentials",
			})
		}

		// Verify password
		err = bcrypt.CompareHashAndPassword([]byte(userPassword.PasswordHash), []byte(password))
		if err != nil {
			return c.Status(http.StatusUnauthorized).JSON(fiber.Map{
				"error":   "Unauthorized",
				"message": "Invalid credentials",
			})
		}

		// Set user in context for downstream handlers
		common.SetUserUuid(c, user.ID.String())
		c.Locals("user_id", user.ID)
		c.Locals("user_email", user.Email)

		return c.Next()
	}
}

// OptionalBasicAuthMiddleware allows both Basic Auth and JWT
func OptionalBasicAuthMiddleware(db *sqlx.DB, jwtService *authentication.JWTService) fiber.Handler {
	return func(c *fiber.Ctx) error {
		auth := c.Get("Authorization")

		if auth == "" {
			// No auth, continue as unauthenticated
			return c.Next()
		}

		// Try Basic Auth first
		if strings.HasPrefix(auth, "Basic ") {
			payload, err := base64.StdEncoding.DecodeString(auth[6:])
			if err == nil {
				parts := strings.SplitN(string(payload), ":", 2)
				if len(parts) == 2 {
					email := parts[0]
					password := parts[1]

					userRepo := repository.NewUserRepository(db)
					user, err := userRepo.FindByEmail(email)
					if err == nil && user.IsActive {
						userPassword, err := userRepo.GetPasswordHash(user.ID)
						if err == nil {
							err = bcrypt.CompareHashAndPassword([]byte(userPassword.PasswordHash), []byte(password))
							if err == nil {
								// Basic Auth successful
								common.SetUserUuid(c, user.ID.String())
								c.Locals("user_id", user.ID)
								c.Locals("user_email", user.Email)
								c.Locals("auth_type", "basic")
								return c.Next()
							}
						}
					}
				}
			}
		}

		// Try Bearer Token (JWT)
		if strings.HasPrefix(auth, "Bearer ") {
			token := strings.TrimPrefix(auth, "Bearer ")
			token = strings.TrimSpace(token)

			userId, err := jwtService.ParseAccessToken(token)
			if err == nil && userId != "" {
				common.SetUserUuid(c, userId)
				c.Locals("auth_type", "jwt")
				return c.Next()
			}
		}

		// No valid auth, continue as unauthenticated
		return c.Next()
	}
}

func AuthenticationMiddleware(jwtService *authentication.JWTService,
	util common.Util) fiber.Handler {
	return func(c *fiber.Ctx) error {

		token, _ := getTokenFromAuthorizationHeader(c)

		if token == "" {
			token = getTokenFromCookie(c)
		}

		if token == "Bearer undefined" {
			token = ""
		}

		// Strip "Bearer " prefix if present
		if strings.HasPrefix(token, "Bearer ") {
			token = strings.TrimPrefix(token, "Bearer ")
		}

		if len(token) == 0 {
			var requestPath = c.Path()
			var isPublicRoute = WithinPublicRoutes(requestPath, PublicRoutes)
			if !isPublicRoute {
				var MGS string = "You are not logged into the system."
				c.Status(http.StatusUnauthorized).JSON(fiber.Map{
					"message": MGS,
				})
				return nil
			}
			c.Next()
			return nil
		}

		userId, err := jwtService.ParseAccessToken(token)
		if err != nil {
			MGS := "failed to parse token: " + err.Error()
			c.Status(http.StatusUnauthorized).JSON(fiber.Map{
				"title":   "PERMISSION_DENIED",
				"message": MGS,
			})
			return nil
		}

		if userId != "" {
			// set user in session
			common.SetUserUuid(c, userId)
		}

		c.Next()
		return nil
	}
}

// CombinedAuthMiddleware handles both JWT and Basic Auth
func CombinedAuthMiddleware(db *sqlx.DB, jwtService *authentication.JWTService, util common.Util) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Check if route is public
		requestPath := c.Path()
		isPublicRoute := WithinPublicRoutes(requestPath, PublicRoutes)

		if isPublicRoute {
			return c.Next()
		}

		auth := c.Get("Authorization")

		// Try Basic Auth first
		if strings.HasPrefix(auth, "Basic ") {

			payload, err := base64.StdEncoding.DecodeString(auth[6:])
			if err == nil {
				parts := strings.SplitN(string(payload), ":", 2)
				if len(parts) == 2 {
					email := parts[0]
					password := parts[1]

					userRepo := repository.NewUserRepository(db)
					user, err := userRepo.FindByEmail(email)
					if err == nil && user.IsActive {
						userPassword, err := userRepo.GetPasswordHash(user.ID)
						if err == nil {
							err = bcrypt.CompareHashAndPassword([]byte(userPassword.PasswordHash), []byte(password))
							if err == nil {
								// Basic Auth successful
								common.SetUserUuid(c, user.ID.String())
								c.Locals("user_id", user.ID)
								c.Locals("user_email", user.Email)
								c.Locals("auth_type", "basic")
								return c.Next()
							}
						}
					}
				}
			}

			// Basic Auth failed
			c.Status(http.StatusUnauthorized).JSON(fiber.Map{
				"error":   "Unauthorized",
				"message": "Invalid Basic Auth credentials",
			})
			return nil
		}

		// Try Bearer Token (JWT)
		token, _ := getTokenFromAuthorizationHeader(c)
		if token == "" {
			token = getTokenFromCookie(c)
		}
		if token == "Bearer undefined" {
			token = ""
		}
		if strings.HasPrefix(token, "Bearer ") {
			token = strings.TrimPrefix(token, "Bearer ")
		}

		if len(token) > 0 {
			userId, err := jwtService.ParseAccessToken(token)
			if err == nil && userId != "" {
				common.SetUserUuid(c, userId)
				c.Locals("auth_type", "jwt")
				return c.Next()
			}
		}

		// No valid authentication
		c.Status(http.StatusUnauthorized).JSON(fiber.Map{
			"error":   "Unauthorized",
			"message": "Authentication required",
		})
		return nil
	}
}

func getTokenFromCookie(c *fiber.Ctx) string {
	return c.Cookies("token")
}

func getTokenFromAuthorizationHeader(c *fiber.Ctx) (string, error) {
	var auth = c.Get("X-Access-Token")

	if auth == "" {
		auth = c.Get("Authorization")
	}

	if auth == "" {
		auth = c.Queries()["token"]
	}
	if auth == "" {
		return "", nil
	}

	return auth, nil
}
