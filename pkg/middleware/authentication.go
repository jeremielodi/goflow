package middleware

import (
	"net/http"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/pkg/authentication"
	"github.com/jeremielodi/goflow/pkg/common"
)

var PublicRoutes = []string{
	"/frontend",
	"/_next",
	"/app",
	"/auth/login",
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
	"/socket.io",
	"/images/",
	"/dictionary",
	"/form",
	"/submissions/doc/",
	"/submissions/E36FBD63D0144736ACC8DD7409A3465E/download.xlsx",
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

func AuthenticationMiddleware(jwtService *authentication.JWTService,
	util common.Util,
	userRepo repository.UserRepository) fiber.Handler {
	return func(c *fiber.Ctx) error {

		token, _ := getTokenFromAuthorizationHeader(c)

		if token == "" {
			token = getTokenFromCookie(c)
		}

		if token == "Bearer undefined" {
			token = ""
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
		//c.Request.URL.Path

		userId, err := jwtService.ParseToken(token)
		if err != nil {
			MGS := "failed to parse token"

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

	/*token := strings.Fields(auth)
	if len(token) != 2 || strings.ToLower(token[0]) != "bearer" || token[1] == "" {
		return "", fmt.Errorf("authorization header invaild")
	}

	return auth, nil
	*/
}
