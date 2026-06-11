// internal/api/user_controller.go
package api

import (
	"database/sql"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/pkg/authentication"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
	"golang.org/x/crypto/bcrypt"
)

type UserController struct {
	db         *sqlx.DB
	userRepo   *repository.UserRepository
	jwtService *authentication.JWTService
}

func NewUserController(db *sqlx.DB, jwtService *authentication.JWTService) *UserController {
	return &UserController{
		db:         db,
		userRepo:   repository.NewUserRepository(db),
		jwtService: jwtService,
	}
}

// ============================================================
// HELPERS
// ============================================================

func hashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	return string(bytes), err
}

func checkPasswordHash(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

// getCurrentUserID extracts user ID from context (set by your auth middleware)
func getCurrentUserID(c *fiber.Ctx) (uuid.UUID, error) {
	userUUID := common.GetUserUuid(c)
	if userUUID == "" {
		return uuid.Nil, fiber.NewError(fiber.StatusUnauthorized, "User not authenticated")
	}
	return uuid.Parse(userUUID)
}

// ============================================================
// AUTHENTICATION
// ============================================================

// Login handles POST /auth/login
func (uc *UserController) Login(c *fiber.Ctx) error {
	var req models.LoginRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid request body",
			"message": err.Error(),
		})
	}

	// Validate required fields
	if req.Email == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Validation error",
			"message": "Email is required",
		})
	}
	if req.Password == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Validation error",
			"message": "Password is required",
		})
	}

	// Find user by email
	user, err := uc.userRepo.FindByEmail(req.Email)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error":   "Invalid credentials",
				"message": "Email or password is incorrect",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Database error",
			"message": err.Error(),
		})
	}

	// Check if user is active
	if !user.IsActive {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error":   "Account inactive",
			"message": "Your account has been deactivated. Please contact an administrator.",
		})
	}

	// Get password hash for verification
	userPassword, err := uc.userRepo.GetPasswordHash(user.ID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Authentication error",
			"message": err.Error(),
		})
	}

	// Verify password
	if !checkPasswordHash(req.Password, userPassword.PasswordHash) {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error":   "Invalid credentials",
			"message": "Email or password is incorrect",
		})
	}

	// Generate token pair using JWT service
	tokenPair, err := uc.jwtService.CreateTokenPair(user)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to generate tokens",
			"message": err.Error(),
		})
	}

	// Get user roles and permissions
	roleRepo := repository.NewRoleRepository(uc.db)
	actionRepo := repository.NewActionRepository(uc.db)

	roles, _ := roleRepo.GetUserRoles(user.ID)
	actions, _ := actionRepo.GetUserPermissions(user.ID)

	roleLabels := make([]string, len(roles))
	for i, role := range roles {
		roleLabels[i] = role.Label
	}

	actionCodes := make([]string, len(actions))
	for i, action := range actions {
		actionCodes[i] = action.Code
	}

	// Set refresh token in HTTP-only cookie (optional)
	c.Cookie(&fiber.Cookie{
		Name:     "refresh_token",
		Value:    tokenPair.RefreshToken,
		Expires:  time.Now().Add(7 * 24 * time.Hour),
		HTTPOnly: true,
		Secure:   false, // Set to true in production with HTTPS
		SameSite: "Lax",
		Path:     "/auth/refresh",
	})

	return c.JSON(models.LoginResponse{
		AccessToken:  tokenPair.AccessToken,
		RefreshToken: tokenPair.RefreshToken,
		ExpiresIn:    tokenPair.ExpiresIn,
		User:         user,
		Roles:        roleLabels,
		Actions:      actionCodes,
	})
}

// internal/api/user_controller.go - Update the RefreshToken method

// RefreshToken handles POST /auth/refresh
func (uc *UserController) RefreshToken(c *fiber.Ctx) error {
	var req models.RefreshTokenRequest

	// Try to get refresh token from body first, then from cookie
	if err := c.BodyParser(&req); err != nil || req.RefreshToken == "" {
		// If not in body, try to get from cookie
		req.RefreshToken = c.Cookies("refresh_token")
	}

	if req.RefreshToken == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Refresh token required",
			"message": "Refresh token is required in request body or cookie",
		})
	}

	// Use JWT service to refresh token (includes user validation)
	tokenPair, err := uc.jwtService.RefreshAccessToken(req.RefreshToken)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error":   "Invalid refresh token",
			"message": err.Error(),
		})
	}

	// Update refresh token cookie
	c.Cookie(&fiber.Cookie{
		Name:     "refresh_token",
		Value:    tokenPair.RefreshToken,
		Expires:  time.Now().Add(7 * 24 * time.Hour),
		HTTPOnly: true,
		Secure:   false, // Set to true in production with HTTPS
		SameSite: "Lax",
		Path:     "/auth/refresh",
	})

	return c.JSON(models.RefreshTokenResponse{
		AccessToken:  tokenPair.AccessToken,
		RefreshToken: tokenPair.RefreshToken,
		ExpiresIn:    tokenPair.ExpiresIn,
	})
}

// Logout handles POST /auth/logout
func (uc *UserController) Logout(c *fiber.Ctx) error {
	// Clear the refresh token cookie
	c.Cookie(&fiber.Cookie{
		Name:     "refresh_token",
		Value:    "",
		Expires:  time.Now().Add(-1 * time.Hour),
		HTTPOnly: true,
		Path:     "/auth/refresh",
	})

	return c.JSON(fiber.Map{
		"message": "Logged out successfully",
	})
}

// ============================================================
// USER CRUD OPERATIONS
// ============================================================

// CreateUser handles POST /users
func (uc *UserController) CreateUser(c *fiber.Ctx) error {
	var req struct {
		Email    string      `json:"email"`
		FullName string      `json:"full_name"`
		Password string      `json:"password"`
		RoleIDs  []uuid.UUID `json:"role_ids,omitempty"`
	}

	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid request body",
			"message": err.Error(),
		})
	}

	// Validate required fields
	if req.Email == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Validation error",
			"message": "Email is required",
		})
	}
	if req.Password == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Validation error",
			"message": "Password is required",
		})
	}
	if len(req.Password) < 6 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Validation error",
			"message": "Password must be at least 6 characters",
		})
	}

	// Check if email already exists
	exists, err := uc.userRepo.ExistsByEmail(req.Email)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Database error",
			"message": err.Error(),
		})
	}
	if exists {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"error":   "Email already exists",
			"message": "A user with this email already exists",
		})
	}

	// Hash password
	passwordHash, err := hashPassword(req.Password)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to hash password",
			"message": err.Error(),
		})
	}

	// Start transaction
	tx, err := uc.db.Beginx()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Database error",
			"message": "Failed to start transaction",
		})
	}
	defer tx.Rollback()

	// Create user
	user := models.UserCreateModel{
		ID:           uuid.New(),
		Email:        req.Email,
		FullName:     req.FullName,
		PasswordHash: passwordHash,
		IsActive:     true,
	}

	_, err = uc.userRepo.CreateWithTx(tx, user)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to create user",
			"message": err.Error(),
		})
	}

	// Assign roles if provided
	roleRepo := repository.NewRoleRepository(uc.db)
	if len(req.RoleIDs) > 0 {
		for _, roleID := range req.RoleIDs {
			if err := roleRepo.AssignRoleToUserWithTx(tx, user.ID, roleID); err != nil {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error":   "Failed to assign roles",
					"message": err.Error(),
				})
			}
		}
	} else {
		// Assign default role if no roles specified
		if err := roleRepo.AssignDefaultRoleToUserWithTx(tx, user.ID); err != nil {
			// Log warning but don't fail user creation
			// return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			// 	"error":   "Failed to assign default role",
			// 	"message": err.Error(),
			// })
		}
	}

	if err := tx.Commit(); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Database error",
			"message": "Failed to commit transaction",
		})
	}

	// Get the created user
	createdUser, err := uc.userRepo.FindByID(user.ID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "User created but failed to fetch",
			"message": err.Error(),
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"message": "User created successfully",
		"user":    createdUser,
	})
}

// GetUser handles GET /users/:id
func (uc *UserController) GetUser(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid user ID",
			"message": err.Error(),
		})
	}

	user, err := uc.userRepo.FindByID(id)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error":   "User not found",
				"message": "No user exists with the provided ID",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Database error",
			"message": err.Error(),
		})
	}

	// Get user roles
	roleRepo := repository.NewRoleRepository(uc.db)
	roles, _ := roleRepo.GetUserRoles(user.ID)

	return c.JSON(fiber.Map{
		"user":  user,
		"roles": roles,
	})
}

// GetCurrentUser handles GET /users/me - uses your auth middleware
func (uc *UserController) GetCurrentUser(c *fiber.Ctx) error {
	userID, err := getCurrentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error":   "Unauthorized",
			"message": err.Error(),
		})
	}

	user, err := uc.userRepo.FindByID(userID)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error":   "User not found",
				"message": "User associated with token no longer exists",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Database error",
			"message": err.Error(),
		})
	}

	// Get user roles and permissions
	roleRepo := repository.NewRoleRepository(uc.db)
	actionRepo := repository.NewActionRepository(uc.db)

	roles, _ := roleRepo.GetUserRoles(user.ID)
	actions, _ := actionRepo.GetUserPermissions(user.ID)

	return c.JSON(fiber.Map{
		"user":    user,
		"roles":   roles,
		"actions": actions,
	})
}

// GetUserByEmail handles GET /users/email/:email
func (uc *UserController) GetUserByEmail(c *fiber.Ctx) error {
	email := c.Params("email")
	if email == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid email",
			"message": "Email parameter is required",
		})
	}

	user, err := uc.userRepo.FindByEmail(email)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error":   "User not found",
				"message": "No user exists with the provided email",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Database error",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"user": user,
	})
}

// ListUsers handles GET /users (admin only)
func (uc *UserController) ListUsers(c *fiber.Ctx) error {
	users, err := uc.userRepo.List()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to fetch users",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"users": users,
		"count": len(users),
	})
}

// UpdateUser handles PUT /users/:id
func (uc *UserController) UpdateUser(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid user ID",
			"message": err.Error(),
		})
	}

	// Check if current user is updating their own profile or has permission
	currentUserID, err := getCurrentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error":   "Unauthorized",
			"message": err.Error(),
		})
	}

	// Check permission
	actionRepo := repository.NewActionRepository(uc.db)
	hasPermission, _ := actionRepo.UserHasPermission(currentUserID, "CAN_MANGE_USER")

	// Only allow if user is updating themselves OR has admin permission
	if currentUserID != id && !hasPermission {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error":   "Forbidden",
			"message": "You can only update your own profile",
		})
	}

	var req struct {
		Email    string `json:"email"`
		FullName string `json:"full_name"`
		IsActive bool   `json:"is_active"`
	}

	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid request body",
			"message": err.Error(),
		})
	}

	// Check if user exists
	existingUser, err := uc.userRepo.FindByID(id)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error":   "User not found",
				"message": "No user exists with the provided ID",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Database error",
			"message": err.Error(),
		})
	}

	// If email is changing, check it's not taken
	if existingUser.Email != req.Email {
		exists, err := uc.userRepo.ExistsByEmail(req.Email)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error":   "Database error",
				"message": err.Error(),
			})
		}
		if exists {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"error":   "Email already exists",
				"message": "Another user already has this email",
			})
		}
	}

	updateUser := models.UserUpdateModel{
		Email:    req.Email,
		FullName: req.FullName,
		IsActive: req.IsActive,
	}

	_, err = uc.userRepo.Update(id, updateUser)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to update user",
			"message": err.Error(),
		})
	}

	// Get updated user
	updatedUser, err := uc.userRepo.FindByID(id)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "User updated but failed to fetch",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"message": "User updated successfully",
		"user":    updatedUser,
	})
}

// UpdatePassword handles PUT /users/:id/password
func (uc *UserController) UpdatePassword(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid user ID",
			"message": err.Error(),
		})
	}

	// Check if current user is updating their own password
	currentUserID, err := getCurrentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error":   "Unauthorized",
			"message": err.Error(),
		})
	}

	if currentUserID != id {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error":   "Forbidden",
			"message": "You can only update your own password",
		})
	}

	var req struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}

	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid request body",
			"message": err.Error(),
		})
	}

	if req.NewPassword == "" || len(req.NewPassword) < 6 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Validation error",
			"message": "New password must be at least 6 characters",
		})
	}

	// Get current password hash
	userPassword, err := uc.userRepo.GetPasswordHash(id)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error":   "User not found",
				"message": "No user exists with the provided ID",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Database error",
			"message": err.Error(),
		})
	}

	// Verify current password
	if !checkPasswordHash(req.CurrentPassword, userPassword.PasswordHash) {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error":   "Invalid password",
			"message": "Current password is incorrect",
		})
	}

	// Hash new password
	newPasswordHash, err := hashPassword(req.NewPassword)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to hash password",
			"message": err.Error(),
		})
	}

	// Update password
	_, err = uc.userRepo.UpdatePassword(id, newPasswordHash)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to update password",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"message": "Password updated successfully",
	})
}

// DeleteUser handles DELETE /users/:id
func (uc *UserController) DeleteUser(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid user ID",
			"message": err.Error(),
		})
	}

	// Check if user has permission
	currentUserID := common.GetUserUuid(c)
	currentUserUuid, err := uuid.Parse(currentUserID)

	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid current user ID",
			"message": err.Error(),
		})
	}

	actionRepo := repository.NewActionRepository(uc.db)

	hasPermission, _ := actionRepo.UserHasPermission(currentUserUuid, "CAN_MANGE_USER")

	if !hasPermission {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error":   "Forbidden",
			"message": "You don't have permission to delete users",
		})
	}

	// Check if user exists
	_, err = uc.userRepo.FindByID(id)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error":   "User not found",
				"message": "No user exists with the provided ID",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Database error",
			"message": err.Error(),
		})
	}

	_, err = uc.userRepo.Delete(id)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to delete user",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"message": "User deleted successfully",
	})
}

// ============================================================
// ACCESS CONTROL
// ============================================================

// CheckAccess handles GET /users/:id/access/:resource
func (uc *UserController) CheckAccess(c *fiber.Ctx) error {
	userID := c.Params("id")
	resourceID := c.Params("resource")

	hasAccess, err := uc.userRepo.HasAccess(userID, resourceID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Access check failed",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"has_access": hasAccess,
		"user_id":    userID,
		"resource":   resourceID,
	})
}

// CheckPermission handles GET /users/:id/permission/:actionId
func (uc *UserController) CheckPermission(c *fiber.Ctx) error {
	userID := c.Params("id")
	actionIDStr := c.Params("actionId")

	// Parse action ID as integer
	actionID, err := strconv.Atoi(actionIDStr)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid action ID",
			"message": "Action ID must be a number",
		})
	}

	isAllowed, err := uc.userRepo.IsAllow(&actionID, &userID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Permission check failed",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"is_allowed": isAllowed,
		"user_id":    userID,
		"action_id":  actionID,
	})
}
