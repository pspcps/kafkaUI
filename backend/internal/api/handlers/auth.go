package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prakash/kafbat-ui/internal/auth"
	"github.com/prakash/kafbat-ui/internal/db"
)

type AuthHandler struct {
	users   *db.UserRepo
	jwtConf *auth.Config
}

func NewAuthHandler(users *db.UserRepo, jwtConf *auth.Config) *AuthHandler {
	return &AuthHandler{users: users, jwtConf: jwtConf}
}

type loginRequest struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

type loginResponse struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expiresAt"`
	User      userView  `json:"user"`
}

type userView struct {
	ID       string `json:"id"`
	Username string `json:"username"`
	Role     string `json:"role"`
}

// POST /api/v1/auth/login
func (h *AuthHandler) Login(c *gin.Context) {
	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request", "message": err.Error()})
		return
	}

	user, err := h.users.GetByUsername(req.Username)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "lookup_failed", "message": err.Error()})
		return
	}
	if user == nil || !auth.CheckPassword(user.PasswordHash, req.Password) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid_credentials", "message": "invalid username or password"})
		return
	}

	token, expiresAt, err := h.jwtConf.Sign(user.ID, user.Username, user.Role)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "sign_failed", "message": err.Error()})
		return
	}

	c.JSON(http.StatusOK, loginResponse{
		Token:     token,
		ExpiresAt: expiresAt,
		User:      userView{ID: user.ID, Username: user.Username, Role: user.Role},
	})
}

// GET /api/v1/auth/me
func (h *AuthHandler) Me(c *gin.Context) {
	claims, ok := c.MustGet("claims").(*auth.Claims)
	if !ok || claims == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	c.JSON(http.StatusOK, userView{
		ID:       claims.UserID,
		Username: claims.Username,
		Role:     claims.Role,
	})
}
