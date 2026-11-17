package middleware

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/prakash/kafbat-ui/internal/auth"
)

// JWTAuth requires a valid Bearer token. The decoded *auth.Claims is stored
// under the "claims" context key and can be retrieved with c.MustGet("claims").
func JWTAuth(jwtConf *auth.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		if header == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing_token", "message": "Authorization header required"})
			return
		}
		parts := strings.SplitN(header, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid_token", "message": "Authorization header must be: Bearer <token>"})
			return
		}
		claims, err := jwtConf.Verify(parts[1])
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid_token", "message": "invalid or expired token"})
			return
		}
		c.Set("claims", claims)
		c.Next()
	}
}

// RequireRole aborts with 403 unless the authenticated user has one of the listed roles.
// Must be chained AFTER JWTAuth.
func RequireRole(roles ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
		claims, ok := c.Get("claims")
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}
		got := claims.(*auth.Claims).Role
		for _, want := range roles {
			if got == want {
				c.Next()
				return
			}
		}
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "forbidden", "message": "insufficient role"})
	}
}

// AuthorizeAccess denies unknown roles and makes viewers read-only.
func AuthorizeAccess() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims, ok := c.Get("claims")
		principal, valid := claims.(*auth.Claims)
		if !ok || !valid || principal == nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}
		if principal.Role == "admin" || (principal.Role == "viewer" && (c.Request.Method == http.MethodGet || c.Request.Method == http.MethodHead)) {
			c.Next()
			return
		}
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "forbidden", "message": "administrator access required"})
	}
}

// WorkerAuth uses a separate credential from human login tokens.
// An unset or short credential disables callbacks instead of leaving them public.
func WorkerAuth(token string) gin.HandlerFunc {
	expected := sha256.Sum256([]byte("Bearer " + token))
	return func(c *gin.Context) {
		if len(token) < 32 {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "worker_auth_not_configured"})
			return
		}
		got := sha256.Sum256([]byte(c.GetHeader("Authorization")))
		if subtle.ConstantTimeCompare(expected[:], got[:]) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid_worker_token"})
			return
		}
		c.Next()
	}
}
