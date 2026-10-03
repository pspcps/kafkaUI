package middleware

import (
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
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid_token", "message": err.Error()})
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
