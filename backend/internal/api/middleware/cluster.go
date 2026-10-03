package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prakash/kafbat-ui/internal/kafka"
)

// ClusterMiddleware resolves :clusterId from the path, injects the
// corresponding AdminClient and broker list into the gin context.
func ClusterMiddleware(registry *kafka.Registry) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("clusterId")
		admin, ok := registry.Get(id)
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{
				"error":   "not_found",
				"message": "cluster not found: " + id,
			})
			c.Abort()
			return
		}
		brokers, _ := registry.GetBrokers(id)
		c.Set("admin", admin)
		c.Set("brokers", brokers)
		c.Set("auth", registry.GetAuth(id)) // nil for PLAINTEXT clusters
		c.Set("clusterId", id)
		c.Next()
	}
}
