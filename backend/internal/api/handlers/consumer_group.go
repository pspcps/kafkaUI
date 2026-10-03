package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prakash/kafbat-ui/internal/kafka"
)

type ConsumerGroupHandler struct{}

func NewConsumerGroupHandler() *ConsumerGroupHandler { return &ConsumerGroupHandler{} }

// GET /api/v1/clusters/:clusterId/consumer-groups
func (h *ConsumerGroupHandler) ListConsumerGroups(c *gin.Context) {
	admin := c.MustGet("admin").(*kafka.AdminClient)
	groups, err := admin.ListConsumerGroups()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list consumer groups", "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, groups)
}

// GET /api/v1/clusters/:clusterId/consumer-groups/:id
func (h *ConsumerGroupHandler) GetConsumerGroup(c *gin.Context) {
	admin := c.MustGet("admin").(*kafka.AdminClient)
	detail, err := admin.GetConsumerGroupDetail(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get consumer group", "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, detail)
}
