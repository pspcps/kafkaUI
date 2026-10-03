package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prakash/kafbat-ui/internal/kafka"
)

type ClusterHandler struct{}

func NewClusterHandler() *ClusterHandler { return &ClusterHandler{} }

// GET /api/v1/clusters/:clusterId/cluster
func (h *ClusterHandler) GetClusterInfo(c *gin.Context) {
	admin := c.MustGet("admin").(*kafka.AdminClient)
	info, err := admin.GetClusterInfo()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get cluster info", "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, info)
}
