package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prakash/kafbat-ui/internal/kafka"
	"github.com/prakash/kafbat-ui/internal/models"
)

type ClustersHandler struct {
	registry *kafka.Registry
}

func NewClustersHandler(registry *kafka.Registry) *ClustersHandler {
	return &ClustersHandler{registry: registry}
}

// GET /api/v1/clusters
func (h *ClustersHandler) ListClusters(c *gin.Context) {
	clusters := h.registry.List()
	if clusters == nil {
		clusters = []models.ClusterConfig{}
	}
	c.JSON(http.StatusOK, clusters)
}
