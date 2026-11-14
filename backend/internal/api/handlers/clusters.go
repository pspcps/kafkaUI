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
	c.JSON(http.StatusOK, clusterViews(h.registry.List()))
}

// clusterViews deliberately excludes all authentication settings.
func clusterViews(clusters []models.ClusterConfig) []clusterView {
	views := make([]clusterView, 0, len(clusters))
	for _, cluster := range clusters {
		views = append(views, clusterView{ID: cluster.ID, Name: cluster.Name, Brokers: cluster.Brokers})
	}
	return views
}

type clusterView struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	Brokers []string `json:"brokers"`
}
