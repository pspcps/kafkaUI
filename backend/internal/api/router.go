package api

import (
	"github.com/gin-gonic/gin"
	"github.com/prakash/kafbat-ui/internal/api/handlers"
	"github.com/prakash/kafbat-ui/internal/api/middleware"
	"github.com/prakash/kafbat-ui/internal/auth"
	"github.com/prakash/kafbat-ui/internal/db"
	"github.com/prakash/kafbat-ui/internal/k8s"
	"github.com/prakash/kafbat-ui/internal/kafka"
	"github.com/prakash/kafbat-ui/internal/validation"
)

func NewRouter(
	registry *kafka.Registry,
	schemaRepo *db.SchemaRepo,
	jobRepo *db.JobRepo,
	userRepo *db.UserRepo,
	jwtConf *auth.Config,
	validatorCache *validation.Cache,
	k8sClient *k8s.Client,
	mgmtDSN string,
) *gin.Engine {
	r := gin.Default()
	r.Use(middleware.CORS())

	r.Use(func(c *gin.Context) {
		c.Set("registry", registry)
		c.Next()
	})

	r.GET("/health", handlers.Health)

	v1 := r.Group("/api/v1")

	// ── Public auth endpoints ────────────────────────────────────────────────────
	authH := handlers.NewAuthHandler(userRepo, jwtConf)
	v1.POST("/auth/login", authH.Login)

	// ── Worker-callback endpoints (called by Python worker pods) ─────────────────
	// TODO(security): require WORKER_API_TOKEN here once worker.py sends it.
	// Until then these stay open so existing worker pods keep functioning.
	if k8sClient != nil && jobRepo != nil {
		jobH := handlers.NewJobHandler(jobRepo, k8sClient, mgmtDSN)
		v1.POST("/jobs/:id/metrics", jobH.ReportMetric)
		v1.POST("/jobs/:id/errors", jobH.ReportError)
	}

	// ── All routes below require a valid JWT ─────────────────────────────────────
	protected := v1.Group("")
	protected.Use(middleware.JWTAuth(jwtConf))

	protected.GET("/auth/me", authH.Me)

	clustersH := handlers.NewClustersHandler(registry)
	protected.GET("/clusters", clustersH.ListClusters)

	if schemaRepo != nil {
		schemaH := handlers.NewSchemaHandler(schemaRepo, validatorCache)
		protected.GET("/clusters/:clusterId/topics/:name/schema", schemaH.GetSchema)
		protected.PUT("/clusters/:clusterId/topics/:name/schema", schemaH.UpsertSchema)
		protected.POST("/validate-schema", schemaH.ValidateSchema)
	}

	clusterGroup := protected.Group("/clusters/:clusterId")
	clusterGroup.Use(middleware.ClusterMiddleware(registry))
	{
		clusterH := handlers.NewClusterHandler()
		clusterGroup.GET("/cluster", clusterH.GetClusterInfo)

		topicH := handlers.NewTopicHandler(schemaRepo, validatorCache)
		clusterGroup.GET("/topics", topicH.ListTopics)
		clusterGroup.POST("/topics", topicH.CreateTopic)
		clusterGroup.GET("/topics/:name", topicH.GetTopic)
		clusterGroup.DELETE("/topics/:name", topicH.DeleteTopic)
		clusterGroup.GET("/topics/:name/messages", topicH.GetMessages)
		clusterGroup.POST("/topics/:name/messages", topicH.ProduceMessage)

		cgH := handlers.NewConsumerGroupHandler()
		clusterGroup.GET("/consumer-groups", cgH.ListConsumerGroups)
		clusterGroup.GET("/consumer-groups/:id", cgH.GetConsumerGroup)
	}

	if k8sClient != nil && jobRepo != nil {
		jobH := handlers.NewJobHandler(jobRepo, k8sClient, mgmtDSN)
		jobs := protected.Group("/jobs")
		{
			jobs.GET("", jobH.List)
			jobs.POST("", jobH.Create)
			jobs.GET("/:id", jobH.Get)
			jobs.PATCH("/:id", jobH.UpdateStatus)
			jobs.PUT("/:id/replicas", jobH.ScaleReplicas)
			jobs.PUT("/:id/destination", jobH.UpdateDestination)
			jobs.DELETE("/:id", jobH.Delete)
			jobs.GET("/:id/metrics", jobH.GetMetrics)
			jobs.GET("/:id/errors", jobH.GetErrors)
			jobs.POST("/:id/reset-offset", jobH.ResetOffset)
			jobs.GET("/:id/live-metrics", jobH.LiveMetrics)
			jobs.GET("/:id/logs", jobH.GetLogs)
		}
	}

	return r
}
