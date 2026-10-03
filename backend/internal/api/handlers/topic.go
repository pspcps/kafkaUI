package handlers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/prakash/kafbat-ui/internal/db"
	"github.com/prakash/kafbat-ui/internal/kafka"
	"github.com/prakash/kafbat-ui/internal/models"
	"github.com/prakash/kafbat-ui/internal/validation"
)

func authFromContext(c *gin.Context) *models.AuthConfig {
	if v, exists := c.Get("auth"); exists && v != nil {
		if a, ok := v.(*models.AuthConfig); ok {
			return a
		}
	}
	return nil
}

type TopicHandler struct {
	schemaRepo *db.SchemaRepo    // may be nil if mgmt DB unavailable
	cache      *validation.Cache // always non-nil
}

func NewTopicHandler(schemaRepo *db.SchemaRepo, cache *validation.Cache) *TopicHandler {
	return &TopicHandler{schemaRepo: schemaRepo, cache: cache}
}

// GET /api/v1/clusters/:clusterId/topics
func (h *TopicHandler) ListTopics(c *gin.Context) {
	admin := c.MustGet("admin").(*kafka.AdminClient)
	includeInternal := c.Query("includeInternal") == "true"
	topics, err := admin.ListTopics(includeInternal)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list topics", "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, topics)
}

// GET /api/v1/clusters/:clusterId/topics/:name
func (h *TopicHandler) GetTopic(c *gin.Context) {
	admin := c.MustGet("admin").(*kafka.AdminClient)
	detail, err := admin.GetTopicDetail(c.Param("name"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Topic not found", "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, detail)
}

// createTopicBody allows the topic-create payload to optionally include a schema.
type createTopicBody struct {
	models.CreateTopicRequest
	Schema *struct {
		Format string `json:"format"`
		Text   string `json:"text"`
	} `json:"schema,omitempty"`
}

// POST /api/v1/clusters/:clusterId/topics
func (h *TopicHandler) CreateTopic(c *gin.Context) {
	admin := c.MustGet("admin").(*kafka.AdminClient)
	var body createTopicBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "message": err.Error()})
		return
	}

	// Compile schema first if supplied — fail early before creating the topic,
	// so a bad schema doesn't leave a topic without one in Kafka.
	if body.Schema != nil && body.Schema.Format != "" && body.Schema.Format != "none" {
		if _, err := validation.Compile(body.Schema.Format, body.Schema.Text); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid schema", "message": err.Error()})
			return
		}
	}

	if err := admin.CreateTopic(&body.CreateTopicRequest); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create topic", "message": err.Error()})
		return
	}

	if body.Schema != nil && h.schemaRepo != nil {
		_, err := h.schemaRepo.Upsert(c.Param("clusterId"), body.Name, db.UpsertInput{
			SchemaFormat: body.Schema.Format,
			SchemaText:   body.Schema.Text,
		})
		if err != nil {
			// Topic created in Kafka but schema persist failed — don't roll back
			// (idempotent retry can re-PUT the schema). Return 201 + warning.
			c.JSON(http.StatusCreated, gin.H{
				"message": "Topic created, but schema save failed",
				"name":    body.Name,
				"warning": err.Error(),
			})
			return
		}
	}

	c.JSON(http.StatusCreated, gin.H{"message": "Topic created successfully", "name": body.Name})
}

// DELETE /api/v1/clusters/:clusterId/topics/:name
func (h *TopicHandler) DeleteTopic(c *gin.Context) {
	admin := c.MustGet("admin").(*kafka.AdminClient)
	if err := admin.DeleteTopic(c.Param("name")); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete topic", "message": err.Error()})
		return
	}
	if h.schemaRepo != nil {
		h.cache.Forget(c.Param("clusterId"), c.Param("name"))
	}
	c.JSON(http.StatusOK, gin.H{"message": "Topic deleted successfully"})
}

// GET /api/v1/clusters/:clusterId/topics/:name/messages
func (h *TopicHandler) GetMessages(c *gin.Context) {
	brokers := c.MustGet("brokers").([]string)
	consumer := kafka.NewMessageConsumer(brokers, authFromContext(c))

	opts := kafka.FetchOptions{Partition: -1, Offset: -2, Limit: 50}
	if p := c.Query("partition"); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			opts.Partition = int32(v)
		}
	}
	if o := c.Query("offset"); o != "" {
		if v, err := strconv.ParseInt(o, 10, 64); err == nil {
			opts.Offset = v
		}
	}
	if l := c.Query("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil {
			opts.Limit = v
		}
	}
	opts.FilterKey = c.Query("filterKey")
	opts.FilterValue = c.Query("filterValue")

	result, err := consumer.FetchMessages(c.Param("name"), opts)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch messages", "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, result)
}

// POST /api/v1/clusters/:clusterId/topics/:name/messages
func (h *TopicHandler) ProduceMessage(c *gin.Context) {
	brokers := c.MustGet("brokers").([]string)
	var req models.ProduceMessageRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "message": err.Error()})
		return
	}

	clusterID := c.Param("clusterId")
	topicName := c.Param("name")

	if err := h.validatePayload(clusterID, topicName, []byte(req.Value)); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "schema_validation_failed",
			"message": err.Error(),
		})
		return
	}

	if err := kafka.ProduceMessage(brokers, topicName, &req, authFromContext(c)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to produce message", "message": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"message": "Message produced successfully"})
}

// validatePayload looks up the topic's schema (if any), compiles a validator
// (cached), and runs it against the raw message bytes.
// Returns nil when no schema is configured or the configured format is "none"/"manual".
func (h *TopicHandler) validatePayload(clusterID, topicName string, payload []byte) error {
	if h.schemaRepo == nil {
		return nil // schema features disabled (mgmt DB unavailable)
	}
	schema, err := h.schemaRepo.Get(clusterID, topicName)
	if err != nil || schema == nil {
		return nil // no schema → don't block produce
	}
	v, err := h.cache.Get(clusterID, topicName, schema.SchemaVersion, schema.SchemaFormat, schema.SchemaText)
	if err != nil {
		// A broken stored schema shouldn't block message production indefinitely.
		// Surface a clear error so the user can fix the schema.
		return err
	}
	if v == nil {
		return nil
	}
	return v.Validate(payload)
}
