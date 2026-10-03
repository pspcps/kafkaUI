package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prakash/kafbat-ui/internal/db"
	"github.com/prakash/kafbat-ui/internal/validation"
)

type SchemaHandler struct {
	repo  *db.SchemaRepo
	cache *validation.Cache
}

func NewSchemaHandler(repo *db.SchemaRepo, cache *validation.Cache) *SchemaHandler {
	return &SchemaHandler{repo: repo, cache: cache}
}

// GET /api/v1/clusters/:clusterId/topics/:name/schema
func (h *SchemaHandler) GetSchema(c *gin.Context) {
	schema, err := h.repo.Get(c.Param("clusterId"), c.Param("name"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if schema == nil {
		c.JSON(http.StatusOK, gin.H{
			"clusterId":     c.Param("clusterId"),
			"topicName":     c.Param("name"),
			"schemaType":    "manual",
			"schemaFormat":  "none",
			"schemaVersion": 0,
			"fields":        []interface{}{},
		})
		return
	}
	c.JSON(http.StatusOK, schema)
}

// PUT /api/v1/clusters/:clusterId/topics/:name/schema
func (h *SchemaHandler) UpsertSchema(c *gin.Context) {
	var body struct {
		SchemaType   string           `json:"schemaType"`
		AvroRaw      string           `json:"avroRaw"`
		Fields       []db.SchemaField `json:"fields"`
		SchemaFormat string           `json:"schemaFormat"`
		SchemaText   string           `json:"schemaText"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if body.SchemaFormat != "" && body.SchemaFormat != db.SchemaFormatManual && body.SchemaFormat != db.SchemaFormatNone {
		if _, err := validation.Compile(body.SchemaFormat, body.SchemaText); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_schema", "message": err.Error()})
			return
		}
	}

	schema, err := h.repo.Upsert(
		c.Param("clusterId"), c.Param("name"),
		db.UpsertInput{
			SchemaType:   body.SchemaType,
			SchemaFormat: body.SchemaFormat,
			SchemaText:   body.SchemaText,
			AvroRaw:      body.AvroRaw,
			Fields:       body.Fields,
		},
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	h.cache.Forget(c.Param("clusterId"), c.Param("name"))

	c.JSON(http.StatusOK, schema)
}

// POST /api/v1/validate-schema
// Body: {"format": "json_schema|avro|none", "text": "<schema source>", "sample": "<optional message>"}
func (h *SchemaHandler) ValidateSchema(c *gin.Context) {
	var body struct {
		Format string `json:"format" binding:"required"`
		Text   string `json:"text"`
		Sample string `json:"sample,omitempty"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	v, err := validation.Compile(body.Format, body.Text)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"valid":   false,
			"stage":   "compile",
			"message": err.Error(),
		})
		return
	}

	if body.Sample != "" {
		if err := v.Validate([]byte(body.Sample)); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"valid":   false,
				"stage":   "sample",
				"message": err.Error(),
			})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"valid": true, "format": v.Format()})
}
