package handlers

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prakash/kafbat-ui/internal/db"
	"github.com/prakash/kafbat-ui/internal/k8s"
	"github.com/prakash/kafbat-ui/internal/kafka"
)

type JobHandler struct {
	jobs       *db.JobRepo
	k8s        *k8s.Client
	workerDSN  string // DB DSN reachable from inside k8s pods
	workerAPI  string // Backend API URL reachable from inside k8s pods
	workerHost string // Host/IP reachable from inside k8s pods for host-exposed services
}

func NewJobHandler(jobs *db.JobRepo, k8sClient *k8s.Client, _ string) *JobHandler {
	// K8S_WORKER_DB_DSN / K8S_WORKER_API_URL are the addresses that worker pods
	// inside k8s use — different from the Docker-internal addresses the backend uses.
	workerDSN := os.Getenv("K8S_WORKER_DB_DSN")
	if workerDSN == "" {
		workerDSN = os.Getenv("MGMT_DB_DSN") // fallback for in-cluster deployments
	}
	workerAPI := os.Getenv("K8S_WORKER_API_URL")
	if workerAPI == "" {
		workerAPI = "http://kafbat-backend:8080"
	}
	workerHost := os.Getenv("K8S_WORKER_HOST")
	if workerHost == "" {
		workerHost = "host.docker.internal"
	}
	return &JobHandler{jobs: jobs, k8s: k8sClient, workerDSN: workerDSN, workerAPI: workerAPI, workerHost: workerHost}
}

// GET /api/v1/jobs
func (h *JobHandler) List(c *gin.Context) {
	jobs, err := h.jobs.List()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, jobs)
}

// GET /api/v1/jobs/:id
func (h *JobHandler) Get(c *gin.Context) {
	job, err := h.jobs.GetByID(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if job == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}
	c.JSON(http.StatusOK, job)
}

// POST /api/v1/jobs
func (h *JobHandler) Create(c *gin.Context) {
	var body struct {
		Name              string                `json:"name" binding:"required"`
		ClusterID         string                `json:"clusterId" binding:"required"`
		TopicName         string                `json:"topicName" binding:"required"`
		ConsumerGroup     string                `json:"consumerGroup" binding:"required"`
		ServiceName       string                `json:"serviceName" binding:"required"`
		DestinationType   string                `json:"destinationType" binding:"required"`
		DestinationConfig db.DestinationConfig  `json:"destinationConfig" binding:"required"`
		StartPosition     string                `json:"startPosition"`
		IncludeHistorical bool                  `json:"includeHistorical"`
		ConsumerCountMode string                `json:"consumerCountMode"`
		ConsumerCount     int                   `json:"consumerCount"`
		TeamName          string                `json:"teamName"`
		AlertChannel      string                `json:"alertChannel"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if body.StartPosition == "" {
		body.StartPosition = "latest"
	}
	if body.ConsumerCountMode == "" {
		body.ConsumerCountMode = "fixed"
	}
	if body.ConsumerCount <= 0 {
		body.ConsumerCount = 1
	}
	// Worker pods run inside k8s — localhost/127.0.0.1 is unreachable from there.
	// Transparently rewrite so users don't have to know about host.docker.internal.
	body.DestinationConfig.Host = rewriteLocalhost(body.DestinationConfig.Host, h.workerHost)
	if body.DestinationType == "postgresql" && body.DestinationConfig.Port == 5432 {
		// Local docker-compose Postgres is published on host port 5433.
		body.DestinationConfig.Port = 5433
	}

	job, err := h.jobs.Create(&db.ReplicationJob{
		Name:              body.Name,
		ClusterID:         body.ClusterID,
		TopicName:         body.TopicName,
		ConsumerGroup:     body.ConsumerGroup,
		ServiceName:       body.ServiceName,
		DestinationType:   body.DestinationType,
		DestinationConfig: body.DestinationConfig,
		StartPosition:     body.StartPosition,
		IncludeHistorical: body.IncludeHistorical,
		ConsumerCountMode: body.ConsumerCountMode,
		ConsumerCount:     body.ConsumerCount,
		TeamName:          body.TeamName,
		AlertChannel:      body.AlertChannel,
		K8sNamespace:      "kafbat",
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Resolve brokers for this cluster from the registry.
	// Worker pods run in k8s and cannot reach Docker-internal hostnames,
	// so swap Docker-internal broker addresses with host.docker.internal equivalents.
	reg := c.MustGet("registry").(*kafka.Registry)
	brokerList, _ := reg.GetBrokers(body.ClusterID)
	for i, b := range brokerList {
		// kafka:9093 (INTERNAL) → worker-reachable host:9092 (PLAINTEXT, host-exposed)
		brokerList[i] = strings.ReplaceAll(b, "kafka:9093", h.workerHost+":9092")
	}
	brokers := strings.Join(brokerList, ",")

	deployName := fmt.Sprintf("kafbat-worker-%s", job.ID[:8])
	err = h.k8s.CreateDeployment(context.Background(), k8s.DeploymentSpec{
		Name:         deployName,
		JobID:        job.ID,
		Replicas:     int32(job.ConsumerCount),
		MgmtDBDSN:   h.workerDSN,
		MgmtAPIURL:  h.workerAPI,
		KafkaBrokers: brokers,
	})
	if err != nil {
		// Mark job failed but don't delete it — user can retry.
		h.jobs.UpdateStatus(job.ID, "failed", "") //nolint:errcheck
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "job saved but k8s deployment failed",
			"message": err.Error(),
			"jobId":   job.ID,
		})
		return
	}

	h.jobs.UpdateStatus(job.ID, "running", deployName) //nolint:errcheck
	job.Status = "running"
	job.K8sDeploymentName = deployName

	c.JSON(http.StatusCreated, job)
}

// PATCH /api/v1/jobs/:id  — body: {"action": "pause" | "resume"}
func (h *JobHandler) UpdateStatus(c *gin.Context) {
	var body struct {
		Action string `json:"action" binding:"required"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	job, err := h.jobs.GetByID(c.Param("id"))
	if err != nil || job == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}

	switch body.Action {
	case "pause":
		if err := h.k8s.ScaleDeployment(context.Background(), job.K8sDeploymentName, 0); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		h.jobs.UpdateStatus(job.ID, "paused", job.K8sDeploymentName) //nolint:errcheck
		c.JSON(http.StatusOK, gin.H{"status": "paused"})

	case "resume":
		replicas := int32(job.ConsumerCount)
		if err := h.k8s.ScaleDeployment(context.Background(), job.K8sDeploymentName, replicas); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		h.jobs.UpdateStatus(job.ID, "running", job.K8sDeploymentName) //nolint:errcheck
		c.JSON(http.StatusOK, gin.H{"status": "running"})

	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "action must be pause or resume"})
	}
}

// DELETE /api/v1/jobs/:id
func (h *JobHandler) Delete(c *gin.Context) {
	job, err := h.jobs.GetByID(c.Param("id"))
	if err != nil || job == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}
	if job.K8sDeploymentName != "" {
		h.k8s.DeleteDeployment(context.Background(), job.K8sDeploymentName) //nolint:errcheck
	}
	if err := h.jobs.Delete(job.ID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "job deleted"})
}

// rewriteLocalhost replaces localhost / 127.0.0.1 with host.docker.internal so
// worker pods running in k8s can reach services exposed on the host machine.
func rewriteLocalhost(host, replacement string) string {
	if host == "localhost" || host == "127.0.0.1" || host == "::1" {
		if replacement != "" {
			return replacement
		}
		return "host.docker.internal"
	}
	return host
}

// PUT /api/v1/jobs/:id/replicas  — change replica count at runtime
//
// For a running job the k8s deployment is scaled immediately.
// For a paused/failed job the count is stored and applied on next Resume.
func (h *JobHandler) ScaleReplicas(c *gin.Context) {
	var body struct {
		Replicas int `json:"replicas" binding:"required,min=1,max=32"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	job, err := h.jobs.GetByID(c.Param("id"))
	if err != nil || job == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}

	// Persist new count regardless of status — Resume will pick it up.
	if err := h.jobs.UpdateConsumerCount(job.ID, body.Replicas); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// For running jobs, apply to k8s immediately.
	if job.Status == "running" && job.K8sDeploymentName != "" {
		if err := h.k8s.ScaleDeployment(context.Background(), job.K8sDeploymentName, int32(body.Replicas)); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "k8s scale failed: " + err.Error()})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"replicas": body.Replicas, "applied": job.Status == "running"})
}

// PUT /api/v1/jobs/:id/destination — update destination type + config (job must be paused/failed)
func (h *JobHandler) UpdateDestination(c *gin.Context) {
	var body struct {
		DestinationType   string               `json:"destinationType" binding:"required"`
		DestinationConfig db.DestinationConfig `json:"destinationConfig" binding:"required"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	job, err := h.jobs.GetByID(c.Param("id"))
	if err != nil || job == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}
	if job.Status == "running" {
		c.JSON(http.StatusConflict, gin.H{"error": "pause the job before editing its destination"})
		return
	}

	body.DestinationConfig.Host = rewriteLocalhost(body.DestinationConfig.Host, h.workerHost)
	if body.DestinationType == "postgresql" && body.DestinationConfig.Port == 5432 {
		body.DestinationConfig.Port = 5433
	}
	if err := h.jobs.UpdateDestination(job.ID, body.DestinationType, body.DestinationConfig); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// GET /api/v1/jobs/:id/metrics
func (h *JobHandler) GetMetrics(c *gin.Context) {
	metrics, err := h.jobs.GetMetrics(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, metrics)
}

// POST /api/v1/jobs/:id/reset-offset
// Resets consumer group offsets and restarts the k8s deployment.
//
// Body variants:
//   {"position": "earliest"}              — seek all partitions to earliest
//   {"position": "latest"}               — seek all partitions to latest
//   {"position": "custom", "offsets": {"0": 42, "2": 100}}  — per-partition exact offsets
func (h *JobHandler) ResetOffset(c *gin.Context) {
	var body struct {
		Position string            `json:"position" binding:"required"`
		Offsets  map[string]int64  `json:"offsets"` // partition_id(string) → offset
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if body.Position != "earliest" && body.Position != "latest" && body.Position != "custom" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "position must be 'earliest', 'latest', or 'custom'"})
		return
	}
	if body.Position == "custom" && len(body.Offsets) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "offsets map is required for position=custom"})
		return
	}

	job, err := h.jobs.GetByID(c.Param("id"))
	if err != nil || job == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}

	reg := c.MustGet("registry").(*kafka.Registry)
	admin, ok := reg.Get(job.ClusterID)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cluster not found in registry"})
		return
	}

	// 1. Scale to 0 — consumer group must have no active members.
	if job.K8sDeploymentName != "" {
		h.k8s.ScaleDeployment(context.Background(), job.K8sDeploymentName, 0) //nolint:errcheck
		// Brief pause so Kafka broker detects the group as empty.
		time.Sleep(2 * time.Second)
	}

	// 2. Reset offsets.
	switch body.Position {
	case "earliest", "latest":
		if err := admin.ResetConsumerGroupOffsets(job.ConsumerGroup, job.TopicName, body.Position); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "offset reset failed: " + err.Error()})
			return
		}
	case "custom":
		partOffsets := make(map[int32]int64, len(body.Offsets))
		for ps, off := range body.Offsets {
			var p int32
			if _, err := fmt.Sscanf(ps, "%d", &p); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid partition id %q", ps)})
				return
			}
			partOffsets[p] = off
		}
		if err := admin.ResetConsumerGroupOffsetsCustom(job.ConsumerGroup, job.TopicName, partOffsets); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "offset reset failed: " + err.Error()})
			return
		}
	}

	// 3. Clear error log so the UI shows a clean slate.
	h.jobs.ClearErrors(job.ID) //nolint:errcheck

	// 4. Mark job as paused — the user resumes manually.
	// We never auto-restart after an offset reset: the user decides when to Resume.
	h.jobs.UpdateStatusWithError(job.ID, "paused", job.K8sDeploymentName, "") //nolint:errcheck
	c.JSON(http.StatusOK, gin.H{"status": "paused", "position": body.Position})
}

// GET /api/v1/jobs/:id/errors
func (h *JobHandler) GetErrors(c *gin.Context) {
	errs, err := h.jobs.GetErrors(c.Param("id"), 50)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, errs)
}

// POST /api/v1/jobs/:id/errors  — called by worker pods
func (h *JobHandler) ReportError(c *gin.Context) {
	var e db.JobError
	if err := c.ShouldBindJSON(&e); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	e.JobID = c.Param("id")
	if err := h.jobs.InsertError(&e); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// Mark as paused (not failed) so the user can simply Resume to retry
	// without being forced to reset offsets. last_error carries the context.
	if job, _ := h.jobs.GetByID(e.JobID); job != nil {
		h.jobs.UpdateStatusWithError(e.JobID, "paused", job.K8sDeploymentName,
			fmt.Sprintf("partition %d offset %d: %s", e.PartitionID, e.Offset, e.ErrorMsg),
		) //nolint:errcheck
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// GET /api/v1/jobs/:id/live-metrics
// Queries Kafka directly for consumer-group committed offsets + end offsets.
// Returns []kafka.LiveMetric sorted by partition id.
func (h *JobHandler) LiveMetrics(c *gin.Context) {
	job, err := h.jobs.GetByID(c.Param("id"))
	if err != nil || job == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}
	reg := c.MustGet("registry").(*kafka.Registry)
	admin, ok := reg.Get(job.ClusterID)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cluster not found in registry"})
		return
	}
	resp, err := admin.GetLiveMetrics(job.ConsumerGroup, job.TopicName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, resp)
}

// GET /api/v1/jobs/:id/logs?lines=200
// Returns the last N lines of logs from the most recently started worker pod.
func (h *JobHandler) GetLogs(c *gin.Context) {
	job, err := h.jobs.GetByID(c.Param("id"))
	if err != nil || job == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}

	lines := int64(200)
	if l := c.Query("lines"); l != "" {
		if _, err := fmt.Sscanf(l, "%d", &lines); err != nil || lines <= 0 {
			lines = 200
		}
	}

	logs, err := h.k8s.GetPodLogs(c.Request.Context(), job.ID, lines)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"logs": logs})
}

// POST /api/v1/jobs/:id/metrics  — called by worker pods
func (h *JobHandler) ReportMetric(c *gin.Context) {
	var m db.ConsumerMetric
	if err := c.ShouldBindJSON(&m); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	m.JobID = c.Param("id")
	if err := h.jobs.UpsertMetric(&m); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}
