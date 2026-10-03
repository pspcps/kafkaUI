package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// DestinationConfig holds connection details for a replication target.
type DestinationConfig struct {
	Host       string `json:"host"`
	Port       int    `json:"port"`
	Database   string `json:"database"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	// SQL targets
	TableName  string `json:"tableName,omitempty"`
	// MongoDB
	Collection string `json:"collection,omitempty"`
	// SQL Server
	Schema     string `json:"schema,omitempty"`
}

// ReplicationJob is one user-created replication job.
type ReplicationJob struct {
	ID                 string            `json:"id"`
	Name               string            `json:"name"`
	ClusterID          string            `json:"clusterId"`
	TopicName          string            `json:"topicName"`
	ConsumerGroup      string            `json:"consumerGroup"`
	ServiceName        string            `json:"serviceName"`
	DestinationType    string            `json:"destinationType"`
	DestinationConfig  DestinationConfig `json:"destinationConfig"`
	StartPosition      string            `json:"startPosition"`
	IncludeHistorical  bool              `json:"includeHistorical"`
	ConsumerCountMode  string            `json:"consumerCountMode"`
	ConsumerCount      int               `json:"consumerCount"`
	TeamName           string            `json:"teamName"`
	AlertChannel       string            `json:"alertChannel"`
	Status             string            `json:"status"`
	LastError          string            `json:"lastError,omitempty"`
	K8sDeploymentName  string            `json:"k8sDeploymentName,omitempty"`
	K8sNamespace       string            `json:"k8sNamespace"`
	CreatedAt          time.Time         `json:"createdAt"`
	UpdatedAt          time.Time         `json:"updatedAt"`
	// Joined from consumer_metrics
	Metrics            []ConsumerMetric  `json:"metrics,omitempty"`
}

// ConsumerMetric is one partition's lag row.
type ConsumerMetric struct {
	JobID           string     `json:"jobId"`
	PartitionID     int        `json:"partitionId"`
	CurrentOffset   int64      `json:"currentOffset"`
	EndOffset       int64      `json:"endOffset"`
	Lag             int64      `json:"lag"`
	ReplicatedCount int64      `json:"replicatedCount"`
	LastMessageAt   *time.Time `json:"lastMessageAt,omitempty"`
	UpdatedAt       time.Time  `json:"updatedAt"`
}

// JobRepo handles replication job persistence.
type JobRepo struct{ db *sql.DB }

func NewJobRepo(db *sql.DB) *JobRepo { return &JobRepo{db: db} }

func (r *JobRepo) Create(j *ReplicationJob) (*ReplicationJob, error) {
	cfg, err := json.Marshal(j.DestinationConfig)
	if err != nil {
		return nil, err
	}
	err = r.db.QueryRow(`
		INSERT INTO replication_jobs
		  (name, cluster_id, topic_name, consumer_group, service_name,
		   destination_type, destination_config,
		   start_position, include_historical,
		   consumer_count_mode, consumer_count,
		   team_name, alert_channel,
		   status, k8s_namespace)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'pending',$14)
		RETURNING id, created_at, updated_at`,
		j.Name, j.ClusterID, j.TopicName, j.ConsumerGroup, j.ServiceName,
		j.DestinationType, cfg,
		j.StartPosition, j.IncludeHistorical,
		j.ConsumerCountMode, j.ConsumerCount,
		j.TeamName, j.AlertChannel,
		j.K8sNamespace,
	).Scan(&j.ID, &j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}
	j.Status = "pending"
	return j, nil
}

func (r *JobRepo) List() ([]ReplicationJob, error) {
	rows, err := r.db.Query(`
		SELECT id, name, cluster_id, topic_name, consumer_group, service_name,
		       destination_type, destination_config,
		       start_position, include_historical,
		       consumer_count_mode, consumer_count,
		       COALESCE(team_name,''), COALESCE(alert_channel,''),
		       status, COALESCE(last_error,''), COALESCE(k8s_deployment_name,''), k8s_namespace,
		       created_at, updated_at
		FROM replication_jobs ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []ReplicationJob
	for rows.Next() {
		var j ReplicationJob
		var cfgRaw []byte
		if err := rows.Scan(
			&j.ID, &j.Name, &j.ClusterID, &j.TopicName, &j.ConsumerGroup, &j.ServiceName,
			&j.DestinationType, &cfgRaw,
			&j.StartPosition, &j.IncludeHistorical,
			&j.ConsumerCountMode, &j.ConsumerCount,
			&j.TeamName, &j.AlertChannel,
			&j.Status, &j.LastError, &j.K8sDeploymentName, &j.K8sNamespace,
			&j.CreatedAt, &j.UpdatedAt,
		); err != nil {
			return nil, err
		}
		json.Unmarshal(cfgRaw, &j.DestinationConfig) //nolint:errcheck
		jobs = append(jobs, j)
	}
	if jobs == nil {
		jobs = []ReplicationJob{}
	}
	return jobs, rows.Err()
}

// ListActive returns all jobs that have an active k8s deployment (running or paused).
func (r *JobRepo) ListActive() ([]ReplicationJob, error) {
	rows, err := r.db.Query(`
		SELECT id, cluster_id, topic_name, consumer_group,
		       consumer_count, status, COALESCE(k8s_deployment_name,''), k8s_namespace
		FROM replication_jobs
		WHERE status IN ('running', 'paused') AND k8s_deployment_name IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []ReplicationJob
	for rows.Next() {
		var j ReplicationJob
		if err := rows.Scan(
			&j.ID, &j.ClusterID, &j.TopicName, &j.ConsumerGroup,
			&j.ConsumerCount, &j.Status, &j.K8sDeploymentName, &j.K8sNamespace,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func (r *JobRepo) GetByID(id string) (*ReplicationJob, error) {
	var j ReplicationJob
	var cfgRaw []byte
	err := r.db.QueryRow(`
		SELECT id, name, cluster_id, topic_name, consumer_group, service_name,
		       destination_type, destination_config,
		       start_position, include_historical,
		       consumer_count_mode, consumer_count,
		       COALESCE(team_name,''), COALESCE(alert_channel,''),
		       status, COALESCE(last_error,''), COALESCE(k8s_deployment_name,''), k8s_namespace,
		       created_at, updated_at
		FROM replication_jobs WHERE id=$1`, id).Scan(
		&j.ID, &j.Name, &j.ClusterID, &j.TopicName, &j.ConsumerGroup, &j.ServiceName,
		&j.DestinationType, &cfgRaw,
		&j.StartPosition, &j.IncludeHistorical,
		&j.ConsumerCountMode, &j.ConsumerCount,
		&j.TeamName, &j.AlertChannel,
		&j.Status, &j.LastError, &j.K8sDeploymentName, &j.K8sNamespace,
		&j.CreatedAt, &j.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	json.Unmarshal(cfgRaw, &j.DestinationConfig) //nolint:errcheck

	metrics, err := r.GetMetrics(id)
	if err != nil {
		return nil, err
	}
	j.Metrics = metrics
	return &j, nil
}

func (r *JobRepo) UpdateStatus(id, status, deploymentName string) error {
	_, err := r.db.Exec(`
		UPDATE replication_jobs
		SET status=$2, k8s_deployment_name=NULLIF($3,''), updated_at=NOW()
		WHERE id=$1`, id, status, deploymentName)
	return err
}

// UpdateConsumerCount updates the stored replica count for a job.
func (r *JobRepo) UpdateConsumerCount(id string, count int) error {
	_, err := r.db.Exec(`
		UPDATE replication_jobs SET consumer_count=$2, updated_at=NOW() WHERE id=$1`, id, count)
	return err
}

// UpdateDestination updates the destination type and config for a job.
// The job must not be currently running (caller is responsible for pausing first).
func (r *JobRepo) UpdateDestination(id string, destType string, cfg DestinationConfig) error {
	raw, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	_, err = r.db.Exec(`
		UPDATE replication_jobs
		SET destination_type=$2, destination_config=$3, updated_at=NOW()
		WHERE id=$1`, id, destType, raw)
	return err
}

// UpdateStatusWithError sets the status and records an error message (empty string clears it).
func (r *JobRepo) UpdateStatusWithError(id, status, deploymentName, lastError string) error {
	_, err := r.db.Exec(`
		UPDATE replication_jobs
		SET status=$2, k8s_deployment_name=NULLIF($3,''), last_error=NULLIF($4,''), updated_at=NOW()
		WHERE id=$1`, id, status, deploymentName, lastError)
	return err
}

func (r *JobRepo) Delete(id string) error {
	_, err := r.db.Exec(`DELETE FROM replication_jobs WHERE id=$1`, id)
	return err
}

func (r *JobRepo) GetMetrics(jobID string) ([]ConsumerMetric, error) {
	rows, err := r.db.Query(`
		SELECT job_id, partition_id, current_offset, end_offset, lag,
		       replicated_count, last_message_at, updated_at
		FROM consumer_metrics WHERE job_id=$1 ORDER BY partition_id`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metrics []ConsumerMetric
	for rows.Next() {
		var m ConsumerMetric
		if err := rows.Scan(
			&m.JobID, &m.PartitionID, &m.CurrentOffset, &m.EndOffset, &m.Lag,
			&m.ReplicatedCount, &m.LastMessageAt, &m.UpdatedAt,
		); err != nil {
			return nil, err
		}
		metrics = append(metrics, m)
	}
	if metrics == nil {
		metrics = []ConsumerMetric{}
	}
	return metrics, rows.Err()
}

// JobError is one error event reported by a worker pod.
type JobError struct {
	ID          string    `json:"id"`
	JobID       string    `json:"jobId"`
	PartitionID int       `json:"partitionId"`
	Offset      int64     `json:"offset"`
	ErrorMsg    string    `json:"errorMsg"`
	OccurredAt  time.Time `json:"occurredAt"`
}

// InsertError stores a worker error event.
func (r *JobRepo) InsertError(e *JobError) error {
	_, err := r.db.Exec(`
		INSERT INTO job_errors (job_id, partition_id, "offset", error_msg)
		VALUES ($1,$2,$3,$4)`,
		e.JobID, e.PartitionID, e.Offset, e.ErrorMsg,
	)
	return err
}

// GetErrors returns the most recent errors for a job (newest first).
func (r *JobRepo) GetErrors(jobID string, limit int) ([]JobError, error) {
	rows, err := r.db.Query(`
		SELECT id, job_id, partition_id, "offset", error_msg, occurred_at
		FROM job_errors
		WHERE job_id=$1
		ORDER BY occurred_at DESC
		LIMIT $2`, jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var errs []JobError
	for rows.Next() {
		var e JobError
		if err := rows.Scan(&e.ID, &e.JobID, &e.PartitionID, &e.Offset, &e.ErrorMsg, &e.OccurredAt); err != nil {
			return nil, err
		}
		errs = append(errs, e)
	}
	if errs == nil {
		errs = []JobError{}
	}
	return errs, rows.Err()
}

// ClearErrors deletes all error records for a job (called after offset reset).
func (r *JobRepo) ClearErrors(jobID string) error {
	_, err := r.db.Exec(`DELETE FROM job_errors WHERE job_id=$1`, jobID)
	return err
}

// UpsertMetric is called by worker pods to report progress.
func (r *JobRepo) UpsertMetric(m *ConsumerMetric) error {
	_, err := r.db.Exec(`
		INSERT INTO consumer_metrics
		  (job_id, partition_id, current_offset, end_offset, replicated_count, last_message_at)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (job_id, partition_id) DO UPDATE
		  SET current_offset   = EXCLUDED.current_offset,
		      end_offset       = EXCLUDED.end_offset,
		      replicated_count = EXCLUDED.replicated_count,
		      last_message_at  = EXCLUDED.last_message_at,
		      updated_at       = NOW()`,
		m.JobID, m.PartitionID, m.CurrentOffset, m.EndOffset,
		m.ReplicatedCount, m.LastMessageAt,
	)
	return err
}
