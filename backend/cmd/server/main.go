package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	"github.com/prakash/kafbat-ui/internal/api"
	"github.com/prakash/kafbat-ui/internal/auth"
	"github.com/prakash/kafbat-ui/internal/db"
	"github.com/prakash/kafbat-ui/internal/k8s"
	"github.com/prakash/kafbat-ui/internal/kafka"
	"github.com/prakash/kafbat-ui/internal/models"
	"github.com/prakash/kafbat-ui/internal/validation"
)

func registerWithRetry(registry *kafka.Registry, cfg models.ClusterConfig, attempts int, delay time.Duration) error {
	for i := 1; i <= attempts; i++ {
		log.Printf("Connecting to cluster %q brokers: %v (attempt %d/%d)", cfg.ID, cfg.Brokers, i, attempts)
		if err := registry.Register(cfg); err != nil {
			if i == attempts {
				return err
			}
			log.Printf("  failed: %v — retrying in %s…", err, delay)
			time.Sleep(delay)
			continue
		}
		log.Printf("Connected to cluster %q", cfg.ID)
		return nil
	}
	return nil
}

func loadConfigs() []models.ClusterConfig {
	// 1. JSON file (multi-cluster, advanced).
	if path := os.Getenv("CLUSTERS_CONFIG_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			log.Fatalf("Failed to read CLUSTERS_CONFIG_FILE %q: %v", path, err)
		}
		var configs []models.ClusterConfig
		if err := json.Unmarshal(data, &configs); err != nil {
			log.Fatalf("Failed to parse %q: %v", path, err)
		}
		log.Printf("Loaded %d cluster(s) from %s", len(configs), path)
		return configs
	}
	// 2. Inline JSON in env (multi-cluster).
	if raw := os.Getenv("KAFKA_CLUSTERS"); raw != "" {
		var configs []models.ClusterConfig
		if err := json.Unmarshal([]byte(raw), &configs); err != nil {
			log.Fatalf("Failed to parse KAFKA_CLUSTERS: %v", err)
		}
		return configs
	}
	// 3. Single-cluster env vars — the simple path for MSK / Azure / Confluent / self-managed.
	if bootstrap := firstNonEmpty(os.Getenv("KAFKA_BOOTSTRAP"), os.Getenv("KAFKA_BOOTSTRAP_SERVERS")); bootstrap != "" {
		return []models.ClusterConfig{buildClusterFromEnv(bootstrap)}
	}
	// 4. Backwards-compat fallback.
	brokerList := os.Getenv("KAFKA_BROKERS")
	if brokerList == "" {
		brokerList = "localhost:9092"
	}
	return []models.ClusterConfig{{
		ID:      "default",
		Name:    "Default",
		Brokers: strings.Split(brokerList, ","),
	}}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// buildClusterFromEnv constructs a single-cluster config from KAFKA_* env vars.
// Supported auth types: none | iam | sasl_scram | sasl_plain (see models.AuthConfig).
func buildClusterFromEnv(bootstrap string) models.ClusterConfig {
	name := os.Getenv("KAFKA_CLUSTER_NAME")
	if name == "" {
		name = "Default"
	}
	id := os.Getenv("KAFKA_CLUSTER_ID")
	if id == "" {
		id = strings.ToLower(strings.ReplaceAll(name, " ", "-"))
	}
	cfg := models.ClusterConfig{
		ID:      id,
		Name:    name,
		Brokers: strings.Split(bootstrap, ","),
	}

	authType := strings.ToLower(strings.TrimSpace(os.Getenv("KAFKA_AUTH_TYPE")))
	tls := strings.EqualFold(os.Getenv("KAFKA_TLS"), "true")
	if authType == "" || authType == "none" {
		if tls {
			cfg.Auth = &models.AuthConfig{Type: "none", TLS: true}
		}
		return cfg
	}

	cfg.Auth = &models.AuthConfig{
		Type:            authType,
		TLS:             tls,
		Username:        os.Getenv("KAFKA_USERNAME"),
		Password:        os.Getenv("KAFKA_PASSWORD"),
		Region:          os.Getenv("KAFKA_AWS_REGION"),
		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		SessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
	}
	return cfg
}

// syncJobStatuses runs in the background, polling k8s every 30s to detect
// crashed pods and updating the DB accordingly.
func syncJobStatuses(k8sClient *k8s.Client, jobRepo *db.JobRepo) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		jobs, err := jobRepo.ListActive()
		if err != nil {
			log.Printf("syncJobStatuses: list error: %v", err)
			continue
		}
		for _, j := range jobs {
			if j.K8sDeploymentName == "" {
				continue
			}
			status, reason, err := k8sClient.GetDeploymentHealth(context.Background(), j.K8sDeploymentName, j.ID)
			if err != nil {
				log.Printf("syncJobStatuses: health check for %s: %v", j.ID, err)
				continue
			}
			// Only write when something changed.
			if status == j.Status && reason == "" {
				continue
			}
			if status != j.Status || reason != "" {
				log.Printf("syncJobStatuses: job %s %s→%s reason=%q", j.ID, j.Status, status, reason)
				jobRepo.UpdateStatusWithError(j.ID, status, j.K8sDeploymentName, reason) //nolint:errcheck
			}
		}
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// ── Kafka clusters ───────────────────────────────────────────────────────────
	configs := loadConfigs()
	registry := kafka.NewRegistry()
	for _, cfg := range configs {
		attempts := 5
		if cfg.Auth == nil {
			attempts = 10
		}
		if err := registerWithRetry(registry, cfg, attempts, 2*time.Second); err != nil {
			log.Printf("Warning: could not connect to cluster %q: %v", cfg.ID, err)
		}
	}
	defer registry.Close()

	// ── Management DB ────────────────────────────────────────────────────────────
	var schemaRepo *db.SchemaRepo
	var jobRepo *db.JobRepo
	var userRepo *db.UserRepo
	mgmtDSN := os.Getenv("MGMT_DB_DSN")
	if mgmtDSN != "" {
		database, err := db.Open(mgmtDSN)
		if err != nil {
			log.Fatalf("management DB required for auth, but unavailable: %v", err)
		}
		schemaRepo = db.NewSchemaRepo(database)
		jobRepo = db.NewJobRepo(database)
		userRepo = db.NewUserRepo(database)
		if err := userRepo.EnsureSchema(); err != nil {
			log.Fatalf("failed to ensure users schema: %v", err)
		}
		if err := schemaRepo.EnsureSchema(); err != nil {
			log.Fatalf("failed to ensure topic_schemas columns: %v", err)
		}
		log.Printf("Connected to management DB")
	} else {
		log.Fatalf("MGMT_DB_DSN is required (auth needs the users table)")
	}

	// ── JWT config ───────────────────────────────────────────────────────────────
	jwtConf, err := auth.LoadConfig()
	if err != nil {
		log.Fatalf("auth config: %v", err)
	}

	// ── Bootstrap admin user (first run, or when env credentials change) ─────────
	if u := os.Getenv("KAFBAT_ADMIN_USER"); u != "" {
		p := os.Getenv("KAFBAT_ADMIN_PASSWORD")
		if p == "" {
			log.Fatalf("KAFBAT_ADMIN_USER set but KAFBAT_ADMIN_PASSWORD missing")
		}
		hash, err := auth.HashPassword(p)
		if err != nil {
			log.Fatalf("hash admin password: %v", err)
		}
		if _, err := userRepo.Upsert(u, hash, db.RoleAdmin); err != nil {
			log.Fatalf("bootstrap admin: %v", err)
		}
		log.Printf("Bootstrapped admin user %q", u)
	} else {
		n, err := userRepo.Count()
		if err != nil {
			log.Fatalf("count users: %v", err)
		}
		if n == 0 {
			log.Fatalf("no users exist — set KAFBAT_ADMIN_USER and KAFBAT_ADMIN_PASSWORD on first run")
		}
	}

	// ── Kubernetes ───────────────────────────────────────────────────────────────
	var k8sClient *k8s.Client
	namespace := os.Getenv("K8S_NAMESPACE")
	if namespace == "" {
		namespace = "kafbat"
	}
	workerImage := os.Getenv("WORKER_IMAGE")
	if workerImage == "" {
		workerImage = "kafbat-worker:latest"
	}
	k8sClientTmp, err := k8s.NewClient(namespace, workerImage)
	if err != nil {
		log.Printf("Warning: k8s unavailable: %v — job deployment disabled", err)
	} else {
		k8sClient = k8sClientTmp
		log.Printf("Connected to Kubernetes (namespace: %s)", namespace)
	}

	// ── k8s status sync ──────────────────────────────────────────────────────────
	if k8sClient != nil && jobRepo != nil {
		go syncJobStatuses(k8sClient, jobRepo)
	}

	// ── HTTP server ──────────────────────────────────────────────────────────────
	validatorCache := validation.NewCache()
	router := api.NewRouter(registry, schemaRepo, jobRepo, userRepo, jwtConf, validatorCache, k8sClient, mgmtDSN)
	log.Printf("Starting server on :%s", port)
	if err := router.Run(":" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
