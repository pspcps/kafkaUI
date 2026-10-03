package kafka

import (
	"fmt"

	"github.com/prakash/kafbat-ui/internal/models"
)

// Registry holds all configured Kafka clusters.
type Registry struct {
	entries map[string]*registryEntry
	configs []models.ClusterConfig
}

type registryEntry struct {
	config  models.ClusterConfig
	admin   *AdminClient
}

func NewRegistry() *Registry {
	return &Registry{entries: make(map[string]*registryEntry)}
}

// Register connects to a cluster and stores it in the registry.
func (r *Registry) Register(cfg models.ClusterConfig) error {
	admin, err := NewAdminClient(cfg.Brokers, cfg.Auth)
	if err != nil {
		return fmt.Errorf("cluster %q: %w", cfg.ID, err)
	}
	r.entries[cfg.ID] = &registryEntry{config: cfg, admin: admin}
	r.configs = append(r.configs, cfg)
	return nil
}

// Get returns the AdminClient for the given cluster ID.
func (r *Registry) Get(id string) (*AdminClient, bool) {
	e, ok := r.entries[id]
	if !ok {
		return nil, false
	}
	return e.admin, true
}

// GetBrokers returns the broker addresses for the given cluster ID.
func (r *Registry) GetBrokers(id string) ([]string, bool) {
	e, ok := r.entries[id]
	if !ok {
		return nil, false
	}
	return e.config.Brokers, true
}

// GetAuth returns the auth config for the given cluster ID (nil = PLAINTEXT).
func (r *Registry) GetAuth(id string) *models.AuthConfig {
	e, ok := r.entries[id]
	if !ok {
		return nil
	}
	return e.config.Auth
}

// List returns metadata for all registered clusters.
func (r *Registry) List() []models.ClusterConfig {
	return r.configs
}

// Close shuts down all admin clients.
func (r *Registry) Close() {
	for _, e := range r.entries {
		e.admin.Close()
	}
}
