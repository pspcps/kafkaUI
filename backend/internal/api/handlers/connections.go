package handlers

import (
	"github.com/gin-gonic/gin"
	"github.com/prakash/kafbat-ui/internal/connections"
	"github.com/prakash/kafbat-ui/internal/db"
	"net/http"
)

func ListConnections(c *gin.Context) {
	profiles, err := connections.Load()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Connection profiles are unavailable; contact your administrator"})
		return
	}
	views := make([]connections.View, 0, len(profiles))
	for _, p := range profiles {
		views = append(views, p.View())
	}
	c.JSON(http.StatusOK, views)
}

// validateDestination rejects inline credentials and arbitrary endpoint overrides.
func validateDestination(cfg db.DestinationConfig, destinationType string) (*connections.Profile, error) {
	if cfg.Password != "" || cfg.Host != "" || cfg.Username != "" || cfg.Database != "" || cfg.Port != 0 {
		return nil, &destinationError{"Inline connection credentials/endpoints are no longer accepted; select an approved connection"}
	}
	for _, name := range []string{cfg.TableName, cfg.Collection, cfg.Schema} {
		if name != "" && !connections.Identifier.MatchString(name) {
			return nil, &destinationError{"Use a simple database identifier: letters, digits and underscores, starting with a letter or underscore"}
		}
	}
	if destinationType != "postgresql" {
		return nil, &destinationError{"Replay-safe delivery currently supports PostgreSQL only; this destination is disabled until transactional deduplication is implemented"}
	}
	return connections.Resolve(cfg.ConnectionID, destinationType)
}

type destinationError struct{ message string }

func (e *destinationError) Error() string { return e.message }

// PublicJob keeps legacy database credentials out of every job response.
func publicJob(job db.ReplicationJob) db.ReplicationJob {
	job.DestinationConfig = db.DestinationConfig{ConnectionID: job.DestinationConfig.ConnectionID, TableName: job.DestinationConfig.TableName, Collection: job.DestinationConfig.Collection, Schema: job.DestinationConfig.Schema}
	return job
}
