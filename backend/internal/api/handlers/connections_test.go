package handlers

import (
	"encoding/json"
	"github.com/prakash/kafbat-ui/internal/db"
	"strings"
	"testing"
)

func TestLegacyJobCredentialsNeverReturned(t *testing.T) {
	job := db.ReplicationJob{DestinationConfig: db.DestinationConfig{Host: "private-host", Username: "private-user", Password: "sensitive-value", ConnectionID: "profile", TableName: "orders"}}
	safe := publicJob(job)
	raw, _ := json.Marshal(safe)
	for _, secret := range []string{"private-host", "private-user", "sensitive-value"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("legacy connection data leaked")
		}
	}
	if job.DestinationConfig.Password != "sensitive-value" {
		t.Fatal("redaction changed stored configuration")
	}
	if safe.DestinationConfig.ConnectionID != "profile" {
		t.Fatal("connection reference missing")
	}
}
func TestInlineCredentialRequestRejected(t *testing.T) {
	_, err := validateDestination(db.DestinationConfig{Password: "raw-password"}, "postgresql")
	if err == nil || strings.Contains(err.Error(), "raw-password") {
		t.Fatal("unsafe credential validation")
	}
	_, err = validateDestination(db.DestinationConfig{TableName: `orders; DROP TABLE users`}, "postgresql")
	if err == nil {
		t.Fatal("invalid identifier accepted")
	}
}
