package handlers

import (
	"encoding/json"
	"github.com/prakash/kafbat-ui/internal/models"
	"testing"
)

func TestClusterResponseExcludesCredentials(t *testing.T) {
	cfg := models.ClusterConfig{ID: "prod", Name: "Production", Brokers: []string{"broker:9093"}, Auth: &models.AuthConfig{
		Type: "iam", Username: "user", Password: "password", AccessKeyID: "access", SecretAccessKey: "secret", SessionToken: "session",
	}}
	raw, err := json.Marshal(clusterViews([]models.ClusterConfig{cfg}))
	if err != nil {
		t.Fatal(err)
	}
	var response []map[string]any
	if err := json.Unmarshal(raw, &response); err != nil {
		t.Fatal(err)
	}
	if len(response) != 1 || len(response[0]) != 3 || response[0]["id"] != "prod" || response[0]["name"] != "Production" {
		t.Fatalf("unexpected public response: %s", raw)
	}
	for _, key := range []string{"auth", "password", "accessKeyId", "secretAccessKey", "sessionToken", "username"} {
		if _, exists := response[0][key]; exists {
			t.Fatalf("credential field %s leaked", key)
		}
	}
	if cfg.Auth.Password != "password" {
		t.Fatal("redaction modified server configuration")
	}
	empty, _ := json.Marshal(clusterViews(nil))
	if string(empty) != "[]" {
		t.Fatalf("expected empty array: %s", empty)
	}
}
