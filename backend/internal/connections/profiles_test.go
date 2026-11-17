package connections

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestOnlyApprovedMatchingConnections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "profiles.json")
	p := Profile{ID: "approved", Name: "Analytics", DestinationType: "postgresql", Provider: "rds_iam", ServiceAccount: "analytics-worker", Host: "db.example", Port: 5432, Database: "analytics", Username: "worker", Region: "us-east-1", CASecretName: "rds-ca"}
	raw, _ := json.Marshal([]Profile{p})
	os.WriteFile(path, raw, 0600)
	t.Setenv("CONNECTION_PROFILES_FILE", path)
	if _, err := Resolve("approved", "postgresql"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range [][2]string{{"unknown", "postgresql"}, {"approved", "mysql"}} {
		if _, err := Resolve(tc[0], tc[1]); err == nil {
			t.Fatal("unapproved connection accepted")
		}
	}
	p.CASecretName = ""
	if p.Validate() == nil {
		t.Fatal("accepted IAM without verified TLS")
	}
	p.Provider = "vault"
	p.CASecretName = "ca"
	if p.Validate() == nil {
		t.Fatal("accepted Vault without a CSI class")
	}
	public, _ := json.Marshal(p.View())
	var view map[string]any
	json.Unmarshal(public, &view)
	if len(view) != 4 {
		t.Fatal("profile response exposes operator details")
	}
}
