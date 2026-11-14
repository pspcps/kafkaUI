package k8s

import (
	"encoding/json"
	"github.com/prakash/kafbat-ui/internal/connections"
	"strings"
	"testing"
)

func TestWorkerUsesSecretReferencesAndScopedIdentity(t *testing.T) {
	client := &Client{namespace: "test", image: "worker:test"}
	for _, provider := range []string{"vault", "aws_secrets_manager", "kubernetes_secret", "rds_iam"} {
		p := &connections.Profile{ID: "analytics", Name: "Analytics", DestinationType: "postgresql", Provider: provider, ServiceAccount: "analytics-worker", SecretName: "db-password", SecretProviderClass: "analytics-vault", CASecretName: "db-ca", Host: "db", Port: 5432, Database: "analytics", Username: "worker", Region: "us-east-1"}
		deployment, err := client.workerDeployment(DeploymentSpec{Name: "test", JobID: "id", Replicas: 1, MgmtDBDSN: "must-not-appear", Connection: p})
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(deployment)
		if strings.Contains(string(raw), "must-not-appear") {
			t.Fatal("management DSN embedded in manifest")
		}
		pod := deployment.Spec.Template.Spec
		if pod.ServiceAccountName != "analytics-worker" {
			t.Fatal("wrong workload identity")
		}
		env := pod.Containers[0].Env
		for _, value := range env {
			if value.Name == "MGMT_DB_DSN" && (value.ValueFrom == nil || value.ValueFrom.SecretKeyRef.Name != "kafbat-worker-db") {
				t.Fatal("missing management secret reference")
			}
		}
		if provider != "rds_iam" && len(pod.Containers[0].VolumeMounts) != 2 {
			t.Fatal("missing readonly credential/CA mounts")
		}
	}
}
