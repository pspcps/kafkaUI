package k8s

import (
	"context"
	"encoding/json"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestResumeReplacesAlreadyRunningPausedWorker(t *testing.T) {
	deployment := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "test"}}
	deployment.Spec.Replicas = ptr(int32(2))
	updated := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/apis/apps/v1/namespaces/test/deployments/worker" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if r.Method == http.MethodPut {
			if err := json.NewDecoder(r.Body).Decode(&deployment); err != nil {
				t.Error(err)
			}
			updated = true
		}
		_ = json.NewEncoder(w).Encode(deployment)
	}))
	defer server.Close()
	cs, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{cs: cs, namespace: "test"}
	if err := client.ResumeDeployment(context.Background(), "worker", 2); err != nil {
		t.Fatal(err)
	}
	if !updated || *deployment.Spec.Replicas != 2 || deployment.Spec.Template.Annotations["kafkaui.io/resumed-at"] == "" {
		t.Fatal("resume did not replace in-process paused workers")
	}
}
