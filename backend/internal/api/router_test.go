package api

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prakash/kafbat-ui/internal/auth"
	"github.com/prakash/kafbat-ui/internal/db"
	"github.com/prakash/kafbat-ui/internal/k8s"
	"github.com/prakash/kafbat-ui/internal/kafka"
	"github.com/prakash/kafbat-ui/internal/validation"
)

func TestViewerCannotReachMutationHandlers(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &auth.Config{Secret: []byte(strings.Repeat("x", 32)), TTL: time.Hour}
	r := NewRouter(kafka.NewRegistry(), &db.SchemaRepo{}, &db.JobRepo{}, nil, cfg, validation.NewCache(), &k8s.Client{}, "")
	token, _, err := cfg.Sign("viewer-id", "viewer", "viewer")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ method, path string }{
		{"POST", "/api/v1/clusters/test/topics"},
		{"DELETE", "/api/v1/clusters/test/topics/test"},
		{"POST", "/api/v1/clusters/test/topics/test/messages"},
		{"PUT", "/api/v1/clusters/test/topics/test/schema"},
		{"POST", "/api/v1/jobs"}, {"PATCH", "/api/v1/jobs/id"},
		{"PUT", "/api/v1/jobs/id/replicas"}, {"PUT", "/api/v1/jobs/id/destination"},
		{"DELETE", "/api/v1/jobs/id"}, {"POST", "/api/v1/jobs/id/reset-offset"},
		{"GET", "/api/v1/connections"}, {"GET", "/api/v1/jobs"}, {"GET", "/api/v1/jobs/id"},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			req.Header.Set("Authorization", "Bearer "+token)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != 403 {
				t.Fatalf("got %d want 403: %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestWorkerRoutesRequireCredential(t *testing.T) {
	t.Setenv("WORKER_API_TOKEN", strings.Repeat("x", 32))
	r := NewRouter(kafka.NewRegistry(), nil, &db.JobRepo{}, nil, &auth.Config{}, validation.NewCache(), &k8s.Client{}, "")
	for _, path := range []string{"/api/v1/jobs/id/metrics", "/api/v1/jobs/id/errors"} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("POST", path, nil))
		if w.Code != 401 {
			t.Fatalf("%s got %d want 401", path, w.Code)
		}
	}
}

func TestUnavailableWorkersReturnActionableError(t *testing.T) {
	cfg := &auth.Config{Secret: []byte(strings.Repeat("x", 32)), TTL: time.Hour}
	r := NewRouter(kafka.NewRegistry(), nil, &db.JobRepo{}, nil, cfg, validation.NewCache(), nil, "")
	token, _, _ := cfg.Sign("admin-id", "admin", "admin")
	req := httptest.NewRequest("POST", "/api/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 503 || !strings.Contains(w.Body.String(), "workers_unavailable") {
		t.Fatalf("expected explicit worker capability error, got %d %s", w.Code, w.Body.String())
	}
}
