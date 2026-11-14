package middleware

import (
	"github.com/gin-gonic/gin"
	"github.com/prakash/kafbat-ui/internal/auth"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAccessPolicy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		role, method string
		status       int
	}{
		{"admin", "DELETE", 204}, {"admin", "POST", 204},
		{"viewer", "GET", 204}, {"viewer", "HEAD", 204},
		{"viewer", "POST", 403}, {"viewer", "PUT", 403},
		{"viewer", "PATCH", 403}, {"viewer", "DELETE", 403},
		{"unknown", "GET", 403},
	} {
		t.Run(tc.role+tc.method, func(t *testing.T) {
			r := gin.New()
			r.Use(func(c *gin.Context) { c.Set("claims", &auth.Claims{Role: tc.role}) })
			r.Use(AuthorizeAccess())
			r.Any("/resource", func(c *gin.Context) { c.Status(204) })
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(tc.method, "/resource", nil))
			if w.Code != tc.status {
				t.Fatalf("got %d want %d", w.Code, tc.status)
			}
		})
	}
}

func TestWorkerCredential(t *testing.T) {
	token := strings.Repeat("x", 32)
	for _, tc := range []struct {
		configured, header string
		status             int
	}{
		{"", "", 503}, {"short", "Bearer short", 503},
		{token, "", 401}, {token, "Bearer wrong", 401},
		{token, "Bearer " + token, 204},
	} {
		r := gin.New()
		r.POST("/callback", WorkerAuth(tc.configured), func(c *gin.Context) { c.Status(http.StatusNoContent) })
		req := httptest.NewRequest("POST", "/callback", nil)
		req.Header.Set("Authorization", tc.header)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != tc.status {
			t.Fatalf("got %d want %d", w.Code, tc.status)
		}
	}
}
