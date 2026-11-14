package auth

import (
	"github.com/golang-jwt/jwt/v5"
	"strings"
	"testing"
	"time"
)

func TestTokenValidation(t *testing.T) {
	cfg := &Config{Secret: []byte(strings.Repeat("x", 32)), TTL: time.Hour}
	token, _, err := cfg.Sign("user-id", "alice", "viewer")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.Verify(token); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, issuer string
		expiry       *jwt.NumericDate
		method       jwt.SigningMethod
	}{
		{"missing expiry", "kafbat-ui", nil, jwt.SigningMethodHS256},
		{"wrong issuer", "other", jwt.NewNumericDate(time.Now().Add(time.Hour)), jwt.SigningMethodHS256},
		{"expired", "kafbat-ui", jwt.NewNumericDate(time.Now().Add(-time.Hour)), jwt.SigningMethodHS256},
		{"wrong algorithm", "kafbat-ui", jwt.NewNumericDate(time.Now().Add(time.Hour)), jwt.SigningMethodHS512},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claims := Claims{UserID: "user-id", Username: "alice", Role: "viewer", RegisteredClaims: jwt.RegisteredClaims{Issuer: tc.issuer, ExpiresAt: tc.expiry}}
			token, err := jwt.NewWithClaims(tc.method, claims).SignedString(cfg.Secret)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := cfg.Verify(token); err == nil {
				t.Fatal("accepted invalid token")
			}
		})
	}
}

func TestRejectNonPositiveTTL(t *testing.T) {
	t.Setenv("JWT_SECRET", strings.Repeat("x", 32))
	for _, ttl := range []string{"0s", "-1h"} {
		t.Setenv("JWT_TTL", ttl)
		if _, err := LoadConfig(); err == nil {
			t.Fatalf("accepted TTL %s", ttl)
		}
	}
}
