package auth

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Claims is the JWT payload.
type Claims struct {
	UserID   string `json:"uid"`
	Username string `json:"sub"`
	Role     string `json:"role"`
	jwt.RegisteredClaims
}

// Config controls token issuance.
type Config struct {
	Secret []byte
	TTL    time.Duration
}

// LoadConfig reads JWT_SECRET (required) and JWT_TTL (optional) from env.
func LoadConfig() (*Config, error) {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		return nil, errors.New("JWT_SECRET is required (generate with: openssl rand -hex 32)")
	}
	if len(secret) < 32 {
		return nil, errors.New("JWT_SECRET must be at least 32 characters")
	}
	ttl := 24 * time.Hour
	if v := os.Getenv("JWT_TTL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid JWT_TTL %q: %w", v, err)
		}
		ttl = d
	}
	return &Config{Secret: []byte(secret), TTL: ttl}, nil
}

// Sign issues a JWT for the given user.
func (c *Config) Sign(userID, username, role string) (string, time.Time, error) {
	expiresAt := time.Now().Add(c.TTL)
	claims := Claims{
		UserID:   userID,
		Username: username,
		Role:     role,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Issuer:    "kafbat-ui",
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString(c.Secret)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sign jwt: %w", err)
	}
	return signed, expiresAt, nil
}

// Verify parses and validates a JWT, returning the embedded claims.
func (c *Config) Verify(tokenStr string) (*Claims, error) {
	claims := &Claims{}
	tok, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return c.Secret, nil
	})
	if err != nil {
		return nil, err
	}
	if !tok.Valid {
		return nil, errors.New("invalid token")
	}
	return claims, nil
}
