package db

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const (
	RoleAdmin  = "admin"
	RoleViewer = "viewer"
)

// User is the authenticated principal.
type User struct {
	ID           string    `json:"id"`
	Username     string    `json:"username"`
	PasswordHash string    `json:"-"`
	Role         string    `json:"role"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

type UserRepo struct{ db *sql.DB }

func NewUserRepo(db *sql.DB) *UserRepo { return &UserRepo{db: db} }

// EnsureSchema creates the users table if it doesn't exist.
// Runs idempotently on every startup so the auth flow works even on databases
// where migration files weren't applied.
func (r *UserRepo) EnsureSchema() error {
	const ddl = `
	CREATE TABLE IF NOT EXISTS users (
		id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
		username      TEXT        NOT NULL UNIQUE,
		password_hash TEXT        NOT NULL,
		role          TEXT        NOT NULL DEFAULT 'viewer',
		created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
	`
	if _, err := r.db.Exec(ddl); err != nil {
		return fmt.Errorf("ensure users schema: %w", err)
	}
	return nil
}

func (r *UserRepo) Count() (int, error) {
	var n int
	if err := r.db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func (r *UserRepo) GetByUsername(username string) (*User, error) {
	const q = `SELECT id, username, password_hash, role, created_at, updated_at FROM users WHERE username = $1`
	u := &User{}
	err := r.db.QueryRow(q, username).Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Role, &u.CreatedAt, &u.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return u, nil
}

func (r *UserRepo) GetByID(id string) (*User, error) {
	const q = `SELECT id, username, password_hash, role, created_at, updated_at FROM users WHERE id = $1`
	u := &User{}
	err := r.db.QueryRow(q, id).Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Role, &u.CreatedAt, &u.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return u, nil
}

func (r *UserRepo) Create(username, passwordHash, role string) (*User, error) {
	const q = `
		INSERT INTO users (username, password_hash, role)
		VALUES ($1, $2, $3)
		RETURNING id, username, password_hash, role, created_at, updated_at
	`
	u := &User{}
	err := r.db.QueryRow(q, username, passwordHash, role).Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Role, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("create user: %w", err)
	}
	return u, nil
}

// Upsert creates the user if missing, otherwise updates the password & role.
// Used by the bootstrap-admin flow.
func (r *UserRepo) Upsert(username, passwordHash, role string) (*User, error) {
	const q = `
		INSERT INTO users (username, password_hash, role)
		VALUES ($1, $2, $3)
		ON CONFLICT (username) DO UPDATE
		SET password_hash = EXCLUDED.password_hash,
		    role          = EXCLUDED.role,
		    updated_at    = NOW()
		RETURNING id, username, password_hash, role, created_at, updated_at
	`
	u := &User{}
	err := r.db.QueryRow(q, username, passwordHash, role).Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Role, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("upsert user: %w", err)
	}
	return u, nil
}
