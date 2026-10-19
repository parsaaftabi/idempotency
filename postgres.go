package idempotency

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

// Schema is the DDL for PostgresStore. Apply it with your migration tool of
// choice (golang-migrate, atlas, plain psql); the store never migrates
// itself.
const Schema = `
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key         TEXT        PRIMARY KEY,
    fingerprint TEXT        NOT NULL,
    status      INT         NOT NULL DEFAULT 0,
    headers     TEXT        NOT NULL DEFAULT '{}',
    body        BYTEA       NOT NULL DEFAULT ''::bytea,
    in_progress BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idempotency_keys_expires_at_idx
    ON idempotency_keys (expires_at);
`

// PostgresStore is a Store backed by PostgreSQL over database/sql. It fits
// when Redis is not in the stack: the claim is atomic via INSERT ..
// ON CONFLICT DO NOTHING inside a transaction, and expired rows are
// reclaimed on claim so a crashed process cannot poison a key forever.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore builds a store over db. The driver is yours
// (pgx stdlib, lib/pq); only database/sql is required.
func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

func (s *PostgresStore) Load(ctx context.Context, key string) (*Record, error) {
	var rec Record
	var headersJSON string
	err := s.db.QueryRowContext(ctx, `
		SELECT fingerprint, status, headers, body, in_progress, created_at
		FROM idempotency_keys
		WHERE key = $1 AND expires_at > now()
	`, key).Scan(&rec.Fingerprint, &rec.Status, &headersJSON, &rec.Body, &rec.InProgress, &rec.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(headersJSON), &rec.Header); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (s *PostgresStore) TryClaim(ctx context.Context, key, fingerprint string, ttl time.Duration) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	// Reclaim rows whose TTL lapsed (e.g. a process died mid-claim).
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM idempotency_keys WHERE key = $1 AND expires_at <= now()`, key); err != nil {
		return false, err
	}
	res, err := tx.ExecContext(ctx, `
		INSERT INTO idempotency_keys (key, fingerprint, in_progress, expires_at)
		VALUES ($1, $2, TRUE, now() + ($3 * INTERVAL '1 second'))
		ON CONFLICT (key) DO NOTHING
	`, key, fingerprint, int64(ttl.Seconds()))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return n == 1, nil
}

func (s *PostgresStore) Complete(ctx context.Context, key string, rec Record, ttl time.Duration) error {
	headersJSON, err := json.Marshal(rec.Header)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		UPDATE idempotency_keys
		SET fingerprint = $2, status = $3, headers = $4, body = $5,
		    in_progress = FALSE, expires_at = now() + ($6 * INTERVAL '1 second')
		WHERE key = $1
	`, key, rec.Fingerprint, rec.Status, string(headersJSON), rec.Body, int64(ttl.Seconds()))
	return err
}

func (s *PostgresStore) Abandon(ctx context.Context, key string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM idempotency_keys WHERE key = $1`, key)
	return err
}
