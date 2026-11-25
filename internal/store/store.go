package store

import (
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrNotFound is returned when a requested record is not found in the database.
	ErrNotFound = errors.New("monitor not found")
	// ErrClaimLost is returned when persisting a check fails because the monitor's
	// claim token is missing, mismatched, or expired.
	ErrClaimLost = errors.New("monitor claim lost or expired")
)

// Store provides data access operations backed by PostgreSQL using pgxpool.
type Store struct {
	pool *pgxpool.Pool
}

// New creates a new Store instance with the provided pgxpool.Pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Pool returns the underlying pgx connection pool.
func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}
