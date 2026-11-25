package db_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"pulse/internal/config"
	"pulse/internal/db"
)

func TestNewPoolConfig_Defaults(t *testing.T) {
	connStr := "postgres://pulse:pulse@localhost:5432/pulse?sslmode=disable"
	poolCfg, err := db.NewPoolConfig(connStr)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if poolCfg.MinConns != db.DefaultMinConns {
		t.Errorf("expected MinConns %d, got %d", db.DefaultMinConns, poolCfg.MinConns)
	}
	if poolCfg.MaxConns != db.DefaultMaxConns {
		t.Errorf("expected MaxConns %d, got %d", db.DefaultMaxConns, poolCfg.MaxConns)
	}
	if poolCfg.MaxConnLifetime != db.DefaultMaxConnLifetime {
		t.Errorf("expected MaxConnLifetime %v, got %v", db.DefaultMaxConnLifetime, poolCfg.MaxConnLifetime)
	}
	if poolCfg.MaxConnIdleTime != db.DefaultMaxConnIdleTime {
		t.Errorf("expected MaxConnIdleTime %v, got %v", db.DefaultMaxConnIdleTime, poolCfg.MaxConnIdleTime)
	}
	if poolCfg.HealthCheckPeriod != db.DefaultHealthCheckPeriod {
		t.Errorf("expected HealthCheckPeriod %v, got %v", db.DefaultHealthCheckPeriod, poolCfg.HealthCheckPeriod)
	}
}

func TestNewPoolConfig_CustomLimits(t *testing.T) {
	connStr := "postgres://pulse:pulse@localhost:5432/pulse?sslmode=disable"
	poolCfg, err := db.NewPoolConfig(connStr, db.WithPoolLimits(5, 25, 2*time.Hour, 45*time.Minute))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if poolCfg.MinConns != 5 {
		t.Errorf("expected MinConns 5, got %d", poolCfg.MinConns)
	}
	if poolCfg.MaxConns != 25 {
		t.Errorf("expected MaxConns 25, got %d", poolCfg.MaxConns)
	}
	if poolCfg.MaxConnLifetime != 2*time.Hour {
		t.Errorf("expected MaxConnLifetime 2h, got %v", poolCfg.MaxConnLifetime)
	}
	if poolCfg.MaxConnIdleTime != 45*time.Minute {
		t.Errorf("expected MaxConnIdleTime 45m, got %v", poolCfg.MaxConnIdleTime)
	}
}

func TestNewPoolConfig_InvalidURL(t *testing.T) {
	t.Run("empty URL returns error", func(t *testing.T) {
		_, err := db.NewPoolConfig("")
		if err == nil {
			t.Fatal("expected error with empty URL, got nil")
		}
	})

	t.Run("malformed URL returns error", func(t *testing.T) {
		_, err := db.NewPoolConfig("://not-a-valid-conn-str")
		if err == nil {
			t.Fatal("expected error with malformed URL, got nil")
		}
	})
}

func TestSchemaSQL_Structure(t *testing.T) {
	// Verify SchemaSQL contains the idempotent definitions for exactly two tables
	if !strings.Contains(db.SchemaSQL, "CREATE TABLE IF NOT EXISTS monitors") {
		t.Error("SchemaSQL missing CREATE TABLE IF NOT EXISTS monitors")
	}
	if !strings.Contains(db.SchemaSQL, "CREATE TABLE IF NOT EXISTS check_results") {
		t.Error("SchemaSQL missing CREATE TABLE IF NOT EXISTS check_results")
	}
	if !strings.Contains(db.SchemaSQL, "idx_monitors_next_check_at") {
		t.Error("SchemaSQL missing idx_monitors_next_check_at index")
	}
	if !strings.Contains(db.SchemaSQL, "idx_monitors_due_claim") {
		t.Error("SchemaSQL missing idx_monitors_due_claim index")
	}
	if !strings.Contains(db.SchemaSQL, "claim_token UUID NULL") {
		t.Error("SchemaSQL missing claim_token UUID NULL column")
	}
	if !strings.Contains(db.SchemaSQL, "claim_expires_at TIMESTAMPTZ NULL") {
		t.Error("SchemaSQL missing claim_expires_at TIMESTAMPTZ NULL column")
	}
	if !strings.Contains(db.SchemaSQL, "idx_check_results_monitor_checked_at") {
		t.Error("SchemaSQL missing idx_check_results_monitor_checked_at index")
	}
	if !strings.Contains(db.SchemaSQL, "REFERENCES monitors(id) ON DELETE CASCADE") {
		t.Error("SchemaSQL missing foreign key constraint with ON DELETE CASCADE")
	}

	// Verify no prohibited tables exist in schema
	prohibitedTables := []string{"jobs", "workers", "queues", "tasks", "leases", "incidents", "notifications", "users", "audits"}
	for _, table := range prohibitedTables {
		if strings.Contains(strings.ToLower(db.SchemaSQL), "create table if not exists "+table) {
			t.Errorf("SchemaSQL contains prohibited table: %s", table)
		}
	}
}

func TestDB_NilSafety(t *testing.T) {
	var nilDB *db.DB

	if err := nilDB.Ping(context.Background()); err == nil {
		t.Error("expected error on nilDB.Ping, got nil")
	}
	if err := nilDB.RunSchema(context.Background()); err == nil {
		t.Error("expected error on nilDB.RunSchema, got nil")
	}
	if pool := nilDB.Pool(); pool != nil {
		t.Errorf("expected nil pool from nilDB, got %v", pool)
	}
	// Calling Close on nilDB must not panic
	nilDB.Close()
}

func TestDatabase_LiveIntegration(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = config.DefaultDatabaseURL
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	database, err := db.Connect(ctx, dbURL)
	if err != nil {
		t.Skipf("PostgreSQL unavailable at %s; live DB integration test skipped (%v)", dbURL, err)
		return
	}
	defer database.Close()

	lockConn, err := database.Pool().Acquire(ctx)
	if err != nil {
		t.Fatalf("failed to acquire DB lock connection: %v", err)
	}
	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_lock(70756973)`); err != nil {
		lockConn.Release()
		t.Fatalf("failed to acquire advisory lock: %v", err)
	}
	defer func() {
		_, _ = lockConn.Exec(context.Background(), `SELECT pg_advisory_unlock(70756973)`)
		lockConn.Release()
	}()

	// Verify ping
	if err := database.Ping(ctx); err != nil {
		t.Fatalf("database ping failed: %v", err)
	}

	// Verify schema execution
	if err := database.RunSchema(ctx); err != nil {
		t.Fatalf("first RunSchema failed: %v", err)
	}

	// Verify idempotency: running schema a second time must succeed without errors
	if err := database.RunSchema(ctx); err != nil {
		t.Fatalf("second RunSchema (idempotency check) failed: %v", err)
	}

	// Verify tables exist in PostgreSQL
	var monitorsTableExists, checkResultsTableExists bool
	err = database.Pool().QueryRow(ctx,
		`SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_name = 'monitors'
		)`,
	).Scan(&monitorsTableExists)
	if err != nil {
		t.Fatalf("failed to query information_schema for monitors: %v", err)
	}
	if !monitorsTableExists {
		t.Error("expected monitors table to exist in PostgreSQL")
	}

	err = database.Pool().QueryRow(ctx,
		`SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_name = 'check_results'
		)`,
	).Scan(&checkResultsTableExists)
	if err != nil {
		t.Fatalf("failed to query information_schema for check_results: %v", err)
	}
	if !checkResultsTableExists {
		t.Error("expected check_results table to exist in PostgreSQL")
	}
}
