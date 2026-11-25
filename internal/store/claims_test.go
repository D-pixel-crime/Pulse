package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"pulse/internal/config"
	"pulse/internal/db"
	"pulse/internal/model"
)

func connectSecondTestDB(t *testing.T) *db.DB {
	t.Helper()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = config.DefaultDatabaseURL
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	database, err := db.Connect(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed to open second DB pool: %v", err)
	}
	t.Cleanup(database.Close)
	return database
}

func TestClaimDueMonitors_LivePropertiesAndNoScheduleAdvance(t *testing.T) {
	database := connectTestDB(t)
	if database == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	s := New(database.Pool())

	m, err := s.Create(ctx, &model.Monitor{
		Name:            "Claim Properties Monitor",
		URL:             "https://example.com/claim-props",
		IntervalSeconds: 60,
		TimeoutSeconds:  7,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	t.Cleanup(func() {
		cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cCancel()
		_ = s.Delete(cCtx, m.ID)
	})

	var beforeNextCheckAt, dbBefore time.Time
	if err := database.Pool().QueryRow(ctx,
		`UPDATE monitors
		 SET next_check_at = clock_timestamp() - INTERVAL '10 seconds'
		 WHERE id = $1
		 RETURNING next_check_at, clock_timestamp()`,
		m.ID,
	).Scan(&beforeNextCheckAt, &dbBefore); err != nil {
		t.Fatalf("failed to backdate next_check_at: %v", err)
	}

	claimed, err := s.ClaimDueMonitors(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimDueMonitors failed: %v", err)
	}
	var dbAfter time.Time
	if err := database.Pool().QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbAfter); err != nil {
		t.Fatalf("failed to read clock_timestamp(): %v", err)
	}

	var got *model.ClaimedMonitor
	for i := range claimed {
		if claimed[i].Monitor != nil && claimed[i].Monitor.ID == m.ID {
			got = &claimed[i]
			break
		}
	}
	if got == nil {
		t.Fatalf("expected monitor %v to be claimed", m.ID)
	}
	if got.ClaimToken == uuid.Nil {
		t.Fatal("expected non-nil claim_token")
	}

	// Expiry must come from PostgreSQL statement_timestamp() + timeout_seconds (7s) + safety margin (30s).
	leaseOffset := 7*time.Second + DefaultClaimSafetyMargin
	minExpiry := dbBefore.Add(leaseOffset).Add(-time.Second)
	maxExpiry := dbAfter.Add(leaseOffset).Add(time.Second)
	if got.ClaimExpiresAt.Before(minExpiry) || got.ClaimExpiresAt.After(maxExpiry) {
		t.Fatalf("expected claim_expires_at within [%v, %v], got %v", minExpiry, maxExpiry, got.ClaimExpiresAt)
	}

	// next_check_at must NOT be advanced on claim.
	assertTimeMicroEqual(t, "next_check_at unchanged after claim", got.Monitor.NextCheckAt, beforeNextCheckAt)

	// Subsequent claim while lease is active must not return the already-claimed monitor.
	again, err := s.ClaimDueMonitors(ctx, 10)
	if err != nil {
		t.Fatalf("second ClaimDueMonitors failed: %v", err)
	}
	for _, cm := range again {
		if cm.Monitor != nil && cm.Monitor.ID == m.ID {
			t.Fatalf("monitor %v should not be claimed again while lease is active", m.ID)
		}
	}

	// Verify public Monitor JSON never includes claim_token or claim_expires_at.
	fetched, err := s.GetByID(ctx, m.ID)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}
	rawJSON, err := json.Marshal(fetched)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	if strings.Contains(string(rawJSON), "claim_token") || strings.Contains(string(rawJSON), got.ClaimToken.String()) {
		t.Fatalf("public Monitor JSON exposed claim token: %s", rawJSON)
	}
}

func TestClaimDueMonitors_ConcurrentTwoClients_LiveDB(t *testing.T) {
	databaseA := connectTestDB(t)
	if databaseA == nil {
		return
	}
	databaseB := connectSecondTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	storeA := New(databaseA.Pool())
	storeB := New(databaseB.Pool())

	createDue := func(idx int) *model.Monitor {
		t.Helper()
		m, err := storeA.Create(ctx, &model.Monitor{
			Name:            fmt.Sprintf("Concurrent Claim Monitor %d", idx),
			URL:             fmt.Sprintf("https://example.com/concurrent/%d", idx),
			IntervalSeconds: 60,
			TimeoutSeconds:  5,
		})
		if err != nil {
			t.Fatalf("Create %d failed: %v", idx, err)
		}
		t.Cleanup(func() {
			cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cCancel()
			_ = storeA.Delete(cCtx, m.ID)
		})
		if _, err := databaseA.Pool().Exec(ctx,
			`UPDATE monitors SET next_check_at = clock_timestamp() - make_interval(secs => $2::integer) WHERE id = $1`,
			m.ID, idx+10,
		); err != nil {
			t.Fatalf("failed to set next_check_at for %d: %v", idx, err)
		}
		return m
	}

	// Part 1: Batch of 6 due monitors claimed concurrently by two independent clients.
	seeded := make(map[uuid.UUID]bool, 6)
	for i := 0; i < 6; i++ {
		m := createDue(i)
		seeded[m.ID] = true
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	var resA, resB []model.ClaimedMonitor
	var errA, errB error

	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		resA, errA = storeA.ClaimDueMonitors(ctx, 3)
	}()
	go func() {
		defer wg.Done()
		<-start
		resB, errB = storeB.ClaimDueMonitors(ctx, 3)
	}()
	close(start)
	wg.Wait()

	if errA != nil {
		t.Fatalf("storeA ClaimDueMonitors failed: %v", errA)
	}
	if errB != nil {
		t.Fatalf("storeB ClaimDueMonitors failed: %v", errB)
	}

	seen := make(map[uuid.UUID]string)
	for _, cm := range resA {
		if !seeded[cm.Monitor.ID] {
			continue
		}
		if _, dup := seen[cm.Monitor.ID]; dup {
			t.Fatalf("monitor %v claimed twice in resA", cm.Monitor.ID)
		}
		seen[cm.Monitor.ID] = "A"
	}
	for _, cm := range resB {
		if !seeded[cm.Monitor.ID] {
			continue
		}
		if prev, dup := seen[cm.Monitor.ID]; dup {
			t.Fatalf("monitor %v returned to both %s and B while lease active!", cm.Monitor.ID, prev)
		}
		seen[cm.Monitor.ID] = "B"
	}
	if len(seen) != len(seeded) {
		t.Fatalf("expected all %d monitors claimed across A and B without overlap, got %d", len(seeded), len(seen))
	}

	// Clean up batch monitors before single-monitor contention rounds.
	for id := range seeded {
		_ = storeA.Delete(ctx, id)
	}

	// Part 2: Direct contention on a single due monitor across repeated rounds.
	for round := 0; round < 5; round++ {
		single := createDue(100 + round)

		roundStart := make(chan struct{})
		var roundWG sync.WaitGroup
		var cA, cB []model.ClaimedMonitor
		var eA, eB error

		roundWG.Add(2)
		go func() {
			defer roundWG.Done()
			<-roundStart
			cA, eA = storeA.ClaimDueMonitors(ctx, 1)
		}()
		go func() {
			defer roundWG.Done()
			<-roundStart
			cB, eB = storeB.ClaimDueMonitors(ctx, 1)
		}()
		close(roundStart)
		roundWG.Wait()

		if eA != nil || eB != nil {
			t.Fatalf("round %d claim error: eA=%v eB=%v", round, eA, eB)
		}

		countSingle := 0
		for _, cm := range cA {
			if cm.Monitor.ID == single.ID {
				countSingle++
			}
		}
		for _, cm := range cB {
			if cm.Monitor.ID == single.ID {
				countSingle++
			}
		}
		if countSingle != 1 {
			t.Fatalf("round %d: expected single due monitor %v claimed by exactly 1 client, got %d", round, single.ID, countSingle)
		}
		_ = storeA.Delete(ctx, single.ID)
	}
}

func TestClaimDueMonitors_LeaseExpiryRecovery_LiveDB(t *testing.T) {
	databaseA := connectTestDB(t)
	if databaseA == nil {
		return
	}
	databaseB := connectSecondTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	storeA := New(databaseA.Pool())
	storeB := New(databaseB.Pool())

	m, err := storeA.Create(ctx, &model.Monitor{
		Name:            "Lease Expiry Monitor",
		URL:             "https://example.com/expiry",
		IntervalSeconds: 60,
		TimeoutSeconds:  2,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	t.Cleanup(func() {
		cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cCancel()
		_ = storeA.Delete(cCtx, m.ID)
	})

	if _, err := databaseA.Pool().Exec(ctx,
		`UPDATE monitors SET next_check_at = clock_timestamp() - INTERVAL '5 seconds' WHERE id = $1`,
		m.ID,
	); err != nil {
		t.Fatalf("failed to backdate next_check_at: %v", err)
	}

	// Client A claims using PostgreSQL's clock -> lease expires in ~32s.
	claimedA, err := storeA.ClaimDueMonitors(ctx, 10)
	if err != nil {
		t.Fatalf("storeA ClaimDueMonitors failed: %v", err)
	}
	var tokenA uuid.UUID
	for _, cm := range claimedA {
		if cm.Monitor.ID == m.ID {
			tokenA = cm.ClaimToken
			break
		}
	}
	if tokenA == uuid.Nil {
		t.Fatalf("expected storeA to claim monitor %v", m.ID)
	}

	// Before database lease expiry: Client B cannot claim it.
	claimedBefore, err := storeB.ClaimDueMonitors(ctx, 10)
	if err != nil {
		t.Fatalf("storeB ClaimDueMonitors before expiry failed: %v", err)
	}
	for _, cm := range claimedBefore {
		if cm.Monitor.ID == m.ID {
			t.Fatalf("expected monitor %v not to be claimed before lease expiry", m.ID)
		}
	}

	// Explicitly expire the lease in PostgreSQL using clock_timestamp().
	tag, err := databaseA.Pool().Exec(ctx,
		`UPDATE monitors
		 SET claim_expires_at = clock_timestamp() - INTERVAL '1 second'
		 WHERE id = $1
		   AND claim_token = $2`,
		m.ID, tokenA,
	)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("failed to expire lease in DB: err=%v rows=%d", err, tag.RowsAffected())
	}

	// After database lease expiry: Client B recovers and claims it with a new distinct token.
	claimedAfter, err := storeB.ClaimDueMonitors(ctx, 10)
	if err != nil {
		t.Fatalf("storeB ClaimDueMonitors after expiry failed: %v", err)
	}
	var tokenB uuid.UUID
	for _, cm := range claimedAfter {
		if cm.Monitor.ID == m.ID {
			tokenB = cm.ClaimToken
			break
		}
	}
	if tokenB == uuid.Nil {
		t.Fatalf("expected storeB to claim monitor %v after lease expiry", m.ID)
	}
	if tokenB == tokenA {
		t.Fatalf("expected new claim token after lease recovery, got identical token %v", tokenB)
	}
}

func TestRecordCheckResult_FencingStaleAndExpiredClaim_LiveDB(t *testing.T) {
	databaseA := connectTestDB(t)
	if databaseA == nil {
		return
	}
	databaseB := connectSecondTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	storeA := New(databaseA.Pool())
	storeB := New(databaseB.Pool())

	m, err := storeA.Create(ctx, &model.Monitor{
		Name:            "Fencing Monitor",
		URL:             "https://example.com/fencing",
		IntervalSeconds: 60,
		TimeoutSeconds:  2,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	t.Cleanup(func() {
		cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cCancel()
		_ = storeA.Delete(cCtx, m.ID)
	})

	// 1. Unclaimed monitor rejects persistence with ErrClaimLost.
	now := time.Now().UTC().Truncate(time.Microsecond)
	status200 := 200
	if _, err := storeA.RecordCheckResult(ctx, RecordCheckParams{
		MonitorID:  m.ID,
		ClaimToken: uuid.New(),
		CheckedAt:  now,
		Success:    true,
		HTTPStatus: &status200,
		LatencyMs:  10,
	}); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("expected ErrClaimLost on unclaimed monitor, got %v", err)
	}

	// 2. Client A claims -> tokenA. Test expires tokenA in DB, then Client B claims -> tokenB.
	claimedA, err := storeA.ClaimDueMonitors(ctx, 100)
	if err != nil {
		t.Fatalf("storeA ClaimDueMonitors failed: %v", err)
	}
	var tokenA uuid.UUID
	for _, cm := range claimedA {
		if cm.Monitor.ID == m.ID {
			tokenA = cm.ClaimToken
			break
		}
	}
	if tokenA == uuid.Nil {
		t.Fatalf("expected storeA to claim monitor %v", m.ID)
	}

	if _, err := databaseA.Pool().Exec(ctx,
		`UPDATE monitors
		 SET claim_expires_at = clock_timestamp() - INTERVAL '1 second'
		 WHERE id = $1
		   AND claim_token = $2`,
		m.ID, tokenA,
	); err != nil {
		t.Fatalf("failed to expire Client A lease: %v", err)
	}

	claimedB, err := storeB.ClaimDueMonitors(ctx, 100)
	if err != nil {
		t.Fatalf("storeB ClaimDueMonitors failed: %v", err)
	}
	var tokenB uuid.UUID
	for _, cm := range claimedB {
		if cm.Monitor.ID == m.ID {
			tokenB = cm.ClaimToken
			break
		}
	}
	if tokenB == uuid.Nil || tokenB == tokenA {
		t.Fatalf("expected storeB to obtain distinct new token, got %v (tokenA=%v)", tokenB, tokenA)
	}

	// Stale Client A attempts to persist with tokenA -> must fail with ErrClaimLost and NOT overwrite Client B's claim!
	status503 := 503
	err503 := "stale failure"
	if _, err := storeA.RecordCheckResult(ctx, RecordCheckParams{
		MonitorID:    m.ID,
		ClaimToken:   tokenA,
		CheckedAt:    now,
		Success:      false,
		HTTPStatus:   &status503,
		LatencyMs:    25,
		ErrorMessage: &err503,
	}); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("expected ErrClaimLost for stale tokenA, got %v", err)
	}

	// Verify 0 check_results inserted and tokenB is still stored on the monitor.
	var checkCount int
	if err := databaseA.Pool().QueryRow(ctx, `SELECT count(*) FROM check_results WHERE monitor_id = $1`, m.ID).Scan(&checkCount); err != nil {
		t.Fatalf("count check_results failed: %v", err)
	}
	if checkCount != 0 {
		t.Fatalf("expected 0 check_results after stale write rejection, got %d", checkCount)
	}
	var storedToken uuid.UUID
	if err := databaseA.Pool().QueryRow(ctx, `SELECT claim_token FROM monitors WHERE id = $1`, m.ID).Scan(&storedToken); err != nil {
		t.Fatalf("select claim_token failed: %v", err)
	}
	if storedToken != tokenB {
		t.Fatalf("expected Client B claim_token %v preserved, got %v", tokenB, storedToken)
	}

	// Client B persists with valid tokenB -> succeeds, clears claim, advances next_check_at.
	outB, err := storeB.RecordCheckResult(ctx, RecordCheckParams{
		MonitorID:  m.ID,
		ClaimToken: tokenB,
		CheckedAt:  now,
		Success:    true,
		HTTPStatus: &status200,
		LatencyMs:  15,
	})
	if err != nil {
		t.Fatalf("storeB RecordCheckResult failed: %v", err)
	}
	if outB.Monitor.CurrentStatus != model.StatusUp {
		t.Fatalf("expected status UP, got %q", outB.Monitor.CurrentStatus)
	}

	// 3. Expired claim with no newer claimant is rejected by RecordCheckResult against DB clock_timestamp().
	if _, err := databaseA.Pool().Exec(ctx,
		`UPDATE monitors SET next_check_at = clock_timestamp() - INTERVAL '2 minutes' WHERE id = $1`,
		m.ID,
	); err != nil {
		t.Fatalf("failed to reset next_check_at: %v", err)
	}
	claimedExp, err := storeA.ClaimDueMonitors(ctx, 100)
	if err != nil {
		t.Fatalf("ClaimDueMonitors for expiry test failed: %v", err)
	}
	var expiredToken uuid.UUID
	for _, cm := range claimedExp {
		if cm.Monitor.ID == m.ID {
			expiredToken = cm.ClaimToken
			break
		}
	}
	if expiredToken == uuid.Nil {
		t.Fatalf("expected monitor %v to be claimed", m.ID)
	}

	if _, err := databaseA.Pool().Exec(ctx,
		`UPDATE monitors
		 SET claim_expires_at = clock_timestamp() - INTERVAL '1 second'
		 WHERE id = $1
		   AND claim_token = $2`,
		m.ID, expiredToken,
	); err != nil {
		t.Fatalf("failed to expire lease in DB: %v", err)
	}

	if _, err := storeA.RecordCheckResult(ctx, RecordCheckParams{
		MonitorID:  m.ID,
		ClaimToken: expiredToken,
		CheckedAt:  now,
		Success:    true,
		HTTPStatus: &status200,
		LatencyMs:  10,
	}); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("expected ErrClaimLost for expired lease against DB clock_timestamp(), got %v", err)
	}

	// 4. Lease expiring AFTER SELECT ... FOR UPDATE and check_results INSERT must cause the
	// conditional UPDATE (claim_expires_at > clock_timestamp()) to match 0 rows, return ErrClaimLost,
	// and roll back the inserted check_result.
	if err := storeA.Delete(ctx, m.ID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	m2, err := storeA.Create(ctx, &model.Monitor{
		Name:            "Mid-Tx Expiry Monitor",
		URL:             "https://example.com/mid-tx-expiry",
		IntervalSeconds: 60,
		TimeoutSeconds:  2,
	})
	if err != nil {
		t.Fatalf("Create m2 failed: %v", err)
	}
	t.Cleanup(func() {
		cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cCancel()
		_ = storeA.Delete(cCtx, m2.ID)
	})
	claimedM2, err := storeA.ClaimDueMonitors(ctx, 100)
	if err != nil {
		t.Fatalf("ClaimDueMonitors m2 failed: %v", err)
	}
	var tokenM2 uuid.UUID
	for _, cm := range claimedM2 {
		if cm.Monitor.ID == m2.ID {
			tokenM2 = cm.ClaimToken
			break
		}
	}
	if tokenM2 == uuid.Nil {
		t.Fatalf("expected m2 %v to be claimed", m2.ID)
	}

	tx, err := databaseA.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	_, err = storeA.recordCheckResultTx(ctx, tx, RecordCheckParams{
		MonitorID:  m2.ID,
		ClaimToken: tokenM2,
		CheckedAt:  now,
		Success:    true,
		HTTPStatus: &status200,
		LatencyMs:  10,
	}, func(hookCtx context.Context, hookTx checkTx) error {
		_, execErr := hookTx.Exec(hookCtx,
			`UPDATE monitors
			 SET claim_expires_at = clock_timestamp() - INTERVAL '1 second'
			 WHERE id = $1 AND claim_token = $2`,
			m2.ID, tokenM2,
		)
		return execErr
	})
	if !errors.Is(err, ErrClaimLost) {
		t.Fatalf("expected ErrClaimLost when lease expires before conditional UPDATE, got %v", err)
	}
	var m2Checks int
	if err := databaseA.Pool().QueryRow(ctx, `SELECT count(*) FROM check_results WHERE monitor_id = $1`, m2.ID).Scan(&m2Checks); err != nil {
		t.Fatalf("count m2 check_results failed: %v", err)
	}
	if m2Checks != 0 {
		t.Fatalf("expected 0 check_results after conditional UPDATE rollback, got %d", m2Checks)
	}
}

func TestReleaseClaim_LiveDB(t *testing.T) {
	database := connectTestDB(t)
	if database == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	s := New(database.Pool())

	m, err := s.Create(ctx, &model.Monitor{
		Name:            "Release Claim Monitor",
		URL:             "https://example.com/release",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	t.Cleanup(func() {
		cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cCancel()
		_ = s.Delete(cCtx, m.ID)
	})

	claimed, err := s.ClaimDueMonitors(ctx, 100)
	if err != nil {
		t.Fatalf("ClaimDueMonitors failed: %v", err)
	}
	var token uuid.UUID
	for _, cm := range claimed {
		if cm.Monitor.ID == m.ID {
			token = cm.ClaimToken
			break
		}
	}
	if token == uuid.Nil {
		t.Fatalf("expected monitor %v to be claimed", m.ID)
	}

	// Wrong token must return false and preserve active claim.
	released, err := s.ReleaseClaim(ctx, m.ID, uuid.New())
	if err != nil {
		t.Fatalf("ReleaseClaim with wrong token errored: %v", err)
	}
	if released {
		t.Fatal("expected ReleaseClaim with wrong token to return false")
	}

	// Matching token releases claim without advancing next_check_at.
	released, err = s.ReleaseClaim(ctx, m.ID, token)
	if err != nil {
		t.Fatalf("ReleaseClaim with matching token errored: %v", err)
	}
	if !released {
		t.Fatal("expected ReleaseClaim with matching token to return true")
	}

	fetched, err := s.GetByID(ctx, m.ID)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}
	assertTimeMicroEqual(t, "next_check_at unchanged on ReleaseClaim", fetched.NextCheckAt, m.NextCheckAt)

	// Monitor is immediately claimable again with a new token.
	reclaimed, err := s.ClaimDueMonitors(ctx, 100)
	if err != nil {
		t.Fatalf("re-ClaimDueMonitors failed: %v", err)
	}
	var token2 uuid.UUID
	for _, cm := range reclaimed {
		if cm.Monitor.ID == m.ID {
			token2 = cm.ClaimToken
			break
		}
	}
	if token2 == uuid.Nil || token2 == token {
		t.Fatalf("expected monitor %v to be reclaimable with a new token, got %v", m.ID, token2)
	}
}

func TestMigration_AddMonitorClaims_LiveDB(t *testing.T) {
	database := connectTestDB(t)
	if database == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	migrationBytes, err := os.ReadFile(filepath.Join("..", "..", "migrations", "001_add_monitor_claims.sql"))
	if err != nil {
		t.Fatalf("failed to read migration script: %v", err)
	}
	migrationSQL := string(migrationBytes)

	s := New(database.Pool())
	m, err := s.Create(ctx, &model.Monitor{
		Name:            "Migration Data Preservation Monitor",
		URL:             "https://example.com/migration",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	t.Cleanup(func() {
		cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cCancel()
		_ = s.Delete(cCtx, m.ID)
	})

	// Executing the upgrade script repeatedly must be idempotent and preserve existing rows.
	for i := 0; i < 2; i++ {
		if _, err := database.Pool().Exec(ctx, migrationSQL); err != nil {
			t.Fatalf("migration execution %d failed: %v", i+1, err)
		}
	}

	fetched, err := s.GetByID(ctx, m.ID)
	if err != nil {
		t.Fatalf("expected existing monitor row preserved after migration, got: %v", err)
	}
	if fetched.Name != m.Name {
		t.Errorf("expected preserved name %q, got %q", m.Name, fetched.Name)
	}
}
