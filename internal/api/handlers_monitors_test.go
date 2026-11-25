package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"pulse/internal/api"
	"pulse/internal/model"
	"pulse/internal/store"
)

// fakeMonitorRepository implements api.MonitorRepository in-memory for testing.
type fakeMonitorRepository struct {
	mu              sync.Mutex
	monitors        map[uuid.UUID]*model.Monitor
	checks          map[uuid.UUID][]*model.CheckResult
	createCalls     int
	listChecksCalls int
	statsCalls      int
	createErr       error
	getErr          error
	listErr         error
	deleteErr       error
	listChecksErr   error
	statsErr        error
}

func newFakeMonitorRepo() *fakeMonitorRepository {
	return &fakeMonitorRepository{
		monitors: make(map[uuid.UUID]*model.Monitor),
		checks:   make(map[uuid.UUID][]*model.CheckResult),
	}
}

func (f *fakeMonitorRepository) Create(ctx context.Context, m *model.Monitor) (*model.Monitor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.createCalls++

	if f.createErr != nil {
		return nil, f.createErr
	}

	if m.ID == uuid.Nil {
		m.ID = uuid.New()
	}
	now := time.Now().UTC()
	m.CreatedAt = now
	m.UpdatedAt = now
	m.NextCheckAt = now
	m.CurrentStatus = model.StatusUnknown
	m.ConsecutiveFailures = 0

	cp := *m
	f.monitors[m.ID] = &cp
	return &cp, nil
}

func (f *fakeMonitorRepository) CreateCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.createCalls
}

func (f *fakeMonitorRepository) GetByID(ctx context.Context, id uuid.UUID) (*model.Monitor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.getErr != nil {
		return nil, f.getErr
	}

	m, ok := f.monitors[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *m
	return &cp, nil
}

func (f *fakeMonitorRepository) List(ctx context.Context) ([]*model.Monitor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.listErr != nil {
		return nil, f.listErr
	}

	result := make([]*model.Monitor, 0, len(f.monitors))
	for _, m := range f.monitors {
		cp := *m
		result = append(result, &cp)
	}
	return result, nil
}

func (f *fakeMonitorRepository) Delete(ctx context.Context, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.deleteErr != nil {
		return f.deleteErr
	}

	if _, ok := f.monitors[id]; !ok {
		return store.ErrNotFound
	}
	delete(f.monitors, id)
	delete(f.checks, id)
	return nil
}

func (f *fakeMonitorRepository) AddCheck(c *model.CheckResult) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *c
	f.checks[c.MonitorID] = append(f.checks[c.MonitorID], &cp)
}

func (f *fakeMonitorRepository) ListChecksCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listChecksCalls
}

func (f *fakeMonitorRepository) StatsCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.statsCalls
}

func (f *fakeMonitorRepository) ListChecks(ctx context.Context, monitorID uuid.UUID, limit int) ([]*model.CheckResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.listChecksCalls++

	if f.listChecksErr != nil {
		return nil, f.listChecksErr
	}

	if _, ok := f.monitors[monitorID]; !ok {
		return nil, store.ErrNotFound
	}

	existing := f.checks[monitorID]
	sorted := make([]*model.CheckResult, len(existing))
	for i, c := range existing {
		cp := *c
		sorted[i] = &cp
	}
	sort.Slice(sorted, func(i, j int) bool {
		if !sorted[i].CheckedAt.Equal(sorted[j].CheckedAt) {
			return sorted[i].CheckedAt.After(sorted[j].CheckedAt)
		}
		return sorted[i].ID.String() > sorted[j].ID.String()
	})

	if limit < len(sorted) {
		sorted = sorted[:limit]
	}
	return sorted, nil
}

func (f *fakeMonitorRepository) GetMonitorStats(ctx context.Context, monitorID uuid.UUID, hours int) (*model.MonitorStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.statsCalls++

	if f.statsErr != nil {
		return nil, f.statsErr
	}

	if _, ok := f.monitors[monitorID]; !ok {
		return nil, store.ErrNotFound
	}

	cutoff := time.Now().UTC().Add(-time.Duration(hours) * time.Hour)
	stats := &model.MonitorStats{
		MonitorID:   monitorID,
		WindowHours: hours,
	}

	var latencySum int64
	for _, c := range f.checks[monitorID] {
		if c.CheckedAt.Before(cutoff) {
			continue
		}
		stats.TotalChecks++
		if c.Success {
			stats.SuccessfulChecks++
			latencySum += c.LatencyMs
		} else {
			stats.FailedChecks++
		}
	}

	if stats.TotalChecks > 0 {
		avail := (float64(stats.SuccessfulChecks) / float64(stats.TotalChecks)) * 100.0
		stats.Availability = &avail
	}
	if stats.SuccessfulChecks > 0 {
		avg := float64(latencySum) / float64(stats.SuccessfulChecks)
		stats.AverageLatencyMs = &avg
	}

	return stats, nil
}

func setupMonitorTestRouter(repo api.MonitorRepository) http.Handler {
	return api.NewRouterWithRepo(nil, repo)
}

func TestCreateMonitor_Success(t *testing.T) {
	repo := newFakeMonitorRepo()
	router := setupMonitorTestRouter(repo)

	t.Run("creates monitor with custom interval and timeout", func(t *testing.T) {
		payload := map[string]any{
			"name":             "Production API Gateway",
			"url":              "https://api.example.com/health",
			"interval_seconds": 120,
			"timeout_seconds":  10,
		}
		body, _ := json.Marshal(payload)

		req := httptest.NewRequest(http.MethodPost, "/api/v1/monitors", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusCreated {
			t.Fatalf("expected status 201 Created, got %d", res.StatusCode)
		}

		if ct := res.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("expected Content-Type application/json, got %q", ct)
		}

		var created model.Monitor
		if err := json.NewDecoder(res.Body).Decode(&created); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if created.ID == uuid.Nil {
			t.Errorf("expected non-nil UUID")
		}
		if created.Name != "Production API Gateway" {
			t.Errorf("expected name %q, got %q", "Production API Gateway", created.Name)
		}
		if created.URL != "https://api.example.com/health" {
			t.Errorf("expected url %q, got %q", "https://api.example.com/health", created.URL)
		}
		if created.IntervalSeconds != 120 {
			t.Errorf("expected interval 120, got %d", created.IntervalSeconds)
		}
		if created.TimeoutSeconds != 10 {
			t.Errorf("expected timeout 10, got %d", created.TimeoutSeconds)
		}
		if created.CurrentStatus != model.StatusUnknown {
			t.Errorf("expected current_status UNKNOWN, got %q", created.CurrentStatus)
		}
		if created.ConsecutiveFailures != 0 {
			t.Errorf("expected consecutive_failures 0, got %d", created.ConsecutiveFailures)
		}
		if created.LastCheckedAt != nil {
			t.Errorf("expected last_checked_at nil, got %v", created.LastCheckedAt)
		}
		if created.LastSuccessAt != nil {
			t.Errorf("expected last_success_at nil, got %v", created.LastSuccessAt)
		}
	})

	t.Run("applies defaults when interval and timeout are omitted", func(t *testing.T) {
		payload := map[string]any{
			"name": "Default Settings Monitor",
			"url":  "http://example.com/probe",
		}
		body, _ := json.Marshal(payload)

		req := httptest.NewRequest(http.MethodPost, "/api/v1/monitors", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusCreated {
			t.Fatalf("expected status 201 Created, got %d", res.StatusCode)
		}

		var created model.Monitor
		if err := json.NewDecoder(res.Body).Decode(&created); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if created.IntervalSeconds != model.DefaultIntervalSeconds {
			t.Errorf("expected default interval %d, got %d", model.DefaultIntervalSeconds, created.IntervalSeconds)
		}
		if created.TimeoutSeconds != model.DefaultTimeoutSeconds {
			t.Errorf("expected default timeout %d, got %d", model.DefaultTimeoutSeconds, created.TimeoutSeconds)
		}
	})
}

func TestCreateMonitor_ValidationFailures(t *testing.T) {
	tests := []struct {
		name           string
		contentType    string
		body           string
		expectedStatus int
		expectedSubstr string
	}{
		{
			name:           "missing content-type header",
			contentType:    "",
			body:           `{"name":"Test","url":"http://example.com"}`,
			expectedStatus: http.StatusBadRequest,
			expectedSubstr: "Content-Type must be application/json",
		},
		{
			name:           "unsupported content-type text/plain",
			contentType:    "text/plain",
			body:           `{"name":"Test","url":"http://example.com"}`,
			expectedStatus: http.StatusBadRequest,
			expectedSubstr: "Content-Type must be application/json",
		},
		{
			name:           "empty request body",
			contentType:    "application/json",
			body:           "",
			expectedStatus: http.StatusBadRequest,
			expectedSubstr: "request body cannot be empty",
		},
		{
			name:           "malformed JSON",
			contentType:    "application/json",
			body:           `{"name": "invalid JSON", ...}`,
			expectedStatus: http.StatusBadRequest,
			expectedSubstr: "invalid JSON payload",
		},
		{
			name:           "empty name",
			contentType:    "application/json",
			body:           `{"name":"","url":"http://example.com"}`,
			expectedStatus: http.StatusBadRequest,
			expectedSubstr: "name must not be empty",
		},
		{
			name:           "whitespace name",
			contentType:    "application/json",
			body:           `{"name":"   ","url":"http://example.com"}`,
			expectedStatus: http.StatusBadRequest,
			expectedSubstr: "name must not be empty",
		},
		{
			name:           "name exceeding 255 characters",
			contentType:    "application/json",
			body:           `{"name":"` + strings.Repeat("A", 256) + `","url":"http://example.com"}`,
			expectedStatus: http.StatusBadRequest,
			expectedSubstr: "name must not exceed 255 characters",
		},
		{
			name:           "empty url",
			contentType:    "application/json",
			body:           `{"name":"Valid Name","url":""}`,
			expectedStatus: http.StatusBadRequest,
			expectedSubstr: "url must not be empty",
		},
		{
			name:           "relative url without scheme",
			contentType:    "application/json",
			body:           `{"name":"Valid Name","url":"api.example.com/health"}`,
			expectedStatus: http.StatusBadRequest,
			expectedSubstr: "url must be a valid absolute URL",
		},
		{
			name:           "unsupported scheme ftp",
			contentType:    "application/json",
			body:           `{"name":"Valid Name","url":"ftp://example.com/health"}`,
			expectedStatus: http.StatusBadRequest,
			expectedSubstr: "url scheme must be http or https",
		},
		{
			name:           "embedded credentials in URL",
			contentType:    "application/json",
			body:           `{"name":"Valid Name","url":"http://user:pass@example.com/health"}`,
			expectedStatus: http.StatusBadRequest,
			expectedSubstr: "url must not contain embedded user credentials",
		},
		{
			name:           "interval below minimum 30s",
			contentType:    "application/json",
			body:           `{"name":"Valid Name","url":"http://example.com","interval_seconds":29}`,
			expectedStatus: http.StatusBadRequest,
			expectedSubstr: "interval_seconds must be between 30 and 86400",
		},
		{
			name:           "interval above maximum 86400s",
			contentType:    "application/json",
			body:           `{"name":"Valid Name","url":"http://example.com","interval_seconds":86401}`,
			expectedStatus: http.StatusBadRequest,
			expectedSubstr: "interval_seconds must be between 30 and 86400",
		},
		{
			name:           "timeout below minimum 1s",
			contentType:    "application/json",
			body:           `{"name":"Valid Name","url":"http://example.com","timeout_seconds":0}`,
			expectedStatus: http.StatusBadRequest,
			expectedSubstr: "timeout_seconds must be between 1 and 30",
		},
		{
			name:           "timeout above maximum 30s",
			contentType:    "application/json",
			body:           `{"name":"Valid Name","url":"http://example.com","timeout_seconds":31}`,
			expectedStatus: http.StatusBadRequest,
			expectedSubstr: "timeout_seconds must be between 1 and 30",
		},
		{
			name:           "timeout exceeds interval",
			contentType:    "application/json",
			body:           `{"name":"Valid Name","url":"http://example.com","interval_seconds":30,"timeout_seconds":35}`,
			expectedStatus: http.StatusBadRequest,
			expectedSubstr: "timeout_seconds must not exceed interval_seconds",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testRepo := newFakeMonitorRepo()
			testRouter := setupMonitorTestRouter(testRepo)

			req := httptest.NewRequest(http.MethodPost, "/api/v1/monitors", strings.NewReader(tc.body))
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			rec := httptest.NewRecorder()

			testRouter.ServeHTTP(rec, req)
			res := rec.Result()
			defer res.Body.Close()

			if res.StatusCode != tc.expectedStatus {
				t.Fatalf("expected status %d, got %d", tc.expectedStatus, res.StatusCode)
			}

			var errRes api.ErrorResponse
			if err := json.NewDecoder(res.Body).Decode(&errRes); err != nil {
				t.Fatalf("failed to decode error body: %v", err)
			}

			if !strings.Contains(errRes.Error, tc.expectedSubstr) {
				t.Errorf("expected error message to contain %q, got %q", tc.expectedSubstr, errRes.Error)
			}

			if testRepo.CreateCalls() != 0 {
				t.Errorf("expected repository.Create never invoked for invalid input, got %d calls", testRepo.CreateCalls())
			}
		})
	}
}

func TestCreateMonitor_BodySizeLimit(t *testing.T) {
	repo := newFakeMonitorRepo()
	router := setupMonitorTestRouter(repo)

	// Construct an oversized body exceeding 1 MB
	hugeName := strings.Repeat("A", 1024*1024+10)
	payload := `{"name":"` + hugeName + `","url":"http://example.com"}`

	req := httptest.NewRequest(http.MethodPost, "/api/v1/monitors", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected status 400 Bad Request for oversized payload, got %d", res.StatusCode)
	}

	var errRes api.ErrorResponse
	if err := json.NewDecoder(res.Body).Decode(&errRes); err != nil {
		t.Fatalf("failed to decode error body: %v", err)
	}
	if !strings.Contains(errRes.Error, "exceeds maximum allowed size") {
		t.Errorf("expected body size error, got %q", errRes.Error)
	}
	if repo.CreateCalls() != 0 {
		t.Errorf("expected repository.Create never invoked on oversized body, got %d calls", repo.CreateCalls())
	}
}

func TestCreateMonitor_TrailingContentAndSize(t *testing.T) {
	t.Run("valid JSON object followed by whitespace is accepted", func(t *testing.T) {
		repo := newFakeMonitorRepo()
		router := setupMonitorTestRouter(repo)

		payload := `{"name":"Whitespace Test","url":"http://example.com"}` + "   \n\t  \r\n"
		req := httptest.NewRequest(http.MethodPost, "/api/v1/monitors", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusCreated {
			t.Fatalf("expected status 201 Created for JSON with trailing whitespace, got %d", res.StatusCode)
		}
		if repo.CreateCalls() != 1 {
			t.Errorf("expected exactly 1 repository.Create call, got %d", repo.CreateCalls())
		}
	})

	t.Run("valid JSON object followed by second JSON object is rejected with 400", func(t *testing.T) {
		repo := newFakeMonitorRepo()
		router := setupMonitorTestRouter(repo)

		payload := `{"name":"First","url":"http://example.com"} {"name":"Second","url":"http://example.com"}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/monitors", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected status 400 Bad Request for second JSON object, got %d", res.StatusCode)
		}
		var errRes api.ErrorResponse
		if err := json.NewDecoder(res.Body).Decode(&errRes); err != nil {
			t.Fatalf("failed to decode error body: %v", err)
		}
		if !strings.Contains(errRes.Error, "single JSON object") {
			t.Errorf("expected error about single JSON object, got %q", errRes.Error)
		}
		if repo.CreateCalls() != 0 {
			t.Errorf("expected repository.Create never invoked for trailing JSON, got %d calls", repo.CreateCalls())
		}
	})

	t.Run("valid JSON object followed by arbitrary invalid characters is rejected with 400", func(t *testing.T) {
		repo := newFakeMonitorRepo()
		router := setupMonitorTestRouter(repo)

		payload := `{"name":"First","url":"http://example.com"} arbitrary trailing invalid data !@#$`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/monitors", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected status 400 Bad Request for trailing invalid data, got %d", res.StatusCode)
		}
		var errRes api.ErrorResponse
		if err := json.NewDecoder(res.Body).Decode(&errRes); err != nil {
			t.Fatalf("failed to decode error body: %v", err)
		}
		if !strings.Contains(errRes.Error, "invalid JSON payload") {
			t.Errorf("expected invalid JSON payload error, got %q", errRes.Error)
		}
		if repo.CreateCalls() != 0 {
			t.Errorf("expected repository.Create never invoked for trailing invalid content, got %d calls", repo.CreateCalls())
		}
	})

	t.Run("valid JSON object followed by more than 1 MB of trailing whitespace is rejected with body-size error", func(t *testing.T) {
		repo := newFakeMonitorRepo()
		router := setupMonitorTestRouter(repo)

		// 1MB is 1<<20 bytes; trailing whitespace exceeds 1MB
		payload := `{"name":"First","url":"http://example.com"}` + strings.Repeat(" ", 1024*1024+100)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/monitors", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected status 400 Bad Request for oversized whitespace, got %d", res.StatusCode)
		}
		var errRes api.ErrorResponse
		if err := json.NewDecoder(res.Body).Decode(&errRes); err != nil {
			t.Fatalf("failed to decode error body: %v", err)
		}
		if !strings.Contains(errRes.Error, "exceeds maximum allowed size") {
			t.Errorf("expected body-size error, got %q", errRes.Error)
		}
		if repo.CreateCalls() != 0 {
			t.Errorf("expected repository.Create never invoked for oversized whitespace, got %d calls", repo.CreateCalls())
		}
	})

	t.Run("valid JSON object followed by more than 1 MB of trailing data is rejected with body-size error", func(t *testing.T) {
		repo := newFakeMonitorRepo()
		router := setupMonitorTestRouter(repo)

		// Trailing JSON data exceeding 1 MB
		payload := `{"name":"First","url":"http://example.com"} "` + strings.Repeat("A", 1024*1024+100) + `"`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/monitors", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected status 400 Bad Request for oversized trailing data, got %d", res.StatusCode)
		}
		var errRes api.ErrorResponse
		if err := json.NewDecoder(res.Body).Decode(&errRes); err != nil {
			t.Fatalf("failed to decode error body: %v", err)
		}
		if !strings.Contains(errRes.Error, "exceeds maximum allowed size") {
			t.Errorf("expected body-size error, got %q", errRes.Error)
		}
		if repo.CreateCalls() != 0 {
			t.Errorf("expected repository.Create never invoked for oversized trailing data, got %d calls", repo.CreateCalls())
		}
	})
}

func TestCreateMonitor_RepositoryInternalError(t *testing.T) {
	repo := newFakeMonitorRepo()
	repo.createErr = errors.New("pq: connection closed unexpectedly")
	router := setupMonitorTestRouter(repo)

	payload := `{"name":"Service","url":"https://example.com"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/monitors", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", res.StatusCode)
	}

	var errRes api.ErrorResponse
	if err := json.NewDecoder(res.Body).Decode(&errRes); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	// Verify database details are not leaked
	if strings.Contains(errRes.Error, "pq:") {
		t.Errorf("internal SQL error leaked to client: %q", errRes.Error)
	}
	if errRes.Error != "failed to create monitor" {
		t.Errorf("expected generic error message, got %q", errRes.Error)
	}
}

func TestListMonitors_Success(t *testing.T) {
	repo := newFakeMonitorRepo()
	router := setupMonitorTestRouter(repo)

	t.Run("empty list returns 200 with empty array", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/monitors", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200 OK, got %d", res.StatusCode)
		}

		var listRes api.ListMonitorsResponse
		if err := json.NewDecoder(res.Body).Decode(&listRes); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if listRes.Monitors == nil || len(listRes.Monitors) != 0 {
			t.Errorf("expected empty non-nil slice, got %v", listRes.Monitors)
		}
	})

	t.Run("populated list returns 200 with all monitors", func(t *testing.T) {
		_, _ = repo.Create(context.Background(), &model.Monitor{Name: "M1", URL: "http://one.example.com", IntervalSeconds: 60, TimeoutSeconds: 5})
		_, _ = repo.Create(context.Background(), &model.Monitor{Name: "M2", URL: "http://two.example.com", IntervalSeconds: 60, TimeoutSeconds: 5})

		req := httptest.NewRequest(http.MethodGet, "/api/v1/monitors", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200 OK, got %d", res.StatusCode)
		}

		var listRes api.ListMonitorsResponse
		if err := json.NewDecoder(res.Body).Decode(&listRes); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if len(listRes.Monitors) != 2 {
			t.Fatalf("expected 2 monitors, got %d", len(listRes.Monitors))
		}
	})
}

func TestListMonitors_RepositoryError(t *testing.T) {
	repo := newFakeMonitorRepo()
	repo.listErr = errors.New("sql query failed")
	router := setupMonitorTestRouter(repo)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/monitors", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", res.StatusCode)
	}
	var errRes api.ErrorResponse
	_ = json.NewDecoder(res.Body).Decode(&errRes)
	if strings.Contains(errRes.Error, "sql") {
		t.Errorf("SQL details leaked: %q", errRes.Error)
	}
}

func TestGetMonitor_Success(t *testing.T) {
	repo := newFakeMonitorRepo()
	m, err := repo.Create(context.Background(), &model.Monitor{
		Name:            "Target Gateway",
		URL:             "https://target.example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
	})
	if err != nil {
		t.Fatalf("failed to create seed monitor: %v", err)
	}

	router := setupMonitorTestRouter(repo)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+m.ID.String(), nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 OK, got %d", res.StatusCode)
	}

	var fetched model.Monitor
	if err := json.NewDecoder(res.Body).Decode(&fetched); err != nil {
		t.Fatalf("failed to decode monitor: %v", err)
	}
	if fetched.ID != m.ID {
		t.Errorf("expected ID %v, got %v", m.ID, fetched.ID)
	}
	if fetched.Name != m.Name {
		t.Errorf("expected name %q, got %q", m.Name, fetched.Name)
	}
}

func TestGetMonitor_NotFoundAndInvalidUUID(t *testing.T) {
	repo := newFakeMonitorRepo()
	router := setupMonitorTestRouter(repo)

	t.Run("non-existent monitor returns 404", func(t *testing.T) {
		randomUUID := uuid.New().String()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+randomUUID, nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("expected status 404 Not Found, got %d", res.StatusCode)
		}

		var errRes api.ErrorResponse
		_ = json.NewDecoder(res.Body).Decode(&errRes)
		if errRes.Error != "monitor not found" {
			t.Errorf("expected 'monitor not found', got %q", errRes.Error)
		}
	})

	t.Run("invalid UUID returns 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/not-a-valid-uuid", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected status 400 Bad Request, got %d", res.StatusCode)
		}

		var errRes api.ErrorResponse
		_ = json.NewDecoder(res.Body).Decode(&errRes)
		if !strings.Contains(errRes.Error, "must be a valid UUID") {
			t.Errorf("expected error about UUID syntax, got %q", errRes.Error)
		}
	})
}

func TestDeleteMonitor_Success(t *testing.T) {
	repo := newFakeMonitorRepo()
	m, err := repo.Create(context.Background(), &model.Monitor{
		Name:            "To Delete",
		URL:             "http://example.com/delete",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
	})
	if err != nil {
		t.Fatalf("failed to seed monitor: %v", err)
	}

	router := setupMonitorTestRouter(repo)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/monitors/"+m.ID.String(), nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("expected status 204 No Content as documented, got %d", res.StatusCode)
	}

	body, _ := io.ReadAll(res.Body)
	if len(body) != 0 {
		t.Errorf("expected empty body for 204 No Content, got %q", string(body))
	}

	// Verify monitor was deleted from repository
	_, err = repo.GetByID(context.Background(), m.ID)
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected monitor to be deleted, got err: %v", err)
	}
}

func TestDeleteMonitor_NotFoundAndInvalidUUID(t *testing.T) {
	repo := newFakeMonitorRepo()
	router := setupMonitorTestRouter(repo)

	t.Run("non-existent monitor returns 404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/monitors/"+uuid.New().String(), nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("expected status 404 Not Found, got %d", res.StatusCode)
		}

		var errRes api.ErrorResponse
		_ = json.NewDecoder(res.Body).Decode(&errRes)
		if errRes.Error != "monitor not found" {
			t.Errorf("expected 'monitor not found', got %q", errRes.Error)
		}
	})

	t.Run("invalid UUID returns 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/monitors/12345", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected status 400 Bad Request, got %d", res.StatusCode)
		}

		var errRes api.ErrorResponse
		_ = json.NewDecoder(res.Body).Decode(&errRes)
		if !strings.Contains(errRes.Error, "must be a valid UUID") {
			t.Errorf("expected error about valid UUID, got %q", errRes.Error)
		}
	})
}

func TestMonitorRoutes_MethodNotAllowed(t *testing.T) {
	repo := newFakeMonitorRepo()
	router := setupMonitorTestRouter(repo)

	disallowedCollection := []string{
		http.MethodPut,
		http.MethodDelete,
		http.MethodPatch,
	}

	for _, method := range disallowedCollection {
		t.Run("Collection_"+method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/api/v1/monitors", nil)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)
			res := rec.Result()
			defer res.Body.Close()

			if res.StatusCode != http.StatusMethodNotAllowed {
				t.Errorf("expected status 405 Method Not Allowed for %s, got %d", method, res.StatusCode)
			}

			var errRes api.ErrorResponse
			_ = json.NewDecoder(res.Body).Decode(&errRes)
			if errRes.Error != "method not allowed" {
				t.Errorf("expected 'method not allowed', got %q", errRes.Error)
			}
		})
	}
}

func TestListMonitorChecks_SuccessOrderingAndLimits(t *testing.T) {
	t.Run("empty history returns 200 with empty checks array and default limit 50", func(t *testing.T) {
		repo := newFakeMonitorRepo()
		m, err := repo.Create(context.Background(), &model.Monitor{
			Name:            "Empty History Monitor",
			URL:             "https://example.com/health",
			IntervalSeconds: 60,
			TimeoutSeconds:  5,
		})
		if err != nil {
			t.Fatalf("failed to seed monitor: %v", err)
		}

		router := setupMonitorTestRouter(repo)
		req := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+m.ID.String()+"/checks", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200 OK, got %d", res.StatusCode)
		}

		rawBody, _ := io.ReadAll(res.Body)
		var raw map[string]any
		if err := json.Unmarshal(rawBody, &raw); err != nil {
			t.Fatalf("failed to decode JSON: %v", err)
		}
		if checksVal, ok := raw["checks"]; !ok || checksVal == nil {
			t.Fatalf("expected non-null checks array in JSON, got %s", string(rawBody))
		}

		var out api.ListChecksResponse
		if err := json.Unmarshal(rawBody, &out); err != nil {
			t.Fatalf("failed to decode ListChecksResponse: %v", err)
		}
		if out.MonitorID != m.ID {
			t.Errorf("expected monitor_id %v, got %v", m.ID, out.MonitorID)
		}
		if out.Limit != 50 {
			t.Errorf("expected default limit 50, got %d", out.Limit)
		}
		if len(out.Checks) != 0 {
			t.Errorf("expected 0 checks, got %d", len(out.Checks))
		}
	})

	t.Run("orders newest first and enforces default 50 and explicit limits", func(t *testing.T) {
		repo := newFakeMonitorRepo()
		m, err := repo.Create(context.Background(), &model.Monitor{
			Name:            "Ordered Checks Monitor",
			URL:             "https://example.com/checks",
			IntervalSeconds: 60,
			TimeoutSeconds:  5,
		})
		if err != nil {
			t.Fatalf("failed to seed monitor: %v", err)
		}

		base := time.Now().UTC().Add(-2 * time.Hour)
		status200 := 200
		status503 := 503
		err503 := "HTTP status 503 Service Unavailable"

		var ids []uuid.UUID
		for i := 0; i < 60; i++ {
			id := uuid.New()
			ids = append(ids, id)
			success := i%2 == 0
			httpStatus := &status200
			var errMsg *string
			if !success {
				httpStatus = &status503
				errMsg = &err503
			}
			repo.AddCheck(&model.CheckResult{
				ID:           id,
				MonitorID:    m.ID,
				CheckedAt:    base.Add(time.Duration(i) * time.Minute),
				Success:      success,
				HTTPStatus:   httpStatus,
				LatencyMs:    int64(100 + i),
				ErrorMessage: errMsg,
			})
		}

		router := setupMonitorTestRouter(repo)

		// Default limit = 50 -> returns 50 newest checks (indices 59 down to 10)
		req := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+m.ID.String()+"/checks", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200 OK, got %d", res.StatusCode)
		}
		var defaultOut api.ListChecksResponse
		if err := json.NewDecoder(res.Body).Decode(&defaultOut); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if defaultOut.Limit != 50 || len(defaultOut.Checks) != 50 {
			t.Fatalf("expected limit 50 and 50 checks, got limit=%d len=%d", defaultOut.Limit, len(defaultOut.Checks))
		}
		if defaultOut.Checks[0].ID != ids[59] || defaultOut.Checks[49].ID != ids[10] {
			t.Errorf("unexpected newest-first ordering: first=%v last=%v", defaultOut.Checks[0].ID, defaultOut.Checks[49].ID)
		}

		// Explicit limit = 3 -> returns 3 newest checks
		req2 := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+m.ID.String()+"/checks?limit=3", nil)
		rec2 := httptest.NewRecorder()
		router.ServeHTTP(rec2, req2)
		res2 := rec2.Result()
		defer res2.Body.Close()

		if res2.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200 OK, got %d", res2.StatusCode)
		}
		var limitOut api.ListChecksResponse
		if err := json.NewDecoder(res2.Body).Decode(&limitOut); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if limitOut.Limit != 3 || len(limitOut.Checks) != 3 {
			t.Fatalf("expected limit 3 and 3 checks, got limit=%d len=%d", limitOut.Limit, len(limitOut.Checks))
		}
		for i := 0; i < 3; i++ {
			if limitOut.Checks[i].ID != ids[59-i] {
				t.Errorf("checks[%d].ID = %v, want %v", i, limitOut.Checks[i].ID, ids[59-i])
			}
		}

		// Explicit boundary limit = 200 is accepted
		reqMax := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+m.ID.String()+"/checks?limit=200", nil)
		recMax := httptest.NewRecorder()
		router.ServeHTTP(recMax, reqMax)
		resMax := recMax.Result()
		defer resMax.Body.Close()
		if resMax.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200 OK for limit=200, got %d", resMax.StatusCode)
		}
	})
}

func TestListMonitorChecks_ErrorsAndValidation(t *testing.T) {
	t.Run("unknown monitor returns 404", func(t *testing.T) {
		repo := newFakeMonitorRepo()
		router := setupMonitorTestRouter(repo)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+uuid.New().String()+"/checks", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("expected 404 Not Found, got %d", res.StatusCode)
		}
		var errRes api.ErrorResponse
		_ = json.NewDecoder(res.Body).Decode(&errRes)
		if errRes.Error != "monitor not found" {
			t.Errorf("expected 'monitor not found', got %q", errRes.Error)
		}
	})

	t.Run("invalid UUID returns 400", func(t *testing.T) {
		repo := newFakeMonitorRepo()
		router := setupMonitorTestRouter(repo)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/not-a-uuid/checks", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got %d", res.StatusCode)
		}
		if repo.ListChecksCalls() != 0 {
			t.Errorf("expected repo.ListChecks not called on invalid UUID")
		}
	})

	invalidLimits := []string{
		"limit=0",
		"limit=-1",
		"limit=201",
		"limit=abc",
		"limit=",
		"limit=10&limit=20",
	}
	for _, query := range invalidLimits {
		t.Run("invalid_"+query, func(t *testing.T) {
			repo := newFakeMonitorRepo()
			m, _ := repo.Create(context.Background(), &model.Monitor{
				Name: "M", URL: "https://example.com", IntervalSeconds: 60, TimeoutSeconds: 5,
			})
			router := setupMonitorTestRouter(repo)

			req := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+m.ID.String()+"/checks?"+query, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			res := rec.Result()
			defer res.Body.Close()

			if res.StatusCode != http.StatusBadRequest {
				t.Fatalf("expected 400 Bad Request for %s, got %d", query, res.StatusCode)
			}
			var errRes api.ErrorResponse
			_ = json.NewDecoder(res.Body).Decode(&errRes)
			if errRes.Error != "invalid query parameter: limit must be between 1 and 200" {
				t.Errorf("unexpected error message: %q", errRes.Error)
			}
			if repo.ListChecksCalls() != 0 {
				t.Errorf("expected repo.ListChecks not called for invalid limit")
			}
		})
	}

	t.Run("repository internal error does not leak SQL details", func(t *testing.T) {
		repo := newFakeMonitorRepo()
		repo.listChecksErr = errors.New("pq: relation check_results error")
		router := setupMonitorTestRouter(repo)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+uuid.New().String()+"/checks", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusInternalServerError {
			t.Fatalf("expected 500, got %d", res.StatusCode)
		}
		var errRes api.ErrorResponse
		_ = json.NewDecoder(res.Body).Decode(&errRes)
		if strings.Contains(errRes.Error, "pq:") {
			t.Errorf("leaked internal error: %q", errRes.Error)
		}
	})
}

func TestGetMonitorStats_SuccessAndMath(t *testing.T) {
	t.Run("empty window returns null availability and null average_latency_ms", func(t *testing.T) {
		repo := newFakeMonitorRepo()
		m, _ := repo.Create(context.Background(), &model.Monitor{
			Name: "Empty Stats Monitor", URL: "https://example.com", IntervalSeconds: 60, TimeoutSeconds: 5,
		})
		router := setupMonitorTestRouter(repo)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+m.ID.String()+"/stats", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", res.StatusCode)
		}

		rawBytes, _ := io.ReadAll(res.Body)
		var rawMap map[string]any
		if err := json.Unmarshal(rawBytes, &rawMap); err != nil {
			t.Fatalf("failed to unmarshal raw JSON: %v", err)
		}
		if v, exists := rawMap["availability"]; !exists || v != nil {
			t.Errorf("expected availability key to be literal null, got exists=%v val=%v", exists, v)
		}
		if v, exists := rawMap["average_latency_ms"]; !exists || v != nil {
			t.Errorf("expected average_latency_ms key to be literal null, got exists=%v val=%v", exists, v)
		}

		var stats model.MonitorStats
		if err := json.Unmarshal(rawBytes, &stats); err != nil {
			t.Fatalf("failed to decode MonitorStats: %v", err)
		}
		if stats.WindowHours != 24 || stats.TotalChecks != 0 || stats.SuccessfulChecks != 0 || stats.FailedChecks != 0 {
			t.Errorf("unexpected empty stats: %+v", stats)
		}
	})

	t.Run("all failed checks returns 0 availability and null average_latency_ms", func(t *testing.T) {
		repo := newFakeMonitorRepo()
		m, _ := repo.Create(context.Background(), &model.Monitor{
			Name: "All Failed Monitor", URL: "https://example.com", IntervalSeconds: 60, TimeoutSeconds: 5,
		})
		now := time.Now().UTC()
		status500 := 500
		err500 := "HTTP status 500 Internal Server Error"
		repo.AddCheck(&model.CheckResult{ID: uuid.New(), MonitorID: m.ID, CheckedAt: now.Add(-10 * time.Minute), Success: false, HTTPStatus: &status500, LatencyMs: 400, ErrorMessage: &err500})
		repo.AddCheck(&model.CheckResult{ID: uuid.New(), MonitorID: m.ID, CheckedAt: now.Add(-5 * time.Minute), Success: false, HTTPStatus: &status500, LatencyMs: 600, ErrorMessage: &err500})

		router := setupMonitorTestRouter(repo)
		req := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+m.ID.String()+"/stats", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", res.StatusCode)
		}

		var stats model.MonitorStats
		if err := json.NewDecoder(res.Body).Decode(&stats); err != nil {
			t.Fatalf("failed to decode MonitorStats: %v", err)
		}
		if stats.TotalChecks != 2 || stats.SuccessfulChecks != 0 || stats.FailedChecks != 2 {
			t.Errorf("unexpected counts: %+v", stats)
		}
		if stats.Availability == nil || *stats.Availability != 0 {
			t.Errorf("expected availability 0, got %v", stats.Availability)
		}
		if stats.AverageLatencyMs != nil {
			t.Errorf("expected nil average_latency_ms when no successful observations exist, got %v", *stats.AverageLatencyMs)
		}
	})

	t.Run("mixed results and window filtering compute accurate math", func(t *testing.T) {
		repo := newFakeMonitorRepo()
		m, _ := repo.Create(context.Background(), &model.Monitor{
			Name: "Mixed Stats Monitor", URL: "https://example.com", IntervalSeconds: 60, TimeoutSeconds: 5,
		})
		now := time.Now().UTC()
		status200 := 200
		status502 := 502
		err502 := "HTTP status 502 Bad Gateway"

		// Outside default 24h window (30h ago)
		repo.AddCheck(&model.CheckResult{ID: uuid.New(), MonitorID: m.ID, CheckedAt: now.Add(-30 * time.Hour), Success: true, HTTPStatus: &status200, LatencyMs: 900})
		// Inside 24h window, outside 2h window (5h ago)
		repo.AddCheck(&model.CheckResult{ID: uuid.New(), MonitorID: m.ID, CheckedAt: now.Add(-5 * time.Hour), Success: false, HTTPStatus: &status502, LatencyMs: 3000, ErrorMessage: &err502})
		// Inside 2h window: 2 successes (100, 169 -> avg 134.5), 1 failure (5000 excluded from avg latency)
		repo.AddCheck(&model.CheckResult{ID: uuid.New(), MonitorID: m.ID, CheckedAt: now.Add(-30 * time.Minute), Success: true, HTTPStatus: &status200, LatencyMs: 100})
		repo.AddCheck(&model.CheckResult{ID: uuid.New(), MonitorID: m.ID, CheckedAt: now.Add(-20 * time.Minute), Success: true, HTTPStatus: &status200, LatencyMs: 169})
		repo.AddCheck(&model.CheckResult{ID: uuid.New(), MonitorID: m.ID, CheckedAt: now.Add(-10 * time.Minute), Success: false, HTTPStatus: &status502, LatencyMs: 5000, ErrorMessage: &err502})

		router := setupMonitorTestRouter(repo)

		// Default 24h window: 4 checks (2 success, 2 fail) -> 50.0% availability, 134.5ms avg latency
		req24 := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+m.ID.String()+"/stats", nil)
		rec24 := httptest.NewRecorder()
		router.ServeHTTP(rec24, req24)
		res24 := rec24.Result()
		defer res24.Body.Close()

		if res24.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", res24.StatusCode)
		}
		var s24 model.MonitorStats
		if err := json.NewDecoder(res24.Body).Decode(&s24); err != nil {
			t.Fatalf("failed to decode 24h stats: %v", err)
		}
		if s24.WindowHours != 24 || s24.TotalChecks != 4 || s24.SuccessfulChecks != 2 || s24.FailedChecks != 2 {
			t.Errorf("unexpected 24h stats: %+v", s24)
		}
		if s24.Availability == nil || *s24.Availability != 50.0 {
			t.Errorf("expected 24h availability 50.0, got %v", s24.Availability)
		}
		if s24.AverageLatencyMs == nil || *s24.AverageLatencyMs != 134.5 {
			t.Errorf("expected 24h average_latency_ms 134.5, got %v", s24.AverageLatencyMs)
		}

		// Custom 2h window: 3 checks (2 success, 1 fail) -> 66.666...% availability, 134.5ms avg latency
		req2 := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+m.ID.String()+"/stats?hours=2", nil)
		rec2 := httptest.NewRecorder()
		router.ServeHTTP(rec2, req2)
		res2 := rec2.Result()
		defer res2.Body.Close()

		if res2.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", res2.StatusCode)
		}
		var s2 model.MonitorStats
		if err := json.NewDecoder(res2.Body).Decode(&s2); err != nil {
			t.Fatalf("failed to decode 2h stats: %v", err)
		}
		if s2.WindowHours != 2 || s2.TotalChecks != 3 || s2.SuccessfulChecks != 2 || s2.FailedChecks != 1 {
			t.Errorf("unexpected 2h stats: %+v", s2)
		}
		wantAvail := (2.0 / 3.0) * 100.0
		if s2.Availability == nil || math.Abs(*s2.Availability-wantAvail) > 1e-9 {
			t.Errorf("expected 2h availability %v, got %v", wantAvail, s2.Availability)
		}
		if s2.AverageLatencyMs == nil || *s2.AverageLatencyMs != 134.5 {
			t.Errorf("expected 2h average_latency_ms 134.5, got %v", s2.AverageLatencyMs)
		}

		// Custom max 720h window includes all 5 checks
		req720 := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+m.ID.String()+"/stats?hours=720", nil)
		rec720 := httptest.NewRecorder()
		router.ServeHTTP(rec720, req720)
		res720 := rec720.Result()
		defer res720.Body.Close()

		if res720.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 OK for hours=720, got %d", res720.StatusCode)
		}
		var s720 model.MonitorStats
		if err := json.NewDecoder(res720.Body).Decode(&s720); err != nil {
			t.Fatalf("failed to decode 720h stats: %v", err)
		}
		if s720.TotalChecks != 5 || s720.SuccessfulChecks != 3 || s720.FailedChecks != 2 {
			t.Errorf("unexpected 720h stats: %+v", s720)
		}
	})
}

func TestGetMonitorStats_ErrorsAndValidation(t *testing.T) {
	t.Run("unknown monitor returns 404", func(t *testing.T) {
		repo := newFakeMonitorRepo()
		router := setupMonitorTestRouter(repo)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+uuid.New().String()+"/stats", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("expected 404 Not Found, got %d", res.StatusCode)
		}
		var errRes api.ErrorResponse
		_ = json.NewDecoder(res.Body).Decode(&errRes)
		if errRes.Error != "monitor not found" {
			t.Errorf("expected 'monitor not found', got %q", errRes.Error)
		}
	})

	t.Run("invalid UUID returns 400", func(t *testing.T) {
		repo := newFakeMonitorRepo()
		router := setupMonitorTestRouter(repo)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/bad-uuid/stats", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got %d", res.StatusCode)
		}
		if repo.StatsCalls() != 0 {
			t.Errorf("expected repo.GetMonitorStats not called on invalid UUID")
		}
	})

	invalidHours := []string{
		"hours=0",
		"hours=-5",
		"hours=721",
		"hours=abc",
		"hours=",
		"hours=1&hours=2",
	}
	for _, query := range invalidHours {
		t.Run("invalid_"+query, func(t *testing.T) {
			repo := newFakeMonitorRepo()
			m, _ := repo.Create(context.Background(), &model.Monitor{
				Name: "M", URL: "https://example.com", IntervalSeconds: 60, TimeoutSeconds: 5,
			})
			router := setupMonitorTestRouter(repo)

			req := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+m.ID.String()+"/stats?"+query, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			res := rec.Result()
			defer res.Body.Close()

			if res.StatusCode != http.StatusBadRequest {
				t.Fatalf("expected 400 Bad Request for %s, got %d", query, res.StatusCode)
			}
			var errRes api.ErrorResponse
			_ = json.NewDecoder(res.Body).Decode(&errRes)
			if errRes.Error != "invalid query parameter: hours must be between 1 and 720" {
				t.Errorf("unexpected error message: %q", errRes.Error)
			}
			if repo.StatsCalls() != 0 {
				t.Errorf("expected repo.GetMonitorStats not called for invalid hours")
			}
		})
	}

	t.Run("repository internal error does not leak SQL details", func(t *testing.T) {
		repo := newFakeMonitorRepo()
		repo.statsErr = errors.New("pq: syntax error at or near")
		router := setupMonitorTestRouter(repo)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+uuid.New().String()+"/stats", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusInternalServerError {
			t.Fatalf("expected 500, got %d", res.StatusCode)
		}
		var errRes api.ErrorResponse
		_ = json.NewDecoder(res.Body).Decode(&errRes)
		if strings.Contains(errRes.Error, "pq:") {
			t.Errorf("leaked internal error: %q", errRes.Error)
		}
	})
}

func TestMonitorHistoryAndStatsRoutes_MethodNotAllowed(t *testing.T) {
	repo := newFakeMonitorRepo()
	router := setupMonitorTestRouter(repo)
	id := uuid.New().String()

	for _, suffix := range []string{"/checks", "/stats"} {
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
			t.Run(method+"_"+suffix, func(t *testing.T) {
				req := httptest.NewRequest(method, "/api/v1/monitors/"+id+suffix, nil)
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				res := rec.Result()
				defer res.Body.Close()

				if res.StatusCode != http.StatusMethodNotAllowed {
					t.Errorf("expected 405 Method Not Allowed for %s %s, got %d", method, suffix, res.StatusCode)
				}
			})
		}
	}
}
