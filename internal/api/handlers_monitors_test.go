package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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
	mu          sync.Mutex
	monitors    map[uuid.UUID]*model.Monitor
	createCalls int
	createErr   error
	getErr      error
	listErr     error
	deleteErr   error
}

func newFakeMonitorRepo() *fakeMonitorRepository {
	return &fakeMonitorRepository{
		monitors: make(map[uuid.UUID]*model.Monitor),
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
	return nil
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
