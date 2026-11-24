package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"pulse/internal/api"
)

type mockReadyChecker struct {
	err error
}

func (m *mockReadyChecker) Ping(ctx context.Context) error {
	return m.err
}

func TestReadyz_Success(t *testing.T) {
	checker := &mockReadyChecker{err: nil}
	router := api.NewRouter(nil, checker)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	res := rec.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 OK, got %d", res.StatusCode)
	}

	contentType := res.Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", contentType)
	}

	var readyRes api.ReadyResponse
	if err := json.NewDecoder(res.Body).Decode(&readyRes); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if readyRes.Status != "ready" {
		t.Errorf("expected status 'ready', got %q", readyRes.Status)
	}
	if readyRes.Error != "" {
		t.Errorf("expected empty error, got %q", readyRes.Error)
	}
}

func TestReadyz_DatabaseFailure(t *testing.T) {
	checker := &mockReadyChecker{err: errors.New("connection refused")}
	router := api.NewRouter(nil, checker)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	res := rec.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503 Service Unavailable, got %d", res.StatusCode)
	}

	contentType := res.Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", contentType)
	}

	var readyRes api.ReadyResponse
	if err := json.NewDecoder(res.Body).Decode(&readyRes); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if readyRes.Status != "unavailable" {
		t.Errorf("expected status 'unavailable', got %q", readyRes.Status)
	}
	if !strings.Contains(readyRes.Error, "connection refused") {
		t.Errorf("expected error to mention 'connection refused', got %q", readyRes.Error)
	}
}

func TestReadyz_NilChecker(t *testing.T) {
	router := api.NewRouter(nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	res := rec.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503 Service Unavailable, got %d", res.StatusCode)
	}

	var readyRes api.ReadyResponse
	if err := json.NewDecoder(res.Body).Decode(&readyRes); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if readyRes.Status != "unavailable" {
		t.Errorf("expected status 'unavailable', got %q", readyRes.Status)
	}
}

func TestReadyz_Head(t *testing.T) {
	t.Run("ready HEAD returns 200 with empty body", func(t *testing.T) {
		checker := &mockReadyChecker{err: nil}
		router := api.NewRouter(nil, checker)

		req := httptest.NewRequest(http.MethodHead, "/readyz", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200 OK, got %d", res.StatusCode)
		}
		if contentType := res.Header.Get("Content-Type"); contentType != "application/json" {
			t.Errorf("expected Content-Type application/json, got %q", contentType)
		}
		body, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("failed to read body: %v", err)
		}
		if len(body) != 0 {
			t.Errorf("expected empty body for HEAD, got %q", string(body))
		}
	})

	t.Run("unavailable HEAD returns 503 with empty body", func(t *testing.T) {
		checker := &mockReadyChecker{err: errors.New("timeout")}
		router := api.NewRouter(nil, checker)

		req := httptest.NewRequest(http.MethodHead, "/readyz", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("expected status 503, got %d", res.StatusCode)
		}
		body, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("failed to read body: %v", err)
		}
		if len(body) != 0 {
			t.Errorf("expected empty body for HEAD, got %q", string(body))
		}
	})
}

func TestReadyz_MethodNotAllowed(t *testing.T) {
	router := api.NewRouter(nil, &mockReadyChecker{})

	disallowedMethods := []string{
		http.MethodPost,
		http.MethodPut,
		http.MethodDelete,
		http.MethodPatch,
	}

	for _, method := range disallowedMethods {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/readyz", nil)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			res := rec.Result()
			defer res.Body.Close()

			if res.StatusCode != http.StatusMethodNotAllowed {
				t.Errorf("expected status 405 Method Not Allowed for %s, got %d", method, res.StatusCode)
			}

			allow := res.Header.Get("Allow")
			if !strings.Contains(allow, http.MethodGet) || !strings.Contains(allow, http.MethodHead) {
				t.Errorf("expected Allow header containing GET and HEAD, got %q", allow)
			}
		})
	}
}

func TestHealthz_IndependentOfReadyz(t *testing.T) {
	// Database is failing, but healthz MUST still report 200 OK (liveness independent of DB readiness)
	failingChecker := &mockReadyChecker{err: errors.New("database down")}
	router := api.NewRouter(nil, failingChecker)

	// Check /readyz -> 503
	readyReq := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	readyRec := httptest.NewRecorder()
	router.ServeHTTP(readyRec, readyReq)
	if readyRec.Result().StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected readyz 503, got %d", readyRec.Result().StatusCode)
	}

	// Check /healthz -> 200 OK
	healthReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	healthRec := httptest.NewRecorder()
	router.ServeHTTP(healthRec, healthReq)
	if healthRec.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected healthz 200 OK even when DB down, got %d", healthRec.Result().StatusCode)
	}
}
