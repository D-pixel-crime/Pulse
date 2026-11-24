package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"pulse/internal/api"
	"pulse/internal/config"
)

func TestHealthz_Success(t *testing.T) {
	router := api.NewRouter(nil)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
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

	var healthRes api.HealthResponse
	if err := json.NewDecoder(res.Body).Decode(&healthRes); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if healthRes.Status != "ok" {
		t.Errorf("expected status 'ok', got %q", healthRes.Status)
	}
}

func TestHealthz_Head(t *testing.T) {
	router := api.NewRouter(nil)

	req := httptest.NewRequest(http.MethodHead, "/healthz", nil)
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

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	if len(body) != 0 {
		t.Errorf("expected empty response body for HEAD request, got %q (len %d)", string(body), len(body))
	}
}

func TestHealthzHandler_Direct(t *testing.T) {
	handler := api.HealthzHandler()

	t.Run("GET returns 200 with JSON body", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusOK {
			t.Errorf("expected status 200 OK, got %d", res.StatusCode)
		}
		if contentType := res.Header.Get("Content-Type"); contentType != "application/json" {
			t.Errorf("expected Content-Type application/json, got %q", contentType)
		}
		var health api.HealthResponse
		if err := json.NewDecoder(res.Body).Decode(&health); err != nil {
			t.Fatalf("failed to decode JSON response: %v", err)
		}
		if health.Status != "ok" {
			t.Errorf("expected status 'ok', got %q", health.Status)
		}
	})

	t.Run("HEAD returns 200 with matching headers and empty body", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodHead, "/healthz", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusOK {
			t.Errorf("expected status 200 OK, got %d", res.StatusCode)
		}
		if contentType := res.Header.Get("Content-Type"); contentType != "application/json" {
			t.Errorf("expected Content-Type application/json, got %q", contentType)
		}
		body, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("failed to read response body: %v", err)
		}
		if len(body) != 0 {
			t.Errorf("expected empty response body, got %q", string(body))
		}
	})

	t.Run("Unsupported method returns 405 with Allow header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/healthz", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		res := rec.Result()
		defer res.Body.Close()

		if res.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("expected status 405 Method Not Allowed, got %d", res.StatusCode)
		}
		allow := res.Header.Get("Allow")
		if !strings.Contains(allow, "GET") || !strings.Contains(allow, "HEAD") {
			t.Errorf("expected Allow header containing GET and HEAD, got %q", allow)
		}
	})
}

func TestHealthz_MethodNotAllowed(t *testing.T) {
	router := api.NewRouter(nil)

	disallowedMethods := []string{
		http.MethodPost,
		http.MethodPut,
		http.MethodDelete,
		http.MethodPatch,
	}

	for _, method := range disallowedMethods {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/healthz", nil)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			res := rec.Result()
			defer res.Body.Close()

			if res.StatusCode != http.StatusMethodNotAllowed {
				t.Errorf("expected status 405 Method Not Allowed for %s, got %d", method, res.StatusCode)
			}

			allow := res.Header.Get("Allow")
			if !strings.Contains(allow, http.MethodGet) || !strings.Contains(allow, http.MethodHead) {
				t.Errorf("expected Allow header to contain GET and HEAD, got %q", allow)
			}

			contentType := res.Header.Get("Content-Type")
			if contentType != "application/json" {
				t.Errorf("expected Content-Type application/json, got %q", contentType)
			}

			var errRes api.ErrorResponse
			if err := json.NewDecoder(res.Body).Decode(&errRes); err != nil {
				t.Fatalf("failed to decode error body: %v", err)
			}
			if errRes.Error != "method not allowed" {
				t.Errorf("expected error 'method not allowed', got %q", errRes.Error)
			}
		})
	}
}

func TestRouter_UnknownRoutes(t *testing.T) {
	router := api.NewRouter(nil)

	unknownPaths := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/unknown"},
		{http.MethodPost, "/unknown"},
		{http.MethodGet, "/api/v1/monitors"},
		{http.MethodPost, "/api/v1/monitors"},
		{http.MethodGet, "/healthz/nested"},
		{http.MethodGet, "/"},
	}

	for _, tc := range unknownPaths {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			res := rec.Result()
			defer res.Body.Close()

			if res.StatusCode != http.StatusNotFound {
				t.Errorf("expected status 404 Not Found for %s %s, got %d", tc.method, tc.path, res.StatusCode)
			}

			contentType := res.Header.Get("Content-Type")
			if contentType != "application/json" {
				t.Errorf("expected Content-Type application/json, got %q", contentType)
			}

			var errRes api.ErrorResponse
			if err := json.NewDecoder(res.Body).Decode(&errRes); err != nil {
				t.Fatalf("failed to decode error body: %v", err)
			}
			if errRes.Error != "not found" {
				t.Errorf("expected error 'not found', got %q", errRes.Error)
			}
		})
	}
}

func TestServer_ConfigAndTimeouts(t *testing.T) {
	logger := slog.Default()

	t.Run("default configuration", func(t *testing.T) {
		srv := api.NewServer("127.0.0.1:8080", logger)

		if srv.Addr() != "127.0.0.1:8080" {
			t.Errorf("expected Addr '127.0.0.1:8080', got %q", srv.Addr())
		}
		if srv.ShutdownTimeout() != api.DefaultShutdownTimeout {
			t.Errorf("expected ShutdownTimeout %v, got %v", api.DefaultShutdownTimeout, srv.ShutdownTimeout())
		}

		httpSrv := srv.HTTPServer()
		if httpSrv.ReadHeaderTimeout != api.DefaultReadHeaderTimeout {
			t.Errorf("expected ReadHeaderTimeout %v, got %v", api.DefaultReadHeaderTimeout, httpSrv.ReadHeaderTimeout)
		}
		if httpSrv.ReadTimeout != api.DefaultReadTimeout {
			t.Errorf("expected ReadTimeout %v, got %v", api.DefaultReadTimeout, httpSrv.ReadTimeout)
		}
		if httpSrv.WriteTimeout != api.DefaultWriteTimeout {
			t.Errorf("expected WriteTimeout %v, got %v", api.DefaultWriteTimeout, httpSrv.WriteTimeout)
		}
		if httpSrv.IdleTimeout != api.DefaultIdleTimeout {
			t.Errorf("expected IdleTimeout %v, got %v", api.DefaultIdleTimeout, httpSrv.IdleTimeout)
		}
		if httpSrv.MaxHeaderBytes != api.DefaultMaxHeaderBytes {
			t.Errorf("expected MaxHeaderBytes %d, got %d", api.DefaultMaxHeaderBytes, httpSrv.MaxHeaderBytes)
		}
		if srv.Handler() == nil {
			t.Fatal("expected non-nil Handler")
		}
	})

	t.Run("from app config", func(t *testing.T) {
		cfg := config.NewDefault()
		cfg.AppAddr = "127.0.0.1:9090"
		srv := api.New(cfg, logger)

		if srv.Addr() != "127.0.0.1:9090" {
			t.Errorf("expected Addr '127.0.0.1:9090', got %q", srv.Addr())
		}
	})

	t.Run("with custom options", func(t *testing.T) {
		srv := api.NewServer("127.0.0.1:8080", logger,
			api.WithShutdownTimeout(10*time.Second),
			api.WithTimeouts(3*time.Second, 6*time.Second, 12*time.Second, 45*time.Second, 15*time.Second),
		)

		if srv.ShutdownTimeout() != 15*time.Second {
			t.Errorf("expected ShutdownTimeout 15s, got %v", srv.ShutdownTimeout())
		}
		httpSrv := srv.HTTPServer()
		if httpSrv.ReadHeaderTimeout != 3*time.Second {
			t.Errorf("expected ReadHeaderTimeout 3s, got %v", httpSrv.ReadHeaderTimeout)
		}
		if httpSrv.ReadTimeout != 6*time.Second {
			t.Errorf("expected ReadTimeout 6s, got %v", httpSrv.ReadTimeout)
		}
		if httpSrv.WriteTimeout != 12*time.Second {
			t.Errorf("expected WriteTimeout 12s, got %v", httpSrv.WriteTimeout)
		}
		if httpSrv.IdleTimeout != 45*time.Second {
			t.Errorf("expected IdleTimeout 45s, got %v", httpSrv.IdleTimeout)
		}
	})
}

func TestServer_GracefulShutdown_ContextCancellation(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer ln.Close()

	srv := api.NewServer(ln.Addr().String(), slog.Default(), api.WithListener(ln))

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- srv.Run(ctx)
	}()

	// Verify server is accepting connections and answering GET /healthz
	baseURL := "http://" + ln.Addr().String()
	client := &http.Client{Timeout: 2 * time.Second}

	var resp *http.Response
	for i := 0; i < 50; i++ {
		resp, err = client.Get(baseURL + "/healthz")
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("failed to make request to server: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	var health api.HealthResponse
	if err := json.Unmarshal(body, &health); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}
	if health.Status != "ok" {
		t.Errorf("expected status 'ok', got %q", health.Status)
	}

	// Trigger graceful shutdown via context cancellation (no OS signal needed)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected graceful shutdown without error, got: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server to shut down")
	}

	// Verify that server is stopped and rejects new connections
	_, err = client.Get(baseURL + "/healthz")
	if err == nil {
		t.Fatal("expected request after shutdown to fail, but it succeeded")
	}
}

func TestServer_GracefulShutdown_DrainActiveRequests(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer ln.Close()

	requestReceived := make(chan struct{})
	requestCanProceed := make(chan struct{})

	slowHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestReceived)
		<-requestCanProceed
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"drained"}`))
	})

	srv := api.NewServer(ln.Addr().String(), slog.Default(),
		api.WithListener(ln),
		api.WithHandler(slowHandler),
		api.WithShutdownTimeout(3*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- srv.Run(ctx)
	}()

	baseURL := "http://" + ln.Addr().String()
	client := &http.Client{Timeout: 5 * time.Second}

	var reqErr error
	var reqResp *http.Response
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		reqResp, reqErr = client.Get(baseURL + "/slow")
	}()

	// Wait until handler is actively processing the request
	<-requestReceived

	// Cancel context to initiate graceful shutdown while request is in-flight
	cancel()

	// Allow the slow request to complete processing
	close(requestCanProceed)

	// Wait for request to finish
	wg.Wait()

	if reqErr != nil {
		t.Fatalf("expected in-flight request to complete during shutdown, got error: %v", reqErr)
	}
	defer reqResp.Body.Close()

	if reqResp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 for drained request, got %d", reqResp.StatusCode)
	}

	// Server should exit gracefully with nil error
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected graceful server shutdown, got: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server shutdown")
	}
}

func TestServer_GracefulShutdown_TimeoutExceeded(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer ln.Close()

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	handlerFinished := make(chan struct{})

	blockingHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerFinished)
		close(handlerStarted)
		<-releaseHandler
		w.WriteHeader(http.StatusOK)
	})

	// Bounded shutdown deadline set to 50ms
	srv := api.NewServer(ln.Addr().String(), slog.Default(),
		api.WithListener(ln),
		api.WithHandler(blockingHandler),
		api.WithShutdownTimeout(50*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	serverErrCh := make(chan error, 1)

	go func() {
		serverErrCh <- srv.Run(ctx)
	}()

	baseURL := "http://" + ln.Addr().String()
	client := &http.Client{Timeout: 5 * time.Second}
	clientErrCh := make(chan error, 1)

	go func() {
		resp, reqErr := client.Get(baseURL + "/blocking")
		if reqErr == nil {
			resp.Body.Close()
		}
		clientErrCh <- reqErr
	}()

	// 1. Verify a real request reaches the deliberately blocked HTTP handler
	select {
	case <-handlerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for request to reach handler")
	}

	// 2. Context cancellation initiates graceful shutdown
	cancel()

	// 3. Verify short shutdown deadline is exceeded and Server.Run returns error satisfying errors.Is(err, context.DeadlineExceeded)
	select {
	case serverErr := <-serverErrCh:
		if serverErr == nil {
			t.Fatal("expected error when shutdown deadline is exceeded, got nil")
		}
		if !errors.Is(serverErr, context.DeadlineExceeded) {
			t.Fatalf("expected errors.Is(serverErr, context.DeadlineExceeded), got: %v", serverErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Server.Run to fail with deadline exceeded")
	}

	// 4. Verify active client connection is forcibly closed so request cannot complete successfully
	select {
	case clientErr := <-clientErrCh:
		if clientErr == nil {
			t.Fatal("expected client request to fail due to forcible connection close, but it succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for client request to fail")
	}

	// 5. Release blocked handler and wait for its goroutine to finish, avoiding test goroutine leaks
	close(releaseHandler)
	select {
	case <-handlerFinished:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for handler goroutine to finish")
	}
}

func TestServer_ListenError(t *testing.T) {
	// Attempt to bind to an invalid host:port combination
	srv := api.NewServer("999.999.999.999:99999", slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := srv.Run(ctx)
	if err == nil {
		t.Fatal("expected listen error for invalid address, got nil")
	}
}
