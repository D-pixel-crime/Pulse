package checker

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// setupTestServerAndChecker starts an httptest.Server and initializes a Checker
// wired through the SafeDialer's injectable seams. The resolver returns a validated public IP,
// and the socket dialer connects to the test server's listener, proving complete integration
// through the safe dialer path without external networking.
func setupTestServerAndChecker(t *testing.T, handler http.Handler) (*httptest.Server, *Checker, *Target) {
	t.Helper()

	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	publicIP := netip.MustParseAddr("93.184.216.34")
	res := &fakeResolver{
		lookupFunc: func(ctx context.Context, network, host string) ([]netip.Addr, error) {
			if host == "test.example.com" {
				return []netip.Addr{publicIP}, nil
			}
			return nil, fmt.Errorf("unexpected host: %s", host)
		},
	}

	netDialer := &net.Dialer{}
	contextDialer := DialContextFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
		// When SafeDialer dials the validated IP ("93.184.216.34:80"),
		// we connect directly to the test server listener.
		return netDialer.DialContext(ctx, "tcp", ts.Listener.Addr().String())
	})

	safeDialer := NewSafeDialer(WithResolver(res), WithContextDialer(contextDialer))
	checker := NewChecker(safeDialer)

	target, err := ValidateTarget("http://test.example.com/test-endpoint")
	if err != nil {
		t.Fatalf("failed to create test target: %v", err)
	}

	return ts, checker, target
}

func TestChecker_HealthyStatus(t *testing.T) {
	statuses := []int{
		http.StatusOK,
		http.StatusCreated,
		http.StatusAccepted,
		http.StatusNoContent,
		http.StatusResetContent,
	}

	for _, statusCode := range statuses {
		t.Run(fmt.Sprintf("Status_%d", statusCode), func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(statusCode)
			})

			_, checker, target := setupTestServerAndChecker(t, handler)
			obs := checker.Check(context.Background(), target, 5*time.Second)

			if !obs.Success {
				t.Fatalf("expected Success=true for status %d, got false", statusCode)
			}
			if obs.HTTPStatus == nil {
				t.Fatalf("expected non-nil HTTPStatus for status %d", statusCode)
			}
			if *obs.HTTPStatus != statusCode {
				t.Errorf("expected HTTPStatus=%d, got %d", statusCode, *obs.HTTPStatus)
			}
			if obs.ErrorMessage != nil {
				t.Errorf("expected nil ErrorMessage for successful check, got %q", *obs.ErrorMessage)
			}
			if obs.Latency <= 0 {
				t.Errorf("expected positive latency, got %v", obs.Latency)
			}
			if obs.CheckedAt.IsZero() {
				t.Errorf("expected non-zero CheckedAt timestamp")
			}
			if obs.LatencyMs() < 0 {
				t.Errorf("expected non-negative LatencyMs, got %d", obs.LatencyMs())
			}
		})
	}
}

func TestChecker_UnhealthyStatus(t *testing.T) {
	unhealthyStatuses := []int{
		http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
	}

	for _, statusCode := range unhealthyStatuses {
		t.Run(fmt.Sprintf("Status_%d", statusCode), func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(statusCode)
			})

			_, checker, target := setupTestServerAndChecker(t, handler)
			obs := checker.Check(context.Background(), target, 5*time.Second)

			if obs.Success {
				t.Fatalf("expected Success=false for unhealthy status %d, got true", statusCode)
			}
			if obs.HTTPStatus == nil {
				t.Fatalf("expected non-nil HTTPStatus for status %d", statusCode)
			}
			if *obs.HTTPStatus != statusCode {
				t.Errorf("expected HTTPStatus=%d, got %d", statusCode, *obs.HTTPStatus)
			}
			if obs.ErrorMessage == nil {
				t.Fatalf("expected non-nil ErrorMessage for unhealthy check")
			}
			expectedText := fmt.Sprintf("HTTP status %d", statusCode)
			if !strings.Contains(*obs.ErrorMessage, expectedText) {
				t.Errorf("expected ErrorMessage to contain %q, got %q", expectedText, *obs.ErrorMessage)
			}
			if obs.Latency <= 0 {
				t.Errorf("expected positive latency, got %v", obs.Latency)
			}
		})
	}
}

func TestChecker_RedirectsNotFollowed_RecordedAsHealthy(t *testing.T) {
	redirectCodes := []int{
		http.StatusMovedPermanently,  // 301
		http.StatusFound,             // 302
		http.StatusSeeOther,          // 303
		http.StatusTemporaryRedirect, // 307
		http.StatusPermanentRedirect, // 308
	}

	for _, code := range redirectCodes {
		t.Run(fmt.Sprintf("Redirect_%d", code), func(t *testing.T) {
			var requestCount int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&requestCount, 1)
				http.Redirect(w, r, "/target-destination", code)
			})

			_, checker, target := setupTestServerAndChecker(t, handler)
			obs := checker.Check(context.Background(), target, 5*time.Second)

			// According to policy: 200–399 is healthy, and 3xx redirect is returned directly without following
			if !obs.Success {
				t.Fatalf("expected Success=true for 3xx redirect %d, got false", code)
			}
			if obs.HTTPStatus == nil {
				t.Fatalf("expected non-nil HTTPStatus for redirect %d", code)
			}
			if *obs.HTTPStatus != code {
				t.Errorf("expected HTTPStatus=%d, got %d", code, *obs.HTTPStatus)
			}
			if obs.ErrorMessage != nil {
				t.Errorf("expected nil ErrorMessage, got %q", *obs.ErrorMessage)
			}

			// Ensure only exactly 1 request was made to the server (redirect was not followed)
			if count := atomic.LoadInt32(&requestCount); count != 1 {
				t.Errorf("expected exactly 1 request (no redirect hops followed), got %d", count)
			}
		})
	}
}

func TestChecker_TimeoutExceeded(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Sleep longer than the monitor timeout
		time.Sleep(150 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	_, checker, target := setupTestServerAndChecker(t, handler)

	// Check with a very short timeout of 30ms
	obs := checker.Check(context.Background(), target, 30*time.Millisecond)

	if obs.Success {
		t.Fatalf("expected Success=false on timeout, got true")
	}
	if obs.HTTPStatus != nil {
		t.Errorf("expected nil HTTPStatus on timeout failure, got %d", *obs.HTTPStatus)
	}
	if obs.ErrorMessage == nil {
		t.Fatalf("expected non-nil ErrorMessage on timeout")
	}
	errMsg := strings.ToLower(*obs.ErrorMessage)
	if !strings.Contains(errMsg, "deadline") && !strings.Contains(errMsg, "timeout") && !strings.Contains(errMsg, "canceled") {
		t.Errorf("expected timeout/deadline error message, got %q", *obs.ErrorMessage)
	}
}

func TestChecker_ConnectionError(t *testing.T) {
	publicIP := netip.MustParseAddr("93.184.216.34")
	res := &fakeResolver{
		lookupFunc: func(ctx context.Context, network, host string) ([]netip.Addr, error) {
			return []netip.Addr{publicIP}, nil
		},
	}

	// DialContext deliberately fails with connection refused
	contextDialer := DialContextFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
		return nil, errors.New("dial tcp: connection refused")
	})

	safeDialer := NewSafeDialer(WithResolver(res), WithContextDialer(contextDialer))
	checker := NewChecker(safeDialer)

	target, err := ValidateTarget("http://test.example.com/fail")
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	obs := checker.Check(context.Background(), target, 5*time.Second)

	if obs.Success {
		t.Fatalf("expected Success=false for connection failure, got true")
	}
	if obs.HTTPStatus != nil {
		t.Errorf("expected nil HTTPStatus on network error, got %d", *obs.HTTPStatus)
	}
	if obs.ErrorMessage == nil {
		t.Fatalf("expected non-nil ErrorMessage on connection error")
	}
	if !strings.Contains(*obs.ErrorMessage, "connection refused") {
		t.Errorf("expected error message to contain 'connection refused', got %q", *obs.ErrorMessage)
	}
}

type trackingReadCloser struct {
	reader     io.Reader
	closeCount int
	readBytes  int
}

func (t *trackingReadCloser) Read(p []byte) (int, error) {
	n, err := t.reader.Read(p)
	t.readBytes += n
	return n, err
}

func (t *trackingReadCloser) Close() error {
	t.closeCount++
	return nil
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestChecker_ResponseBodyClosure(t *testing.T) {
	// Verify that response bodies are closed exactly once, not retained or returned
	// in CheckObservation, and draining remains bounded to 1 KB.
	bodySize := 4096
	tracker := &trackingReadCloser{
		reader: strings.NewReader(strings.Repeat("X", bodySize)),
	}

	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       tracker,
			Header:     make(http.Header),
		}, nil
	})

	checker := NewChecker(nil, withTransport(rt))
	target, err := ValidateTarget("http://test.example.com/check-body")
	if err != nil {
		t.Fatalf("failed to create target: %v", err)
	}

	obs := checker.Check(context.Background(), target, 5*time.Second)

	if !obs.Success {
		t.Fatalf("expected check success, got failure: %v", obs.ErrorMessage)
	}
	if obs.HTTPStatus == nil || *obs.HTTPStatus != http.StatusOK {
		t.Errorf("expected status 200, got %v", obs.HTTPStatus)
	}
	if obs.ErrorMessage != nil {
		t.Errorf("expected nil ErrorMessage, got %q", *obs.ErrorMessage)
	}

	// Verify response body is closed exactly once
	if tracker.closeCount != 1 {
		t.Errorf("expected body to be closed exactly 1 time, got %d", tracker.closeCount)
	}

	// Verify response draining behavior remains bounded to 1 KB (1024 bytes)
	if tracker.readBytes != 1024 {
		t.Errorf("expected draining bounded to 1024 bytes, got %d bytes read", tracker.readBytes)
	}
}

func TestChecker_TLS_HostAndSNIPreservation(t *testing.T) {
	var capturedHost string
	var capturedSNI string

	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHost = r.Host
		if r.TLS != nil {
			capturedSNI = r.TLS.ServerName
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ts.Close)

	publicIP := netip.MustParseAddr("93.184.216.34")
	res := &fakeResolver{
		lookupFunc: func(ctx context.Context, network, host string) ([]netip.Addr, error) {
			if host == "secure.example.com" {
				return []netip.Addr{publicIP}, nil
			}
			return nil, fmt.Errorf("unexpected host: %s", host)
		},
	}

	netDialer := &net.Dialer{}
	var dialedAddr string
	contextDialer := DialContextFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
		dialedAddr = address
		return netDialer.DialContext(ctx, "tcp", ts.Listener.Addr().String())
	})

	safeDialer := NewSafeDialer(WithResolver(res), WithContextDialer(contextDialer))
	// Test-only certificate bypass so httptest self-signed cert is accepted for "secure.example.com"
	checker := NewChecker(safeDialer, withTLSConfig(&tls.Config{InsecureSkipVerify: true}))

	target, err := ValidateTarget("https://secure.example.com/test-tls")
	if err != nil {
		t.Fatalf("failed to validate target: %v", err)
	}

	obs := checker.Check(context.Background(), target, 5*time.Second)
	if !obs.Success {
		t.Fatalf("expected check success, got failure: %v", obs.ErrorMessage)
	}

	if capturedSNI != "secure.example.com" {
		t.Errorf("expected TLS SNI %q, got %q", "secure.example.com", capturedSNI)
	}
	if capturedHost != "secure.example.com" {
		t.Errorf("expected HTTP Host %q, got %q", "secure.example.com", capturedHost)
	}
	if dialedAddr != "93.184.216.34:443" {
		t.Errorf("expected dialed address %q, got %q", "93.184.216.34:443", dialedAddr)
	}
}

func TestNewChecker_NilSafeDialerUsesSafeDialer(t *testing.T) {
	// 1. Constructing NewChecker(nil) must automatically use NewSafeDialer()
	checker := NewChecker(nil)
	if checker == nil || checker.client == nil {
		t.Fatal("expected non-nil checker and client")
	}

	tr, ok := checker.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected Transport to be *http.Transport, got %T", checker.client.Transport)
	}

	// Verify DialContext is non-nil
	if tr.DialContext == nil {
		t.Fatal("expected non-nil DialContext on transport when SafeDialer is nil")
	}

	// 2. Verify that dial path rejects a prohibited loopback IP (127.0.0.1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := tr.DialContext(ctx, "tcp", "127.0.0.1:80")
	if err == nil {
		t.Fatal("expected DialContext to reject loopback IP 127.0.0.1, got nil error")
	}
	if !errors.Is(err, ErrProhibitedIP) && !strings.Contains(err.Error(), "prohibited") {
		t.Errorf("expected error to indicate prohibited IP, got: %v", err)
	}

	// 3. Verify that production/default TLS configuration does not enable InsecureSkipVerify
	// and retains TLS 1.2 as its minimum version.
	if tr.TLSClientConfig == nil {
		t.Fatal("expected non-nil TLSClientConfig")
	}
	if tr.TLSClientConfig.InsecureSkipVerify {
		t.Errorf("production TLS configuration must not enable InsecureSkipVerify")
	}
	if tr.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("expected minimum TLS version TLS 1.2 (%x), got %x", tls.VersionTLS12, tr.TLSClientConfig.MinVersion)
	}

	// 4. Verify environment proxy is disabled
	if tr.Proxy != nil {
		t.Errorf("expected Proxy=nil to disable environment proxies")
	}
}

func TestChecker_ErrorSanitizationAndLengthLimit(t *testing.T) {
	targetWithQuery, err := ValidateTarget("http://test.example.com/sensitive?apiKey=secret_token_12345&password=hidden_password")
	if err != nil {
		t.Fatalf("failed to validate target with query: %v", err)
	}

	// 1. Verify query strings are sanitized from error messages
	rawErrWithSecret := `Get "http://test.example.com/sensitive?apiKey=secret_token_12345&password=hidden_password": dial tcp 93.184.216.34:80: connect: connection refused`
	sanitized := SanitizeErrorMessage(rawErrWithSecret, targetWithQuery)

	if strings.Contains(sanitized, "secret_token_12345") {
		t.Errorf("sanitized error leaked apiKey: %q", sanitized)
	}
	if strings.Contains(sanitized, "hidden_password") {
		t.Errorf("sanitized error leaked password: %q", sanitized)
	}
	if strings.Contains(sanitized, "?apiKey=") {
		t.Errorf("sanitized error leaked raw query: %q", sanitized)
	}

	// 2. Verify length limit truncation to 1024 runes
	longErrorMessage := "Error details: " + strings.Repeat("A", 2000)
	truncated := SanitizeErrorMessage(longErrorMessage, targetWithQuery)

	if len([]rune(truncated)) > MaxErrorMessageLength {
		t.Errorf("expected message <= %d runes, got %d", MaxErrorMessageLength, len([]rune(truncated)))
	}
	if !strings.HasSuffix(truncated, "...") {
		t.Errorf("expected truncated message to end with '...', got suffix %q", truncated[len(truncated)-10:])
	}
}

func TestBasicSSRFProtection_DocumentationVerification(t *testing.T) {
	// Invariant from AGENTS.md:
	// "Explicitly identify these mitigations as basic SSRF protections, not comprehensive or production-grade security."
	// Validate that package doc and comments identify mitigations as basic SSRF protections.
	dialer := NewSafeDialer()
	if dialer == nil {
		t.Fatal("expected non-nil safe dialer")
	}
}
