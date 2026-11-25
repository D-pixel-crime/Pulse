package checker

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// MaxErrorMessageLength defines the maximum character length for persisted check error messages.
const MaxErrorMessageLength = 1024

// CheckObservation captures the raw observation of an executed HTTP health check.
type CheckObservation struct {
	CheckedAt    time.Time     `json:"checked_at"`
	CompletedAt  time.Time     `json:"completed_at"`
	Success      bool          `json:"success"`
	HTTPStatus   *int          `json:"http_status"`
	Latency      time.Duration `json:"latency"`
	ErrorMessage *string       `json:"error_message"`
}

// LatencyMs returns the observed latency converted to milliseconds.
func (o CheckObservation) LatencyMs() int64 {
	return o.Latency.Milliseconds()
}

// Checker executes outbound HTTP health checks using a safe dialer.
//
// Security Note:
// Pulse implements application-level safeguards designed as BASIC SSRF PROTECTIONS
// appropriate for a local demo service. These mitigations reduce accidental access
// to private network infrastructure but DO NOT constitute production-grade or
// enterprise-grade isolation against sophisticated host/network attacks.
type Checker struct {
	client *http.Client
}

// checkerOption configures optional Checker behavior for tests.
type checkerOption func(*checkerConfig)

type checkerConfig struct {
	tlsConfig *tls.Config
	transport http.RoundTripper
}

// withTLSConfig sets a custom TLS configuration on the checker's transport (test-only).
func withTLSConfig(cfg *tls.Config) checkerOption {
	return func(c *checkerConfig) {
		c.tlsConfig = cfg
	}
}

// withTransport sets a custom http.RoundTripper on the checker's HTTP client (test-only).
func withTransport(rt http.RoundTripper) checkerOption {
	return func(c *checkerConfig) {
		c.transport = rt
	}
}

// NewChecker initializes an HTTP health checker configured with bounded timeouts,
// safe dialer integration, disabled environment proxies, and disabled redirect following.
func NewChecker(dialer *SafeDialer, opts ...checkerOption) *Checker {
	cfg := &checkerConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	var clientTransport http.RoundTripper
	if cfg.transport != nil {
		clientTransport = cfg.transport
	} else {
		if dialer == nil {
			dialer = NewSafeDialer()
		}

		var tlsClientConfig *tls.Config
		if cfg.tlsConfig != nil {
			tlsClientConfig = cfg.tlsConfig.Clone()
		} else {
			tlsClientConfig = &tls.Config{
				MinVersion: tls.VersionTLS12,
			}
		}

		clientTransport = &http.Transport{
			DialContext:           dialer.DialContext,
			Proxy:                 nil, // Explicitly disable environment proxy use (HTTP_PROXY, HTTPS_PROXY)
			TLSClientConfig:       tlsClientConfig,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 10 * time.Second,
			DisableKeepAlives:     false,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
		}
	}

	client := &http.Client{
		Transport: clientTransport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// Do not follow redirects: record the returned 3xx response and apply policy
			return http.ErrUseLastResponse
		},
	}

	return &Checker{
		client: client,
	}
}

var (
	queryParamRegex  = regexp.MustCompile(`\?[^"'\s\)]+`)
	credentialsRegex = regexp.MustCompile(`://([^@\s]+)@`)
)

// SanitizeErrorMessage strips query strings and user credentials from error strings
// and truncates the message to MaxErrorMessageLength runes.
func SanitizeErrorMessage(rawMsg string, target *Target) string {
	msg := rawMsg

	// Strip exact raw URL if present, replacing with sanitized URL
	if target != nil {
		if rawURL := target.RawURL(); rawURL != "" && strings.Contains(msg, rawURL) {
			msg = strings.ReplaceAll(msg, rawURL, target.SanitizedURL())
		}
		if u := target.URL(); u != nil && u.RawQuery != "" {
			msg = strings.ReplaceAll(msg, "?"+u.RawQuery, "")
		}
	}

	// Redact any remaining query parameters
	msg = queryParamRegex.ReplaceAllString(msg, "")

	// Redact any credentials embedded in URLs inside error string
	msg = credentialsRegex.ReplaceAllString(msg, "://[REDACTED]@")

	msg = strings.TrimSpace(msg)

	// Length-limit to MaxErrorMessageLength runes
	if utf8.RuneCountInString(msg) > MaxErrorMessageLength {
		runes := []rune(msg)
		return string(runes[:MaxErrorMessageLength-3]) + "..."
	}

	return msg
}

// Check performs an HTTP GET request against the validated target with a per-monitor timeout,
// evaluating the response status code and returning a CheckObservation.
func (c *Checker) Check(ctx context.Context, target *Target, timeout time.Duration) CheckObservation {
	if target == nil {
		now := time.Now().UTC()
		errMsg := "check target must not be nil"
		return CheckObservation{
			CheckedAt:    now,
			CompletedAt:  now,
			Success:      false,
			HTTPStatus:   nil,
			Latency:      0,
			ErrorMessage: &errMsg,
		}
	}

	checkCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	checkedAt := start.UTC()

	req, err := http.NewRequestWithContext(checkCtx, http.MethodGet, target.RawURL(), nil)
	if err != nil {
		latency := time.Since(start)
		completedAt := checkedAt.Add(latency)
		errMsg := SanitizeErrorMessage(err.Error(), target)
		return CheckObservation{
			CheckedAt:    checkedAt,
			CompletedAt:  completedAt,
			Success:      false,
			HTTPStatus:   nil,
			Latency:      latency,
			ErrorMessage: &errMsg,
		}
	}

	// Preserve original hostname for HTTP Host header and TLS SNI
	req.Host = target.HostHeader()
	req.Header.Set("User-Agent", "Pulse-Monitor/1.0")

	resp, err := c.client.Do(req)
	latency := time.Since(start)
	completedAt := checkedAt.Add(latency)

	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		errMsg := SanitizeErrorMessage(err.Error(), target)
		return CheckObservation{
			CheckedAt:    checkedAt,
			CompletedAt:  completedAt,
			Success:      false,
			HTTPStatus:   nil,
			Latency:      latency,
			ErrorMessage: &errMsg,
		}
	}

	// Drain up to 1KB to allow TCP connection reuse, but discard body bytes immediately
	if resp.Body != nil {
		defer resp.Body.Close()
		_, _ = io.CopyN(io.Discard, resp.Body, 1024)
	}

	statusCode := resp.StatusCode

	// HTTP 200–399: Healthy
	if statusCode >= 200 && statusCode <= 399 {
		return CheckObservation{
			CheckedAt:    checkedAt,
			CompletedAt:  completedAt,
			Success:      true,
			HTTPStatus:   &statusCode,
			Latency:      latency,
			ErrorMessage: nil,
		}
	}

	// HTTP 400–599: Unhealthy
	statusText := http.StatusText(statusCode)
	var errMsg string
	if statusText != "" {
		errMsg = fmt.Sprintf("HTTP status %d %s", statusCode, statusText)
	} else {
		errMsg = fmt.Sprintf("HTTP status %d", statusCode)
	}
	sanitized := SanitizeErrorMessage(errMsg, target)

	return CheckObservation{
		CheckedAt:    checkedAt,
		CompletedAt:  completedAt,
		Success:      false,
		HTTPStatus:   &statusCode,
		Latency:      latency,
		ErrorMessage: &sanitized,
	}
}
