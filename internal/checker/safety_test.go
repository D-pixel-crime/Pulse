package checker

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"
)

// fakeResolver implements the Resolver interface using a mock function.
type fakeResolver struct {
	lookupFunc func(ctx context.Context, network, host string) ([]netip.Addr, error)
}

func (f *fakeResolver) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return f.lookupFunc(ctx, network, host)
}

// fakeConn satisfies net.Conn for testing dialer outcomes.
type fakeConn struct {
	net.Conn
	closed bool
}

func (c *fakeConn) Close() error {
	c.closed = true
	return nil
}

func (c *fakeConn) Read(b []byte) (n int, err error) {
	return 0, net.ErrClosed
}

func (c *fakeConn) Write(b []byte) (n int, err error) {
	return len(b), nil
}

func (c *fakeConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 12345}
}

func (c *fakeConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("93.184.216.34"), Port: 80}
}

func (c *fakeConn) SetDeadline(t time.Time) error      { return nil }
func (c *fakeConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *fakeConn) SetWriteDeadline(t time.Time) error { return nil }

func TestValidateTarget_MalformedURLs(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		wantErr error
	}{
		{"empty string", "", ErrEmptyURL},
		{"whitespace string", "   ", ErrEmptyURL},
		{"missing scheme", "example.com/test", ErrInvalidURL},
		{"missing host", "http://", ErrEmptyHost},
		{"invalid port non-numeric", "http://example.com:abc", ErrInvalidPort},
		{"invalid port zero", "http://example.com:0", ErrInvalidPort},
		{"invalid port too large", "http://example.com:65536", ErrInvalidPort},
		{"invalid port empty with colon", "http://example.com:", ErrInvalidPort},
		{"invalid port negative", "http://example.com:-1", ErrInvalidPort},
		{"invalid IPv6 port", "http://[2001:db8::1]:99999", ErrInvalidPort},
		{"unclosed IPv6 bracket", "http://[2001:db8::1", ErrInvalidHost},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ValidateTarget(tc.rawURL)
			if err == nil {
				t.Fatalf("expected error for %q, got nil", tc.rawURL)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Errorf("expected error %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestValidateTarget_UnsupportedSchemes(t *testing.T) {
	unsupported := []string{
		"ftp://example.com/file.txt",
		"file:///etc/passwd",
		"gopher://example.com",
		"ws://example.com/socket",
		"wss://example.com/socket",
		"javascript:alert(1)",
		"data:text/plain;base64,SGVsbG8=",
		"mailto:user@example.com",
		"ssh://git@github.com",
	}

	for _, raw := range unsupported {
		t.Run(raw, func(t *testing.T) {
			_, err := ValidateTarget(raw)
			if !errors.Is(err, ErrInvalidScheme) && !errors.Is(err, ErrInvalidURL) {
				t.Fatalf("expected ErrInvalidScheme or ErrInvalidURL for %q, got %v", raw, err)
			}
		})
	}
}

func TestValidateTarget_CredentialsProhibited(t *testing.T) {
	credentialURLs := []string{
		"http://user:password@example.com",
		"https://admin:secret@api.example.com/health",
		"http://user@example.com",
		"http://:password@example.com",
	}

	for _, raw := range credentialURLs {
		t.Run(raw, func(t *testing.T) {
			_, err := ValidateTarget(raw)
			if !errors.Is(err, ErrCredentialsInURL) {
				t.Fatalf("expected ErrCredentialsInURL for %q, got %v", raw, err)
			}
		})
	}
}

func TestValidateTarget_LocalhostBlocked(t *testing.T) {
	localhostURLs := []string{
		"http://localhost",
		"http://localhost:8080",
		"https://localhost/path",
		"http://localhost.",
		"http://sub.localhost",
		"http://api.sub.localhost:3000",
		"http://localhost.localdomain",
		"http://ip6-localhost",
		"http://ip6-loopback",
	}

	for _, raw := range localhostURLs {
		t.Run(raw, func(t *testing.T) {
			_, err := ValidateTarget(raw)
			if !errors.Is(err, ErrLocalhostBlocked) {
				t.Fatalf("expected ErrLocalhostBlocked for %q, got %v", raw, err)
			}
		})
	}
}

func TestValidateTarget_ProhibitedIPLiterals(t *testing.T) {
	prohibited := []struct {
		url  string
		desc string
	}{
		// IPv4 loopback
		{"http://127.0.0.1", "IPv4 loopback"},
		{"http://127.0.0.2:8080", "IPv4 loopback with port"},
		{"http://127.1.2.3", "IPv4 loopback range"},

		// RFC 1918 Private
		{"http://10.0.0.1", "IPv4 10/8"},
		{"http://10.254.1.1:8080", "IPv4 10/8"},
		{"http://172.16.0.1", "IPv4 172.16/12"},
		{"http://172.31.255.254", "IPv4 172.16/12 upper"},
		{"http://192.168.1.1", "IPv4 192.168/16"},
		{"http://192.168.100.50:8000", "IPv4 192.168/16"},

		// Link-Local
		{"http://169.254.169.254", "IPv4 link-local (cloud metadata)"},
		{"http://169.254.1.1", "IPv4 link-local"},

		// Unspecified / Broadcast / CGNAT
		{"http://0.0.0.0", "IPv4 unspecified"},
		{"http://100.64.0.1", "IPv4 CGNAT"},
		{"http://255.255.255.255", "IPv4 broadcast"},

		// IPv6 loopback and private
		{"http://[::1]", "IPv6 loopback"},
		{"http://[::1]:8080", "IPv6 loopback with port"},
		{"http://[::]", "IPv6 unspecified"},
		{"http://[fc00::1]", "IPv6 ULA"},
		{"http://[fd12:3456:789a::1]", "IPv6 ULA"},
		{"http://[fe80::1]", "IPv6 link-local"},

		// IPv4-mapped IPv6 loopback and private
		{"http://[::ffff:127.0.0.1]", "IPv4-mapped loopback"},
		{"http://[::ffff:10.0.0.1]", "IPv4-mapped private"},
		{"http://[::ffff:192.168.1.1]", "IPv4-mapped private"},
	}

	for _, tc := range prohibited {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := ValidateTarget(tc.url)
			if !errors.Is(err, ErrProhibitedIP) {
				t.Fatalf("expected ErrProhibitedIP for %s (%q), got %v", tc.desc, tc.url, err)
			}
		})
	}
}

func TestValidateTarget_ValidPublicURLs(t *testing.T) {
	valid := []struct {
		raw            string
		expectedScheme string
		expectedHost   string
		expectedPort   string
		expectedHeader string
	}{
		{
			raw:            "http://example.com",
			expectedScheme: "http",
			expectedHost:   "example.com",
			expectedPort:   "80",
			expectedHeader: "example.com",
		},
		{
			raw:            "https://api.example.com/health",
			expectedScheme: "https",
			expectedHost:   "api.example.com",
			expectedPort:   "443",
			expectedHeader: "api.example.com",
		},
		{
			raw:            "https://example.com:8443/status",
			expectedScheme: "https",
			expectedHost:   "example.com",
			expectedPort:   "8443",
			expectedHeader: "example.com:8443",
		},
		{
			raw:            "http://93.184.216.34:8080/probe",
			expectedScheme: "http",
			expectedHost:   "93.184.216.34",
			expectedPort:   "8080",
			expectedHeader: "93.184.216.34:8080",
		},
	}

	for _, tc := range valid {
		t.Run(tc.raw, func(t *testing.T) {
			target, err := ValidateTarget(tc.raw)
			if err != nil {
				t.Fatalf("unexpected error validating %q: %v", tc.raw, err)
			}
			if target.Scheme() != tc.expectedScheme {
				t.Errorf("scheme mismatch: got %q, want %q", target.Scheme(), tc.expectedScheme)
			}
			if target.Host() != tc.expectedHost {
				t.Errorf("host mismatch: got %q, want %q", target.Host(), tc.expectedHost)
			}
			if target.Port() != tc.expectedPort {
				t.Errorf("port mismatch: got %q, want %q", target.Port(), tc.expectedPort)
			}
			if target.HostHeader() != tc.expectedHeader {
				t.Errorf("hostHeader mismatch: got %q, want %q", target.HostHeader(), tc.expectedHeader)
			}
		})
	}
}

func TestSafeDialer_DNSResolution_ProhibitedIPs(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		resolvedIPs []netip.Addr
		wantErr     error
	}{
		{
			name:        "resolves to 127.0.0.1",
			resolvedIPs: []netip.Addr{netip.MustParseAddr("127.0.0.1")},
			wantErr:     ErrDisallowedIP,
		},
		{
			name:        "resolves to ::1",
			resolvedIPs: []netip.Addr{netip.MustParseAddr("::1")},
			wantErr:     ErrDisallowedIP,
		},
		{
			name:        "resolves to RFC 1918 10.0.0.5",
			resolvedIPs: []netip.Addr{netip.MustParseAddr("10.0.0.5")},
			wantErr:     ErrDisallowedIP,
		},
		{
			name:        "resolves to cloud metadata 169.254.169.254",
			resolvedIPs: []netip.Addr{netip.MustParseAddr("169.254.169.254")},
			wantErr:     ErrDisallowedIP,
		},
		{
			name:        "resolves to IPv6 ULA fc00::1",
			resolvedIPs: []netip.Addr{netip.MustParseAddr("fc00::1")},
			wantErr:     ErrDisallowedIP,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := &fakeResolver{
				lookupFunc: func(ctx context.Context, network, host string) ([]netip.Addr, error) {
					return tc.resolvedIPs, nil
				},
			}

			dialer := NewSafeDialer(WithResolver(res))
			_, err := dialer.DialContext(ctx, "tcp", "target.example.com:80")
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestSafeDialer_MixedDNSAnswers_Rejected(t *testing.T) {
	ctx := context.Background()

	// Mixed answer sets: contains both a legitimate public address and a prohibited address.
	// Safety rule: Reject the whole answer set; never select only the public address.
	mixedSets := []struct {
		name string
		ips  []netip.Addr
	}{
		{
			name: "public and loopback",
			ips: []netip.Addr{
				netip.MustParseAddr("93.184.216.34"),
				netip.MustParseAddr("127.0.0.1"),
			},
		},
		{
			name: "public first then 10.0.0.1",
			ips: []netip.Addr{
				netip.MustParseAddr("93.184.216.34"),
				netip.MustParseAddr("10.0.0.1"),
			},
		},
		{
			name: "private first then public",
			ips: []netip.Addr{
				netip.MustParseAddr("192.168.1.1"),
				netip.MustParseAddr("93.184.216.34"),
			},
		},
		{
			name: "public IPv4 with IPv6 loopback",
			ips: []netip.Addr{
				netip.MustParseAddr("93.184.216.34"),
				netip.MustParseAddr("::1"),
			},
		},
	}

	for _, tc := range mixedSets {
		t.Run(tc.name, func(t *testing.T) {
			dialCalled := false
			res := &fakeResolver{
				lookupFunc: func(ctx context.Context, network, host string) ([]netip.Addr, error) {
					return tc.ips, nil
				},
			}
			contextDialer := DialContextFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
				dialCalled = true
				return &fakeConn{}, nil
			})

			dialer := NewSafeDialer(WithResolver(res), WithContextDialer(contextDialer))
			_, err := dialer.DialContext(ctx, "tcp", "mixed.example.com:80")
			if !errors.Is(err, ErrDisallowedIP) {
				t.Fatalf("expected ErrDisallowedIP for mixed DNS response, got: %v", err)
			}
			if dialCalled {
				t.Fatalf("dialer must not be called when DNS response contains disallowed address")
			}
		})
	}
}

func TestSafeDialer_DirectDialToValidatedIP(t *testing.T) {
	ctx := context.Background()

	// Test that the safe dialer intercepts hostname resolution and passes
	// the validated IP address directly to the underlying dialer, preventing
	// a second unvalidated DNS lookup.
	publicIP := netip.MustParseAddr("93.184.216.34")
	res := &fakeResolver{
		lookupFunc: func(ctx context.Context, network, host string) ([]netip.Addr, error) {
			if host == "safe.example.com" {
				return []netip.Addr{publicIP}, nil
			}
			return nil, errors.New("unexpected host")
		},
	}

	var dialedNetwork, dialedAddress string
	contextDialer := DialContextFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
		dialedNetwork = network
		dialedAddress = address
		return &fakeConn{}, nil
	})

	dialer := NewSafeDialer(WithResolver(res), WithContextDialer(contextDialer))
	conn, err := dialer.DialContext(ctx, "tcp", "safe.example.com:80")
	if err != nil {
		t.Fatalf("unexpected dial error: %v", err)
	}
	defer conn.Close()

	if dialedNetwork != "tcp" {
		t.Errorf("expected network 'tcp', got %q", dialedNetwork)
	}
	expectedAddr := "93.184.216.34:80"
	if dialedAddress != expectedAddr {
		t.Errorf("dial path did not connect directly to validated IP: got %q, want %q",
			dialedAddress, expectedAddr)
	}
	// Verify dialedAddress does NOT contain the hostname
	if strings.Contains(dialedAddress, "safe.example.com") {
		t.Errorf("dialed address contains raw hostname instead of validated IP")
	}
}

func TestSafeDialer_DialTimeout_BlockedResolver(t *testing.T) {
	res := &fakeResolver{
		lookupFunc: func(ctx context.Context, network, host string) ([]netip.Addr, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	dialTimeout := 50 * time.Millisecond
	dialer := NewSafeDialer(
		WithResolver(res),
		WithDialTimeout(dialTimeout),
	)

	start := time.Now()
	_, err := dialer.DialContext(context.Background(), "tcp", "example.com:80")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from blocked resolver, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("expected context deadline exceeded error, got: %v", err)
	}
	if elapsed < dialTimeout {
		t.Errorf("dialer returned before timeout: elapsed %v < %v", elapsed, dialTimeout)
	}
	if elapsed > 1*time.Second {
		t.Errorf("dialer took too long to time out: elapsed %v", elapsed)
	}
}

func TestSafeDialer_DialTimeout_BlockedDialer(t *testing.T) {
	publicIP := netip.MustParseAddr("93.184.216.34")
	res := &fakeResolver{
		lookupFunc: func(ctx context.Context, network, host string) ([]netip.Addr, error) {
			return []netip.Addr{publicIP}, nil
		},
	}

	contextDialer := DialContextFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})

	dialTimeout := 50 * time.Millisecond
	dialer := NewSafeDialer(
		WithResolver(res),
		WithContextDialer(contextDialer),
		WithDialTimeout(dialTimeout),
	)

	start := time.Now()
	_, err := dialer.DialContext(context.Background(), "tcp", "example.com:80")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from blocked dialer, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("expected context deadline exceeded error, got: %v", err)
	}
	if elapsed < dialTimeout {
		t.Errorf("dialer returned before timeout: elapsed %v < %v", elapsed, dialTimeout)
	}
	if elapsed > 1*time.Second {
		t.Errorf("dialer took too long to time out: elapsed %v", elapsed)
	}
}

func TestSafeDialer_DefaultDialerInitializedWithOptions(t *testing.T) {
	timeout := 3 * time.Second
	sd := NewSafeDialer(WithDialTimeout(timeout))
	nd, ok := sd.dialer.(*net.Dialer)
	if !ok {
		t.Fatalf("expected default dialer to be *net.Dialer, got %T", sd.dialer)
	}
	if nd.Timeout != timeout {
		t.Errorf("expected net.Dialer.Timeout=%v, got %v", timeout, nd.Timeout)
	}
}

func TestValidateTarget_HostnameValidation(t *testing.T) {
	validHostnames := []string{
		"http://example.com",
		"https://sub-domain.example.com",
		"http://a-b-c.123-456.com",
		"https://api.v1.service.org",
		"http://my-host-123.net",
		"http://foo-bar.baz.qux.co.uk",
	}

	for _, raw := range validHostnames {
		t.Run("Valid_"+raw, func(t *testing.T) {
			target, err := ValidateTarget(raw)
			if err != nil {
				t.Fatalf("expected valid hostname for %q, got error: %v", raw, err)
			}
			if target == nil {
				t.Fatalf("expected non-nil target for %q", raw)
			}
		})
	}

	underscoreHostnames := []string{
		"http://sub_domain.example.com",
		"http://_service.example.com",
		"http://example_domain.com",
		"http://my_host:8080",
		"https://host_name.org/path",
		"http://trailing_.example.com",
	}

	for _, raw := range underscoreHostnames {
		t.Run("Underscore_"+raw, func(t *testing.T) {
			_, err := ValidateTarget(raw)
			if err == nil {
				t.Fatalf("expected error for underscore hostname %q, got nil", raw)
			}
			if !errors.Is(err, ErrInvalidHost) {
				t.Errorf("expected ErrInvalidHost for %q, got: %v", raw, err)
			}
		})
	}
}
