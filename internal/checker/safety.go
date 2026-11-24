package checker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var (
	ErrEmptyURL         = errors.New("url must not be empty")
	ErrInvalidURL       = errors.New("url must be a valid absolute URL")
	ErrInvalidScheme    = errors.New("url scheme must be http or https")
	ErrCredentialsInURL = errors.New("url must not contain embedded user credentials")
	ErrEmptyHost        = errors.New("url must contain a valid host")
	ErrInvalidHost      = errors.New("url contains an invalid host")
	ErrInvalidPort      = errors.New("url contains an invalid port")
	ErrLocalhostBlocked = errors.New("destination is a prohibited localhost/loopback name")
	ErrProhibitedHost   = errors.New("destination hostname is prohibited")
	ErrProhibitedIP     = errors.New("destination IP address is in a prohibited range")
	ErrDisallowedIP     = errors.New("DNS resolution returned a prohibited IP address")
	ErrMixedDNSAnswers  = errors.New("DNS resolution returned mixed public and prohibited IP addresses")
)

// Target represents a validated outbound HTTP/HTTPS destination.
type Target struct {
	rawURL     string
	parsedURL  *url.URL
	scheme     string
	host       string // Hostname or IP literal without brackets
	port       string // Port number string (e.g. "80", "443", "8080")
	hostHeader string // Preserved Host header (e.g. "example.com" or "example.com:8080")
}

// RawURL returns the original validated URL.
func (t *Target) RawURL() string {
	return t.rawURL
}

// String implements fmt.Stringer, returning the raw URL.
func (t *Target) String() string {
	return t.rawURL
}

// URL returns the parsed *url.URL structure.
func (t *Target) URL() *url.URL {
	return t.parsedURL
}

// Scheme returns the normalized lower-case URL scheme ("http" or "https").
func (t *Target) Scheme() string {
	return t.scheme
}

// Host returns the target host (hostname or IP literal without brackets).
func (t *Target) Host() string {
	return t.host
}

// Port returns the target port number as a string.
func (t *Target) Port() string {
	return t.port
}

// HostHeader returns the preserved host header to send in outbound requests.
func (t *Target) HostHeader() string {
	return t.hostHeader
}

// SanitizedURL returns the URL stripped of query parameters and fragments,
// safe for logging or error reporting without leaking query string parameters.
func (t *Target) SanitizedURL() string {
	if t.parsedURL == nil {
		return ""
	}
	u := *t.parsedURL
	u.RawQuery = ""
	u.Fragment = ""
	u.User = nil
	return u.String()
}

// Prohibited IP networks representing non-public, loopback, private, link-local,
// multicast, and reserved addresses.
//
// Security Note:
// Pulse implements application-level safeguards designed as BASIC SSRF PROTECTIONS
// appropriate for a local demo service. These mitigations reduce accidental access
// to private network infrastructure but DO NOT constitute production-grade or
// enterprise-grade isolation against sophisticated host/network attacks.
var prohibitedPrefixes = []struct {
	prefix netip.Prefix
	name   string
}{
	// IPv4
	{netip.MustParsePrefix("0.0.0.0/8"), "unspecified/current network"},
	{netip.MustParsePrefix("10.0.0.0/8"), "private RFC 1918"},
	{netip.MustParsePrefix("100.64.0.0/10"), "shared address space (CGNAT)"},
	{netip.MustParsePrefix("127.0.0.0/8"), "loopback"},
	{netip.MustParsePrefix("169.254.0.0/16"), "link-local"},
	{netip.MustParsePrefix("172.16.0.0/12"), "private RFC 1918"},
	{netip.MustParsePrefix("192.0.0.0/24"), "IETF protocol assignments"},
	{netip.MustParsePrefix("192.0.2.0/24"), "TEST-NET-1"},
	{netip.MustParsePrefix("192.168.0.0/16"), "private RFC 1918"},
	{netip.MustParsePrefix("198.18.0.0/15"), "benchmarking"},
	{netip.MustParsePrefix("198.51.100.0/24"), "TEST-NET-2"},
	{netip.MustParsePrefix("203.0.113.0/24"), "TEST-NET-3"},
	{netip.MustParsePrefix("224.0.0.0/4"), "multicast"},
	{netip.MustParsePrefix("240.0.0.0/4"), "reserved (Class E)"},
	{netip.MustParsePrefix("255.255.255.255/32"), "broadcast"},

	// IPv6
	{netip.MustParsePrefix("::/128"), "unspecified"},
	{netip.MustParsePrefix("::1/128"), "loopback"},
	{netip.MustParsePrefix("::ffff:0:0/96"), "IPv4-mapped"},
	{netip.MustParsePrefix("64:ff9b::/96"), "IPv4/IPv6 translation"},
	{netip.MustParsePrefix("100::/64"), "discard-only prefix"},
	{netip.MustParsePrefix("2001:db8::/32"), "documentation prefix"},
	{netip.MustParsePrefix("2001:10::/28"), "ORCHIDv1"},
	{netip.MustParsePrefix("2001:20::/28"), "ORCHIDv2"},
	{netip.MustParsePrefix("fc00::/7"), "unique local address (ULA)"},
	{netip.MustParsePrefix("fe80::/10"), "link-local unicast"},
	{netip.MustParsePrefix("ff00::/8"), "multicast"},
}

// IsProhibitedIP inspects an IP address and determines if it belongs to any
// loopback, private, link-local, multicast, unspecified, or non-public network range.
func IsProhibitedIP(addr netip.Addr) (bool, string) {
	if !addr.IsValid() {
		return true, "invalid address"
	}

	// Unmap IPv4-in-IPv6 to evaluate underlying IPv4 against prohibited ranges
	if addr.Is4In6() {
		addr = addr.Unmap()
	}

	if addr.IsLoopback() {
		return true, "loopback address"
	}
	if addr.IsPrivate() {
		return true, "private address"
	}
	if addr.IsLinkLocalUnicast() {
		return true, "link-local unicast address"
	}
	if addr.IsLinkLocalMulticast() {
		return true, "link-local multicast address"
	}
	if addr.IsMulticast() {
		return true, "multicast address"
	}
	if addr.IsUnspecified() {
		return true, "unspecified address"
	}

	for _, p := range prohibitedPrefixes {
		if p.prefix.Contains(addr) {
			return true, p.name
		}
	}

	if addr.Is4() && addr.As4() == [4]byte{255, 255, 255, 255} {
		return true, "broadcast address"
	}

	return false, ""
}

// isLocalhostName checks whether a hostname indicates a localhost or loopback name.
func isLocalhostName(host string) bool {
	lower := strings.ToLower(strings.TrimSpace(host))
	lower = strings.TrimSuffix(lower, ".")
	if lower == "localhost" ||
		strings.HasSuffix(lower, ".localhost") ||
		lower == "ip6-localhost" ||
		lower == "ip6-loopback" ||
		lower == "localhost.localdomain" {
		return true
	}
	return false
}

// isValidHostname checks RFC 1123 / RFC 952 hostname compliance.
func isValidHostname(host string) bool {
	if host == "" || len(host) > 253 {
		return false
	}
	h := strings.TrimSuffix(host, ".")
	if h == "" {
		return false
	}
	labels := strings.Split(h, ".")
	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 {
			return false
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-') {
				return false
			}
		}
	}
	return true
}

func defaultPortForScheme(scheme string) string {
	if scheme == "https" {
		return "443"
	}
	return "80"
}

// ValidateTarget validates an input target URL against protocol and basic SSRF rules.
// It accepts only http and https, requires valid hostname/IP and port syntax,
// rejects credentials and malformed URLs, and rejects clearly non-public IP literals and localhost.
func ValidateTarget(rawURL string) (*Target, error) {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return nil, ErrEmptyURL
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		if strings.Contains(err.Error(), "invalid port") {
			return nil, ErrInvalidPort
		}
		if strings.Contains(err.Error(), "missing ']' in host") {
			return nil, ErrInvalidHost
		}
		return nil, ErrInvalidURL
	}
	if !parsed.IsAbs() {
		return nil, ErrInvalidURL
	}

	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil, ErrInvalidScheme
	}

	if parsed.User != nil {
		return nil, ErrCredentialsInURL
	}

	if parsed.Host == "" {
		return nil, ErrEmptyHost
	}

	var host, port string
	rawHost := parsed.Host

	if strings.HasPrefix(rawHost, "[") {
		// Bracketed IPv6 literal, possibly with port: [::1] or [::1]:8080
		closeIdx := strings.LastIndex(rawHost, "]")
		if closeIdx == -1 {
			return nil, ErrInvalidHost
		}
		host = rawHost[1:closeIdx]
		after := rawHost[closeIdx+1:]
		if after == "" {
			port = defaultPortForScheme(scheme)
		} else if strings.HasPrefix(after, ":") {
			p := after[1:]
			pNum, err := strconv.Atoi(p)
			if err != nil || pNum < 1 || pNum > 65535 || p != strconv.Itoa(pNum) {
				return nil, ErrInvalidPort
			}
			port = p
		} else {
			return nil, ErrInvalidHost
		}
	} else if strings.Contains(rawHost, ":") {
		h, p, err := net.SplitHostPort(rawHost)
		if err != nil {
			return nil, ErrInvalidHost
		}
		pNum, err := strconv.Atoi(p)
		if err != nil || pNum < 1 || pNum > 65535 || p != strconv.Itoa(pNum) {
			return nil, ErrInvalidPort
		}
		host = h
		port = p
	} else {
		host = rawHost
		port = defaultPortForScheme(scheme)
	}

	if host == "" {
		return nil, ErrEmptyHost
	}

	// Reject localhost names
	if isLocalhostName(host) {
		return nil, ErrLocalhostBlocked
	}

	// Reject prohibited IP literals
	if addr, err := netip.ParseAddr(host); err == nil {
		if prohibited, reason := IsProhibitedIP(addr); prohibited {
			return nil, fmt.Errorf("%w: %s (%s)", ErrProhibitedIP, host, reason)
		}
	} else {
		// Host is a domain name; validate syntax
		if !isValidHostname(host) {
			return nil, ErrInvalidHost
		}
	}

	// Construct preserved Host header
	var hostHeader string
	if port == defaultPortForScheme(scheme) {
		if strings.Contains(host, ":") {
			hostHeader = "[" + host + "]"
		} else {
			hostHeader = host
		}
	} else {
		hostHeader = net.JoinHostPort(host, port)
	}

	return &Target{
		rawURL:     trimmed,
		parsedURL:  parsed,
		scheme:     scheme,
		host:       host,
		port:       port,
		hostHeader: hostHeader,
	}, nil
}

// ValidateURL is an alias for ValidateTarget.
func ValidateURL(rawURL string) (*Target, error) {
	return ValidateTarget(rawURL)
}

// Resolver defines the DNS resolution interface required by SafeDialer.
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// ContextDialer defines the socket connection interface.
type ContextDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// DialContextFunc is a functional adapter for ContextDialer.
type DialContextFunc func(ctx context.Context, network, address string) (net.Conn, error)

// DialContext invokes the underlying function.
func (f DialContextFunc) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return f(ctx, network, address)
}

// SafeDialer manages outbound connections with basic SSRF protections.
//
// Security Note:
// Pulse implements basic SSRF protections suitable for a local demo service.
// These mitigations reduce accidental connection to private or loopback services,
// but do not provide comprehensive or production-grade isolation.
type SafeDialer struct {
	resolver    Resolver
	dialer      ContextDialer
	dialTimeout time.Duration
}

// SafeDialerOption configures a SafeDialer instance.
type SafeDialerOption func(*SafeDialer)

// WithResolver configures a custom DNS resolver.
func WithResolver(r Resolver) SafeDialerOption {
	return func(d *SafeDialer) {
		d.resolver = r
	}
}

// WithContextDialer configures a custom socket dialer.
func WithContextDialer(cd ContextDialer) SafeDialerOption {
	return func(d *SafeDialer) {
		d.dialer = cd
	}
}

// WithDialTimeout configures the dial timeout duration.
func WithDialTimeout(t time.Duration) SafeDialerOption {
	return func(d *SafeDialer) {
		d.dialTimeout = t
	}
}

// NewSafeDialer creates a SafeDialer with standard defaults.
func NewSafeDialer(opts ...SafeDialerOption) *SafeDialer {
	sd := &SafeDialer{
		resolver:    net.DefaultResolver,
		dialTimeout: 10 * time.Second,
	}
	for _, opt := range opts {
		opt(sd)
	}
	if sd.dialer == nil {
		sd.dialer = &net.Dialer{
			Timeout:   sd.dialTimeout,
			KeepAlive: 30 * time.Second,
		}
	}
	return sd
}

// DialContext intercepts network connections to validate resolved destination addresses
// before establishing a socket connection.
func (d *SafeDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	dialCtx := ctx
	if d.dialTimeout > 0 {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, d.dialTimeout)
		defer cancel()
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid address %q: %w", addr, err)
	}

	// 1. If host is an IP literal
	if ipAddr, err := netip.ParseAddr(host); err == nil {
		if prohibited, reason := IsProhibitedIP(ipAddr); prohibited {
			return nil, fmt.Errorf("%w: prohibited destination IP %s (%s)", ErrProhibitedIP, host, reason)
		}
		return d.dialer.DialContext(dialCtx, network, net.JoinHostPort(ipAddr.String(), port))
	}

	// 2. Reject localhost names
	if isLocalhostName(host) {
		return nil, fmt.Errorf("%w: destination %q is a localhost name", ErrProhibitedHost, host)
	}

	// 3. Resolve hostnames before connecting
	addrs, err := d.resolver.LookupNetIP(dialCtx, "ip", host)
	if err != nil {
		return nil, fmt.Errorf("DNS resolution failed for %q: %w", host, err)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no IP addresses returned for host %q", host)
	}

	// 4. Validate EVERY resolved address.
	// Reject a hostname if its DNS response contains any disallowed address;
	// do not select only a public address from a mixed public/private response.
	for _, addr := range addrs {
		if prohibited, reason := IsProhibitedIP(addr); prohibited {
			return nil, fmt.Errorf("%w: DNS response for %q contained disallowed IP %s (%s)",
				ErrDisallowedIP, host, addr.String(), reason)
		}
	}

	// 5. Connect directly to the validated IP address.
	// This ensures DialContext connects to a validated IP rather than allowing
	// a second unvalidated DNS lookup.
	var lastErr error
	for _, addr := range addrs {
		targetAddr := net.JoinHostPort(addr.String(), port)
		conn, err := d.dialer.DialContext(dialCtx, network, targetAddr)
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}

	return nil, fmt.Errorf("failed to connect to validated IP addresses for %q: %w", host, lastErr)
}
