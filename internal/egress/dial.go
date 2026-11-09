package egress

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/proxy"
)

var (
	errConnect      = errors.New("network_connect_failed")
	errProxyRefused = errors.Join(ErrPolicy, errors.New("proxy_handshake_refused"))
)

type lookupIP func(context.Context, string, string) ([]netip.Addr, error)

func checkedAddress(ctx context.Context, lookup lookupIP, address string, fixture bool) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", ErrPolicy
	}
	if _, err := netip.ParseAddr(host); err != nil {
		host += "." // Avoid search-suffix DNS resolution.
	}
	addresses, err := lookup(ctx, "ip", host)
	if err != nil || len(addresses) == 0 {
		return "", ErrUnavailable
	}
	// Reject mixed public/private answers and hand only a numeric destination
	// to the dialer or proxy. Neither can re-resolve the checked source name.
	for _, ip := range addresses {
		if !allowedIP(ip, fixture) {
			return "", ErrPolicy
		}
	}
	return net.JoinHostPort(addresses[0].Unmap().String(), port), nil
}

// FixtureNetwork is injected by the test registry, never from YAML. Address
// validation still runs before its dial function or any proxy tunnel.
type FixtureNetwork struct {
	Lookup     func(context.Context, string, string) ([]netip.Addr, error)
	DialDirect func(context.Context, string, string) (net.Conn, error)
	RootCAs    *x509.CertPool
}

func connect(ctx context.Context, destination string, endpoint *url.URL, fixture bool, options *FixtureNetwork) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	dialer := &net.Dialer{Timeout: 2 * time.Second, KeepAlive: 20 * time.Second}
	lookup, dialDirect := net.DefaultResolver.LookupNetIP, dialer.DialContext
	var roots *x509.CertPool
	if options != nil {
		roots = options.RootCAs
		if options.Lookup != nil {
			lookup = options.Lookup
		}
		if options.DialDirect != nil {
			dialDirect = options.DialDirect
		}
	}
	address, err := checkedAddress(ctx, lookup, destination, fixture)
	if err != nil {
		return nil, err
	}
	if endpoint == nil {
		conn, err := dialDirect(ctx, "tcp", address)
		if err != nil {
			return nil, errConnect
		}
		return conn, nil
	}
	if endpoint.Scheme == "socks5" {
		var auth *proxy.Auth
		if endpoint.User != nil {
			password, _ := endpoint.User.Password()
			auth = &proxy.Auth{User: endpoint.User.Username(), Password: password}
		}
		dial, err := proxy.SOCKS5("tcp", endpoint.Host, auth, dialer)
		if err != nil {
			return nil, ErrPolicy
		}
		conn, err := dial.(proxy.ContextDialer).DialContext(ctx, "tcp", address)
		if err != nil {
			// SOCKS replies can deny access. Rotate only a failed proxy TCP
			// dial or an incomplete handshake, never a reported policy reply.
			var op *net.OpError
			if errors.As(err, &op) && (isDialError(op) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || op.Timeout()) {
				return nil, errors.Join(ErrProxy, errConnect)
			}
			return nil, errors.Join(ErrProxy, errProxyRefused)
		}
		return conn, nil
	}
	conn, err := connectTunnel(ctx, dialer, endpoint, address, roots)
	if err != nil {
		return nil, errors.Join(ErrProxy, err)
	}
	return conn, nil
}

func isDialError(err error) bool {
	for err != nil {
		if op, ok := err.(*net.OpError); ok && op.Op == "dial" {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

func dialProxy(ctx context.Context, dialer *net.Dialer, endpoint *url.URL, roots *x509.CertPool) (net.Conn, error) {
	if endpoint.Scheme == "https" {
		d := tls.Dialer{NetDialer: dialer, Config: &tls.Config{
			ServerName: endpoint.Hostname(), MinVersion: tls.VersionTLS12, RootCAs: roots,
		}}
		return d.DialContext(ctx, "tcp", endpoint.Host)
	}
	return dialer.DialContext(ctx, "tcp", endpoint.Host)
}

type bufferedConn struct {
	net.Conn
	reader io.Reader
}

func (c bufferedConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

func connectTunnel(ctx context.Context, dialer *net.Dialer, endpoint *url.URL, address string, roots *x509.CertPool) (net.Conn, error) {
	conn, err := dialProxy(ctx, dialer, endpoint, roots)
	if err != nil {
		return nil, errConnect
	}
	ok := false
	defer func() {
		if !ok {
			conn.Close()
		}
	}()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	if conn.SetDeadline(deadline) != nil {
		return nil, errConnect
	}
	req := &http.Request{Method: http.MethodConnect, URL: &url.URL{Host: address}, Host: address, Header: make(http.Header)}
	if endpoint.User != nil {
		password, _ := endpoint.User.Password()
		req.SetBasicAuth(endpoint.User.Username(), password)
		req.Header.Set("Proxy-Authorization", req.Header.Get("Authorization"))
		req.Header.Del("Authorization")
	}
	if req.Write(conn) != nil {
		return nil, errConnect
	}
	reader := bufio.NewReader(io.LimitReader(conn, 16<<10))
	res, err := http.ReadResponse(reader, req)
	if err != nil {
		return nil, errConnect
	}
	if res.StatusCode != http.StatusOK {
		switch res.StatusCode {
		case 502, 503, 504:
			return nil, errConnect
		default:
			return nil, errProxyRefused
		}
	}
	buffer, err := reader.Peek(reader.Buffered())
	if err != nil || !stop() || conn.SetDeadline(time.Time{}) != nil {
		return nil, errConnect
	}
	ok = true
	if len(buffer) == 0 {
		return conn, nil
	}
	return bufferedConn{conn, io.MultiReader(bytes.NewReader(buffer), conn)}, nil
}

const responseHeaderLimit = 16 << 10

func routeTransport(endpoint *url.URL, fixture bool, options ...*FixtureNetwork) *http.Transport {
	var network *FixtureNetwork
	var roots *x509.CertPool
	if len(options) != 0 {
		network = options[0]
		if network != nil {
			roots = network.RootCAs
		}
	}
	// HTTP/1 permits a single accounted request without implicit HTTP/2
	// stream replay. The guarded transport also disables HTTP/1 replay.
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	return &http.Transport{
		Protocols: protocols,
		DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
			return connect(ctx, address, endpoint, fixture, network)
		},
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots},
		MaxIdleConns:    4, MaxIdleConnsPerHost: 2, MaxConnsPerHost: 16,
		IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: 2 * time.Second,
		ResponseHeaderTimeout: 3 * time.Second, MaxResponseHeaderBytes: responseHeaderLimit,
		DisableCompression: true,
	}
}

func allowedHeader(k string) bool {
	switch strings.ToLower(k) {
	case "connection", "upgrade", "proxy-authorization", "proxy-connection", "transfer-encoding",
		"te", "trailer", "expect", "content-length", "host", "accept-encoding":
		return false
	}
	return true
}
