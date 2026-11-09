package egress

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"uuid"
)

// Client is the only network service given to a compiled Provider.
type Client interface {
	Do(*http.Request) (*http.Response, error)
}

type Intent struct {
	OperationID uuid.UUID `json:"operation_id"`
	Profile     string    `json:"profile"`
	PathID      string    `json:"path_id"`
	Origin      int       `json:"origin"`
	Method      string    `json:"method"`
	MaxBytes    int64     `json:"max_bytes"`
	TargetID    uuid.UUID `json:"target_id,omitzero"`
	Destination string    `json:"destination,omitempty"`
	RequestURI  string    `json:"request_uri,omitempty"`
	Previous    uuid.UUID `json:"previous_permit,omitzero"`
}

type Grant struct {
	ID        uuid.UUID `json:"id"`
	LeaseMS   int64     `json:"lease_ms"`
	ExpiresAt time.Time `json:"expires_at,omitzero"`
	RetryMS   int64     `json:"retry_ms,omitempty"`
}

type Usage struct {
	Outcome         string `json:"outcome"`
	Bytes           int64  `json:"bytes"`
	LatencyMS       int64  `json:"latency_ms"`
	ThrottleSeconds int32  `json:"throttle_seconds,omitempty"`
	RedirectDigest  string `json:"redirect_digest,omitempty"`
}

type Gate interface {
	Acquire(context.Context, Intent) (Grant, error)
	Finish(context.Context, uuid.UUID, Usage)
}

type Session struct {
	pool                  *Pool
	ctx                   context.Context
	cancel                context.CancelFunc
	plan                  Plan
	source                Source
	namespace, capability string
	gate                  Gate
	target                WebDestination
	client                http.Client
	mu                    sync.Mutex
	pinned                string
	deferred              time.Time
	calls                 sync.WaitGroup
	closed                bool
}

type WebDestination struct {
	ID     uuid.UUID
	Origin string
}

type responseContextKey struct{}
type responseContext struct {
	permit uuid.UUID
	path   Path
}

func ResponsePath(response *http.Response) Path {
	return responseRoute(response).path
}

func responseRoute(response *http.Response) responseContext {
	if response != nil && response.Request != nil {
		path, _ := response.Request.Context().Value(responseContextKey{}).(responseContext)
		return path
	}
	return responseContext{}
}

func (p *Pool) Session(ctx context.Context, plan Plan, source Source, namespace, capability string, gate Gate, target ...WebDestination) *Session {
	ctx, cancel := context.WithCancel(ctx)
	s := &Session{pool: p, ctx: ctx, cancel: cancel, plan: plan, source: source, namespace: namespace, capability: capability, gate: gate}
	if len(target) == 1 {
		s.target = target[0]
	}
	s.client = http.Client{
		Transport: s,
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			if s.source.FixedPath != "" {
				return http.ErrUseLastResponse
			}
			if len(via) >= 4 || s.source.Origin(r.URL) < 0 ||
				(via[len(via)-1].URL.Scheme == "https" && r.URL.Scheme != "https") {
				return ErrPolicy
			}
			if s.source.Origin(r.URL) != s.source.Origin(via[0].URL) || s.source.Origin(r.URL) != s.source.Origin(via[len(via)-1].URL) {
				// Go forwards some sensitive headers to subdomains by default.
				// Use an allowlist across any change of scheme or authority.
				for k := range r.Header {
					if !strings.EqualFold(k, "Accept") && !strings.EqualFold(k, "User-Agent") {
						delete(r.Header, k)
					}
				}
			}
			return nil
		},
	}
	return s
}

type scopedClient struct{ session *Session }

func (s *Session) Client() Client { return scopedClient{s} }

func (c scopedClient) Do(r *http.Request) (*http.Response, error) {
	if r == nil || r.URL == nil {
		return nil, ErrPolicy
	}
	s := c.session
	if s.source.WebMethod != "" {
		if !s.source.WebTarget(s.target.Origin) || !s.source.webURL(r.URL, s.target.Origin) ||
			r.Method != s.source.WebMethod || r.URL.RequestURI() != "/" || r.Response != nil {
			return nil, ErrPolicy
		}
		return s.webDo(r)
	}
	res, err := s.client.Do(r)
	if err == nil {
		return res, nil
	}
	return nil, safeError(err)
}

func safeError(err error) error {
	for _, safe := range []error{ErrPolicy, ErrBudget, ErrDeferred, ErrUnavailable, ErrResponse, ErrTLSName, ErrTLSExpired, ErrTLSUntrusted, ErrTLSVerify, ErrProxy, context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, safe) {
			return safe
		}
	}
	return ErrUnavailable
}

func (s *Session) Close() {
	s.mu.Lock()
	s.closed = true
	s.cancel()
	s.mu.Unlock()
	s.calls.Wait()
}

func (s *Session) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == "" {
		r = r.Clone(r.Context())
		r.Method = http.MethodGet
	}
	origin := s.source.Origin(r.URL)
	if s.source.WebMethod != "" && s.source.webURL(r.URL, s.target.Origin) && WebRequestURI(r.URL.RequestURI()) {
		origin = 0
	}
	if s.source.FixedPath != "" && (r.URL.EscapedPath() != s.source.FixedPath || r.Method != http.MethodGet) {
		origin = -1
	}
	if origin < 0 || (r.Method != http.MethodGet && r.Method != http.MethodHead) ||
		(r.Host != "" && r.Host != r.URL.Host) || (r.Body != nil && r.Body != http.NoBody) ||
		r.ContentLength != 0 || len(r.TransferEncoding) != 0 || len(r.Trailer) != 0 ||
		len(r.URL.RequestURI()) > 2048 || len(r.Header) > 32 {
		if r.Body != nil {
			r.Body.Close()
		}
		return nil, ErrPolicy
	}
	headerBytes := 0
	for k, values := range r.Header {
		if !allowedHeader(k) || (s.source.WebMethod != "" && !slices.Contains([]string{"accept", "accept-language", "user-agent"}, strings.ToLower(k))) {
			return nil, ErrPolicy
		}
		for _, value := range values {
			headerBytes += len(k) + len(value)
		}
	}
	if headerBytes > 8<<10 {
		return nil, ErrPolicy
	}

	s.mu.Lock()
	if s.closed || s.ctx.Err() != nil {
		s.mu.Unlock()
		return nil, context.Canceled
	}
	if time.Now().Before(s.deferred) {
		s.mu.Unlock()
		return nil, ErrDeferred
	}
	s.calls.Add(1)
	s.mu.Unlock()
	defer s.calls.Done()
	ctx, cancel := context.WithCancel(s.ctx)
	stop := context.AfterFunc(r.Context(), cancel)
	cleanup := func() { stop(); cancel() }
	if r.Context().Err() != nil {
		cleanup()
		return nil, context.Canceled
	}
	// Reserve the accepted body limit plus bounded application HTTP metadata.
	requestBytes := int64(headerBytes + len(r.URL.RequestURI()) + len(r.URL.Host) + 256)
	wireBudget := requestBytes + s.source.MaxBodyBytes + responseHeaderLimit
	var excluded []string
	for range 3 {
		s.mu.Lock()
		path, err := s.pool.reserve(s.plan, s.source, s.namespace, s.capability, s.pinned, excluded, wireBudget)
		if err == nil {
			s.pinned = path.path.ID
		}
		s.mu.Unlock()
		if err != nil {
			cleanup()
			return nil, err
		}
		res, err := s.send(ctx, r, path, origin, requestBytes, cleanup)
		if err == nil {
			return res, nil
		}
		if !s.source.Replay || !errors.Is(err, errConnect) || ctx.Err() != nil {
			cleanup()
			return nil, err
		}
		excluded = append(excluded, path.path.ID)
		s.mu.Lock()
		if s.pinned == path.path.ID {
			s.pinned = ""
		}
		s.mu.Unlock()
	}
	cleanup()
	return nil, ErrUnavailable
}

func (s *Session) send(ctx context.Context, r *http.Request, path reservation, origin int, requestBytes int64, cleanup func()) (*http.Response, error) {
	intent := Intent{
		OperationID: uuid.NewV7(), Profile: s.plan.Profile, PathID: path.path.ID, Origin: origin,
		Method: r.Method, MaxBytes: s.source.MaxBodyBytes,
	}
	if s.source.WebMethod != "" {
		intent.TargetID, intent.Destination, intent.RequestURI = s.target.ID, s.target.Origin, r.URL.RequestURI()
		intent.Previous = responseRoute(r.Response).permit
		if intent.Previous == (uuid.UUID{}) && intent.RequestURI != "/" {
			s.pool.release(path, 0, nil, false)
			return nil, ErrPolicy
		}
	}
	grant, start, err := s.acquire(ctx, intent)
	if err != nil || grant.RetryMS != 0 {
		s.pool.release(path, 0, nil, false)
		if grant.RetryMS > 0 {
			s.mu.Lock()
			s.deferred = time.Now().Add(time.Duration(grant.RetryMS) * time.Millisecond)
			s.mu.Unlock()
			return nil, ErrDeferred
		}
		return nil, err
	}
	// Keep TTL on the Worker's monotonic clock, measured from before the
	// permit RPC, so response latency and clock skew cannot extend authority.
	deadline := start.Add(time.Duration(grant.LeaseMS)*time.Millisecond - 100*time.Millisecond)
	wire, cancel := context.WithDeadline(ctx, deadline)
	var metadataBytes int64
	finish := func(u Usage, networkErr error, reached bool) {
		cancel()
		u.LatencyMS = min(MaxPermitTime.Milliseconds(), time.Since(start).Milliseconds())
		s.pool.release(path, min(path.maxBytes, metadataBytes+u.Bytes), networkErr, reached)
		s.gate.Finish(context.WithoutCancel(ctx), grant.ID, u)
	}
	if wire.Err() != nil {
		finish(Usage{Outcome: "transport"}, nil, false)
		return nil, ErrUnavailable
	}
	metadataBytes = requestBytes
	request := r.Clone(wire)
	// HTTP/1's automatic replay requires either an absent body or GetBody.
	// This EOF-only body is omitted from the wire for GET/HEAD, but prevents
	// both stale-connection retry branches. Each retry needs a fresh permit.
	request.Body = io.NopCloser(strings.NewReader(""))
	request.GetBody = nil
	res, err := path.transport.RoundTrip(request)
	if err != nil {
		wireErr := wire.Err()
		finish(Usage{Outcome: "transport"}, err, false)
		if wireErr := ctx.Err(); wireErr != nil {
			return nil, wireErr
		}
		if s.source.WebMethod != "" {
			var verify *tls.CertificateVerificationError
			if errors.As(err, &verify) {
				var invalid x509.CertificateInvalidError
				var hostname x509.HostnameError
				var authority x509.UnknownAuthorityError
				switch {
				case errors.As(verify.Err, &hostname):
					return nil, ErrTLSName
				case errors.As(verify.Err, &invalid) && invalid.Reason == x509.Expired:
					return nil, ErrTLSExpired
				case errors.As(verify.Err, &authority):
					return nil, ErrTLSUntrusted
				default:
					return nil, ErrTLSVerify
				}
			}
			if errors.Is(err, ErrProxy) {
				return nil, ErrProxy
			}
		}
		if s.source.WebMethod != "" || s.source.FixedPath != "" {
			var timeout net.Error
			if errors.Is(wireErr, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout()) {
				return nil, context.DeadlineExceeded
			}
		}
		if errors.Is(err, errConnect) || errors.Is(err, ErrPolicy) {
			return nil, err
		}
		return nil, ErrUnavailable
	}
	metadataBytes += int64(len(res.Status) + 16)
	for key, values := range res.Header {
		for _, value := range values {
			metadataBytes += int64(len(key) + len(value) + 4)
		}
	}
	outcome := classify(res.StatusCode)
	res.Request = r.WithContext(context.WithValue(r.Context(), responseContextKey{}, responseContext{grant.ID, path.path.Path()}))
	var redirect string
	if s.source.WebMethod != "" && redirectStatus(res.StatusCode) {
		if location, err := res.Location(); err == nil && s.webRedirect(location) {
			redirect = RedirectDigest(location.RequestURI())
		}
	}
	if outcome == "throttled" || outcome == "denied" {
		_ = res.Body.Close()
		usage := Usage{Outcome: outcome}
		if outcome == "throttled" {
			usage.ThrottleSeconds = retryAfterSeconds(res.Header.Get("Retry-After"))
		}
		finish(usage, nil, true)
		cleanup()
		res.Body, res.ContentLength = http.NoBody, 0
		return res, nil
	}
	kind, _, _ := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if s.source.WebMethod == "" && (res.StatusCode < 200 || res.ContentLength > s.source.MaxBodyBytes ||
		(res.StatusCode >= 200 && res.StatusCode < 300 && res.StatusCode != 204 && r.Method != http.MethodHead &&
			!slices.Contains(s.source.ContentTypes, kind)) || res.Header.Get("Content-Encoding") != "") {
		res.Body.Close()
		if outcome == "success" {
			outcome = "policy"
		}
		finish(Usage{Outcome: outcome}, nil, true)
		if s.source.FixedPath != "" {
			return nil, ErrResponse
		}
		return nil, ErrPolicy
	}
	s.calls.Add(1)
	body := &responseBody{
		inner: res.Body, limit: s.source.MaxBodyBytes, status: outcome,
		finish: func(usage Usage) {
			defer s.calls.Done()
			defer cleanup()
			if usage.Outcome == "success" {
				usage.RedirectDigest = redirect
			}
			finish(usage, nil, true)
		},
	}
	context.AfterFunc(wire, body.timeout)
	res.Body = body
	// The returned response must not expose the private transport request.
	return res, nil
}

func (s *Session) acquire(ctx context.Context, intent Intent) (Grant, time.Time, error) {
	waitUntil := time.Now().Add(MaxPermitTime + time.Second)
	for {
		start := time.Now()
		grant, err := s.gate.Acquire(ctx, intent)
		if err != nil || grant.RetryMS <= 0 {
			return grant, start, err
		}
		delay := max(250*time.Millisecond, time.Duration(grant.RetryMS)*time.Millisecond)
		if time.Now().Add(delay).After(waitUntil) {
			return grant, start, nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Grant{}, start, ctx.Err()
		case <-timer.C:
		}
	}
}

func retryAfterSeconds(value string) int32 {
	if n, err := strconv.ParseUint(value, 10, 32); err == nil {
		return int32(min(n, 300))
	}
	if t, err := http.ParseTime(value); err == nil {
		return int32(min(max(0, time.Until(t).Seconds()), 300))
	}
	return 0
}

func classify(status int) string {
	switch {
	case status == 429:
		return "throttled"
	case status == 401 || status == 403:
		return "denied"
	case status >= 200 && status < 400:
		return "success"
	default:
		return "upstream"
	}
}

type responseBody struct {
	inner    io.ReadCloser
	limit    int64
	status   string
	readMu   sync.Mutex
	read     atomic.Int64
	once     sync.Once
	closeErr error
	finish   func(Usage)
}

func (b *responseBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	b.readMu.Lock()
	defer b.readMu.Unlock()
	left := b.limit - b.read.Load()
	n, err := b.inner.Read(p[:min(int64(len(p)), left+1)])
	if int64(n) > left {
		b.read.Add(left)
		b.done("policy")
		return int(left), ErrBudget
	}
	b.read.Add(int64(n))
	if err == io.EOF {
		b.done(b.status)
	} else if err != nil {
		b.timeout()
		err = ErrUnavailable
	}
	return n, err
}

func (b *responseBody) timeout() {
	status := b.status
	if status == "success" {
		status = "transport"
	}
	b.done(status)
}

func (b *responseBody) Close() error { return b.done(b.status) }

func (b *responseBody) done(status string) error {
	b.once.Do(func() {
		if b.inner.Close() != nil {
			b.closeErr = ErrUnavailable
		}
		b.finish(Usage{Outcome: status, Bytes: b.read.Load()})
	})
	return b.closeErr
}
