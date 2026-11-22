package provider

import (
	"bytes"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"io"
	"net/http"
	"time"

	steam "github.com/gofurry/steam-go"

	"github.com/librecosta/harvest/internal/egress"
)

// steamTransport borrows an Attempt's network client. Its method set deliberately
// omits CloseIdleConnections: the Worker owns its shared transport pool.
type steamTransport struct {
	client   egress.Client
	maxBytes int64
	store    *map[string]steamStorePresence
	validate func([]byte) bool
	evidence *SteamEvidence
	status   *int
}

func (t steamTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	res, err := t.client.Do(r)
	if res != nil && t.status != nil {
		*t.status = res.StatusCode
	}
	if err != nil || res.StatusCode != http.StatusOK {
		return res, err
	}
	defer res.Body.Close()
	body, err := egress.ReadBounded(res, t.maxBytes, false)
	if err != nil {
		return nil, err
	}
	var observedAt time.Time
	if t.evidence != nil {
		observedAt = time.Now().UTC()
	}
	if !jsontext.Value(body).IsValid() || t.store != nil && json.Unmarshal(body, t.store) != nil || t.validate != nil && !t.validate(body) {
		return nil, errSteamJSON
	}
	if t.evidence != nil {
		*t.evidence = SteamEvidence{Body: body, ObservedAt: observedAt}
	}
	// The guarded body has already returned its permit. The SDK still owns
	// endpoint decoding and Storefront identity resolution.
	res.Body = io.NopCloser(bytes.NewReader(body))
	return res, nil
}

func newSteamClient(network egress.Client, maxBytes int64, store *map[string]steamStorePresence) (*steam.Client, error) {
	return newSteamClientWithOptions(steamTransport{client: network, maxBytes: maxBytes, store: store})
}

func newSteamClientWithOptions(transport steamTransport, options ...steam.Option) (*steam.Client, error) {
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	opts := []steam.Option{
		steam.WithHTTPClient(client),
		steam.WithTimeout(15 * time.Second),
		steam.WithMaxResponseBodyBytes(transport.maxBytes),
		steam.WithProxySelector(nil),
		steam.WithRetry(0),
	}
	for _, class := range []steam.TrafficClass{steam.TrafficClassOfficialAPI, steam.TrafficClassPublicStorePage} {
		opts = append(opts, steam.WithTrafficPolicy(class, steam.TrafficPolicy{
			Retry: &steam.TrafficRetryPolicy{Retry: 0, Backoff: steam.DefaultRetryBackoffConfig()},
		}))
	}
	return steam.NewClient(append(opts, options...)...)
}
