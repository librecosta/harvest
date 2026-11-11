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
}

func (t steamTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	res, err := t.client.Do(r)
	if err != nil || res.StatusCode != http.StatusOK {
		return res, err
	}
	defer res.Body.Close()
	body, err := egress.ReadBounded(res, t.maxBytes, false)
	if err != nil {
		return nil, err
	}
	if !jsontext.Value(body).IsValid() || t.store != nil && json.Unmarshal(body, t.store) != nil {
		return nil, errSteamJSON
	}
	// The guarded body has already returned its permit. The SDK still owns
	// endpoint decoding and Storefront identity resolution.
	res.Body = io.NopCloser(bytes.NewReader(body))
	return res, nil
}

func newSteamClient(network egress.Client, maxBytes int64, store *map[string]steamStorePresence) (*steam.Client, error) {
	client := &http.Client{
		Transport: steamTransport{client: network, maxBytes: maxBytes, store: store},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	opts := []steam.Option{
		steam.WithHTTPClient(client),
		steam.WithTimeout(15 * time.Second),
		steam.WithMaxResponseBodyBytes(maxBytes),
		steam.WithProxySelector(nil),
		steam.WithRetry(0),
	}
	for _, class := range []steam.TrafficClass{steam.TrafficClassOfficialAPI, steam.TrafficClassPublicStorePage} {
		opts = append(opts, steam.WithTrafficPolicy(class, steam.TrafficPolicy{
			Retry: &steam.TrafficRetryPolicy{Retry: 0, Backoff: steam.DefaultRetryBackoffConfig()},
		}))
	}
	return steam.NewClient(opts...)
}
