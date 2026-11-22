package provider

import (
	"bytes"
	"crypto/sha256"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type evidenceSource struct {
	body  []byte
	calls int
}

func (s *evidenceSource) Do(r *http.Request) (*http.Response, error) {
	s.calls++
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(bytes.NewReader(s.body)), ContentLength: int64(len(s.body)), Request: r}, nil
}

func evidenceTask(enabled bool) Task {
	return Task{ExternalKey: "440", Namespace: "test", Offer: Offer{Capability: "steam.details"},
		Params: []byte(`{"language":"english","store_country":"US"}`),
		Scope:  Scope{Locale: "en", Region: "us", EgressRegion: "cn"}, CaptureSteamEvidence: enabled}
}

func TestSteamEvidenceCapture(t *testing.T) {
	const exact = "{\n  \"alternate\" : {\"success\":true,\"data\":{\"name\":\"Fixture\", \"steam_appid\":440,\"detailed_description\":\"<b>Original</b>\"}}\n}\n"
	cases := []struct {
		name, body string
		valid      bool
	}{
		{"alternate", exact, true},
		{"direct", strings.Replace(exact, "alternate", "440", 1), true},
		{"wrong identity", strings.Replace(exact, "440", "441", 1), false},
		{"unsuccessful", strings.Replace(exact, "true", "false", 1), false},
		{"incomplete identity", strings.Replace(exact, `"steam_appid":440,`, "", 1), false},
		{"incomplete name", strings.Replace(exact, "Fixture", "", 1), false},
		{"malformed", exact[:len(exact)-3], false},
		{"duplicate", `{"440":{"success":false},"440":{"success":true,"data":{"steam_appid":440,"name":"x"}}}`, false},
		{"ambiguous", `{"a":{"success":true,"data":{"steam_appid":440,"name":"x"}},"b":{"success":true,"data":{"steam_appid":440,"name":"y"}}}`, false},
		{"conflicting direct key", `{"440":{"success":true,"data":{"steam_appid":441,"name":"x"}},"a":{"success":true,"data":{"steam_appid":440,"name":"y"}}}`, false},
		{"boundary", exact + strings.Repeat(" ", MaxSteamEvidenceBytes-len(exact)), true},
		{"oversized", exact + strings.Repeat(" ", MaxSteamEvidenceBytes-len(exact)+1), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := &evidenceSource{body: []byte(tc.body)}
			start := time.Now()
			result := acquireSteamDetails(t.Context(), evidenceTask(true), source)
			if source.calls != 1 || (result.SteamEvidence != nil) != tc.valid || ValidSteamEvidence(evidenceTask(true), source.body) != tc.valid {
				t.Fatalf("unexpected capture or validation: calls=%d result=%s/%s sidecar=%t", source.calls, result.State, result.Code, result.SteamEvidence != nil)
			}
			if !tc.valid {
				return
			}
			if e := result.SteamEvidence; !bytes.Equal(e.Body, source.body) || e.ObservedAt.Before(start) || e.ObservedAt.After(time.Now()) {
				t.Fatal("sidecar differs from the observed response")
			}
			without := acquireSteamDetails(t.Context(), evidenceTask(false), source)
			for i := range without.Segments {
				without.Segments[i].ObservedAt = result.Segments[i].ObservedAt
			}
			withWire, err := json.Marshal(result, json.Deterministic(true))
			withoutWire, disabledErr := json.Marshal(without, json.Deterministic(true))
			withJSON, withoutJSON := jsontext.Value(withWire), jsontext.Value(withoutWire)
			if err != nil || disabledErr != nil || without.SteamEvidence != nil ||
				withJSON.Canonicalize() != nil || withoutJSON.Canonicalize() != nil || !bytes.Equal(withJSON, withoutJSON) {
				t.Fatalf("evidence changed the Completion wire payload: %s versus %s (%v / %v)", withJSON, withoutJSON, err, disabledErr)
			}
		})
	}
}

func BenchmarkSteamEvidenceCapture(b *testing.B) {
	const base = `{"440":{"success":true,"data":{"steam_appid":440,"name":"Fixture","detailed_description":"%s"}}}`
	for _, size := range []int{4 << 10, 64 << 10, 256 << 10, MaxSteamEvidenceBytes} {
		body := []byte(fmt.Sprintf(base, strings.Repeat("x", size-len(base)+2)))
		for _, enabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("%d/capture_%t", size, enabled), func(b *testing.B) {
				source := &evidenceSource{body: body}
				task := evidenceTask(enabled)
				b.ReportAllocs()
				b.SetBytes(int64(size))
				for b.Loop() {
					result := acquireSteamDetails(b.Context(), task, source)
					if enabled {
						_ = sha256.Sum256(result.SteamEvidence.Body)
					}
				}
			})
		}
		b.Run(fmt.Sprintf("%d/sha256", size), func(b *testing.B) {
			b.SetBytes(int64(size))
			for b.Loop() {
				_ = sha256.Sum256(body)
			}
		})
	}
}
