package provider

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"time"

	"github.com/gofurry/steam-go/web/storefront"
)

// MaxSteamEvidenceBytes is also the upstream Storefront response limit.
const MaxSteamEvidenceBytes = 512 << 10

// SteamEvidence carries the original response only within the Worker.
type SteamEvidence struct {
	Body       []byte
	ObservedAt time.Time
}

// ValidSteamEvidence applies the SDK's offline identity resolution to untrusted
// uploads. JSON v2 also rejects duplicate names and invalid UTF-8.
func ValidSteamEvidence(task Task, body []byte) bool {
	if len(body) == 0 || len(body) > MaxSteamEvidenceBytes || !jsontext.Value(body).IsValid() {
		return false
	}
	if task.Offer.OutputSchema >= 2 {
		match, _, err := resolveSteamDetails(body, steamAppID(task), task.Offer.OutputSchema == 3)
		return err == nil && steamText(match.Result.Data.Name, 256) != ""
	}
	var envelope storefront.AppDetailsEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return false
	}
	match, err := storefront.ResolveAppDetails(envelope, steamAppID(task))
	return err == nil && steamText(match.Result.Data.Name, 256) != ""
}
