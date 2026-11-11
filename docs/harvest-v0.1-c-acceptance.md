# V0.1-C acceptance

## SDK and network boundary

Harvest pins the MIT-licensed `github.com/gofurry/steam-go v1.3.12`. The
HTTP client, traffic policies, request execution and the three methods used
here match the audited `main` at `cfa3bec158f72c8651310f5f43382952e7c38f36`.
The injected HTTP client routes both traffic classes through the Worker
session. SDK retries are zero, and the adapter borrows connections from
the Worker's pool. The injection test exercised 503, redirect, client Close
and a subsequent successful request. [SDK source](https://github.com/gofurry/steam-go/tree/cfa3bec158f72c8651310f5f43382952e7c38f36)

- Each Attempt can obtain one Steam GET permit. The control plane derives
  the fixed HTTPS origin, path and canonical query from the stored Job.
  Workers independently check the SDK request. Steam redirects stop at
  the initial response. The 5-second Permit limit covers the connection
  and response body. Storefront, News and Players body caps are 512 KiB,
  128 KiB and 4 KiB.
- The selected official methods use public keyless calls. No credential
  is needed to run the fixture. Storefront 429, 401/403 and successful
  HTTP responses with an HTML content type cool the configured account.
  The existing bounded Retry-After handling applies.
  [News API](https://partner.steamgames.com/doc/webapi/ISteamNews),
  [Players API](https://partner.steamgames.com/doc/webapi/ISteamUserStats)
- A4 supplies the common Storefront provider bucket. Start at 30/minute,
  burst 1 and concurrency 1. Official traffic uses separate provider and
  shared account buckets. The account's `daily_limit` is enforced on
  issued permits under the PostgreSQL row lock, independently of refill.
  The ledger resets at UTC midnight, survives control-plane restart and
  charges even a grant whose response never arrives. Queue admission
  waits while the account budget is exhausted. Shared quota identities
  remain bound across source removal and namespace changes. Existing
  schema 6 supports these changes. Operations status shows class, quota
  labels and recent Permit counts scoped to the requested namespace and
  capability. [Valve Web API terms](https://steamcommunity.com/dev/apiterms)

## Executed checks

- PostgreSQL 18.6, two independent Workers and local TLS fixtures exercised
  direct, HTTP CONNECT and SOCKS5 with ambient proxy variables set.
  Six country/language combinations created 18 independent current
  segments with six Storefront grants. Storefront used one existing
  CONNECT tunnel across the six SDK clients. The two Workers shared the
  Storefront rate even on different proxies.
- Missing price fields preserved LKG, explicit free and zero-priced
  promotions were distinct, and localized release text retained its
  precision. Wrong AppID, false success, duplicate envelope keys and
  ambiguous Storefront matches preserved prior good data. Media remained
  bounded Steam links. Wrong AppID, store country, path or extra key
  parameters received no grant.
- News deduplicated GIDs, bounded excerpts, removed URL queries and
  fragments, and rejected identity conflicts and invalid timestamps.
  A snapshot consumer ACKed a baseline and one confirmed-empty page event.
  The prior news observation remained in history.
- Two successful zero player samples remained separate. Missing, null,
  negative or unsuccessful counts and timeouts created no observations.
  Missed point-in-time slots remained gaps. The SDK Raw method exposes
  scalar presence for Players while keeping request construction in the SDK.
- A numeric daily limit edit left one final permit available across both
  Workers. Replaying a grant and finish kept one debit and one recorded
  outcome. The exhausted account remained blocked after restart and
  resumed when the fixture advanced its stored accounting day. Unit tests
  check namespace rebinding, the UTC boundary and the final five-second
  permit window.
- Refusals and redirect responses caused one on-wire attempt. An old
  routing revision could no longer authorize a Permit. Cancellation
  finished the Permit. A killed Worker left an expired Attempt that a new
  process reclaimed. A0–B2 regression coverage includes SQL generation,
  database loss and recovery, bounded history and consumer cursors.

Windows amd64 passed `task check`, `go test -race ./...`, the C fixture
with `-race`, and the A0–B2 integration regressions. The original eight-minute
suite budget expired during C crash recovery, so the combined suite now has
ten minutes. Compose validation and both Linux amd64 and arm64 image builds
passed. The standard integration run uses local fixtures and generated test
credentials.

## Performance

On Windows amd64 (Ryzen 7 5800H, Go 1.27.3), six Storefront scopes took
10.51 s with the 30/minute policy. All five on-wire intervals exceeded
1.85 s. The run used one CONNECT tunnel, six grants and 11 Permit RPCs
including deferrals. Permit RPC p95 was 27.0 ms.

The six-Job window added 1,873 PostgreSQL statements including background
work, 98,904 B of WAL, 3,905 B of upstream bodies and 5,791 B of Worker
control request bodies. PostgreSQL had four connections at the end of the
sample. Go HeapAlloc for the server and two Workers went from 1.81/1.19/1.18
MiB to 2.10/2.01/1.29 MiB. The ten-item News projection took 0.28–0.30 ms/op,
about 83 KiB/op and 408 allocs/op. These are local fixture measurements.

Reproduce with `go test -tags=integration -run '^TestC$' -count=1 -v ./integration`.
Run `go test ./internal/provider -run '^$' -bench '^BenchmarkSteamNewsProjection$' -benchmem`
for the typed ten-item News projection.

## Limits

The fixture exercises the real SDK with local HTTPS responses. Live
Storefront behavior, public API availability and production latency remain
unmeasured. The UTC budget covers this Harvest instance's grants. Shared
upstream use outside that instance must fit in the remaining allowance.
Storefront is an observed source with localized release text and optional
prices. News supplies a bounded current page and retained page history.
