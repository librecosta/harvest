# Steam R1-A acceptance

Verified on Windows with Go 1.27.4 and PostgreSQL 18.6 using the
R0-B baseline commit `513d8d9`.
[Design and rollout](steam-r1-a-versioned-offers-media-v2.md)

## Native verification

- `task check` passed format, dependency checks, vet, package tests,
  native build, Goose validation and sqlc comparison. The tagged
  integration package compiled with a zero-test run.
- Registry and Worker YAML tests cover default and explicit schema
  resolution, exact Offer lookup, dual registration and duplicate
  or unknown Offers. The isolated HTTP test checked the two published
  keys and v2 Schedule output-schema readback.
- On isolated PostgreSQL 18 with an HTTPS AppDetails fixture, a
  v1-only Worker and a dual-Offer Worker held two running Attempts
  against the same Target. The stored v1 fingerprint matched the
  R0-B wire calculation byte for byte. Both observations were
  independently readable, using v1 string arrays and v2 objects.
- The isolated run covered explicit plan admission and pause,
  default v1 and explicit v2 schedules, version-preserving edits,
  stored scheduled recurrence after Engine restart, idempotent
  submission and Completion replay, a wrong schema and Fence,
  lease expiry and retry. A namespace-scoped legacy Consumer saw
  only its original v1 events and kept its cursor during the pilot.
- Raw Evidence verified a single upstream call per Attempt and one
  shared raw row for matching v1/v2 bytes with distinct attribution.
  Formatting-only raw changes emitted no Observation Change Event.
  Changed media emitted one new scoped event per version. An injected
  archive failure preserved v2 Observation, Change Events and staging
  through the subsequent archive retry. An active
  v2-only Schedule protected the latest eligible shared evidence
  from the existing age-pruning pass. The R0-B native evidence suite
  also passed with the preview gate disabled.
- A separate native `task start` and `task dev:worker` smoke run
  registered both exact Offer keys from explicit Worker YAML in
  isolated PostgreSQL. The CLI Worker was scoped to a namespace
  with no fixture Jobs. The resulting worker row had both Offers
  and zero Attempts. Worker and Server shutdown completed.
- Media tests covered all seven playback roles, manifest-only
  videos, missing or false highlight, thumbnail-only input,
  duplicate and overlapping identities, missing thumbnails,
  unsigned 64-bit IDs, alias and conflicting source identity,
  hostile URL forms, count truncation and complete movie retention.
  The Server accepted an exact 32,768-byte segment and rejected a
  corresponding 32,769-byte payload. The two Worker fixture cases
  emitted 17,547 bytes with 12 movies and 30 screenshots, and
  31,130 bytes with 3 movies and 1 screenshot using long URL rows.
  Both had explicit truncation and preserved surrounding facts.

## Shared development database and real sample readback

The configured `harvest_dev` was already at Goose v13. No schema
migration was needed. The verification queries found 62 v1 Steam
AppDetails Schedules, 339 v1 Observations and zero v2 Schedules or
Observations. All four existing evidence payloads passed database
SHA-256 verification. The unchanged private configuration kept
v2 admission disabled against this database.

Native HTTP readiness returned 200 and Provider discovery published
both Steam details Offers. Operator downloads matched the metadata
SHA-256 and the private comparison files for all four archived
responses. The real Steam data was captured during R0-A and reused
offline in R1-A. The v2 executor and isolated Worker verification
made no new requests to real Steam in this stage. The synthetic
HTTPS fixture covers execution and Completion; the archived real
rows cover wire fidelity.

| AppID | Locale / market / egress | Bytes downloaded | v2 movies / screenshots | v1 media data bytes | v2 media data bytes | v2 encoded segment bytes |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 434000 Paws | en / HK / cn | 16,934 | 2 / 12 | 1,685 | 5,794 | 6,029 |
| 434000 Paws | zh-cn / HK / cn | 17,001 | 2 / 12 | 1,685 | 5,794 | 6,032 |
| 1332010 Stray | en / CN / cn | 15,663 | 3 / 8 | 1,923 | 5,358 | 5,593 |
| 1332010 Stray | zh-cn / US / cn | 15,882 | 3 / 8 | 1,923 | 5,358 | 5,596 |

The production executor preserved the actual movie and screenshot
IDs, order, missing formats, header and capsule roles and both
screenshot URL types. The archived captures supplied HLS H264,
DASH H264 and DASH AV1. Synthetic input validated MP4 and WebM.

## Performance

Two Windows local runs measured provider acquisition from an in-memory
response, including the normal SDK call and projection. Each row
shows the range across the two 400 ms benchmark runs. Worker
serialization timings cover the whole structured Result body
without Raw Evidence.

| Fixture | Schema | Acquire and project | Serialize Result |
| --- | --- | ---: | ---: |
| Typical | 1 | 0.21–0.25 ms | 6.30–6.41 µs |
| Typical | 2 | 0.47–0.61 ms | 22.99–24.81 µs |
| Near budget | 1 | 2.11–2.25 ms | 5.41–5.74 µs |
| Near budget | 2 | 4.17–4.63 ms | 28.22–29.18 µs |

Local Completion round trips include JSON validation, the fenced
Observation transaction and the response. Representative isolated
results used the same small response for both schemas.

| Scenario | v1 Completion | v2 Completion |
| --- | ---: | ---: |
| Initial structured result with an established connection | 4.58 ms | 4.23 ms |
| Same semantic data, changed raw whitespace | 3.14 ms | 3.16 ms |
| New media content | 4.00 ms | 4.68 ms |

Receipt replay for v2 took 0.57 ms. Worker execution, including TLS
fixture, upload and Completion, took 11.71–13.00 ms in those repeat
and change cases. The original v1 execution path still uses the
SDK's typed decoder and existing limits. These measurements reflect
local fixtures and do not establish production latency percentiles.

## Reproduction and limits

```powershell
task check
$env:HARVEST_NATIVE_EVIDENCE_DB_CONFIG = '<isolated-loopback-test-yaml>'
go test ./internal/worker -run '^TestSteamVersionedOffersNative$' -count=1 -v
$env:HARVEST_STEAM_V2_SAMPLES_DIR = '<private-operator-download-directory>'
go test ./internal/provider -run '^TestSteamMediaV2' -count=1 -v
go test ./internal/provider -run '^$' -bench '^BenchmarkSteamMediaVersions$' -benchtime=400ms -count=2
```

Apply migrations explicitly to a fresh isolated test database before
running the native fixture. Use another fresh fixture database for
`TestSteamEvidenceNative`. Sample filenames use
`<app_id>-<locale>.json` and stay outside Git. The R0-B v13 database,
existing production stream and Consumer are sufficient to roll out
the Server with an empty preview list. Pilot admission depends on
explicit grants and a dedicated namespace. StoreBrowse and other
R1-B acquisition work are outside this release.
