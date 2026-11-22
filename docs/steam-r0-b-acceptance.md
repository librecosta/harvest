# Steam R0-B acceptance

Verification ran on 2026-11-22 (Asia/Shanghai) with Windows Go 1.27.4
and PostgreSQL 18.6. The starting point was R0-A commit `0a5a789`.
[Retention and access design](steam-r0-b-retention-access-schema.md) ·
[Frozen Steam v2 contract](steam-r0-b-steam-v2-contract.md)

## Shared development database

The configured target was `harvest_dev`, with Goose version 12. A
435,212-byte PostgreSQL custom-format backup restored successfully into an
isolated database. Its SHA-256 is
`0c82f049a3b21471afce4f8f2b4906e33df233fac47e600b1513792bb35f4950`.
All 21 original Harvest tables matched by row count and row-content
fingerprint on restore. Migration 13 passed on the restored database before
running `task migrate ACTION=up` with the existing private Server YAML.
The shared database applied exactly one migration and `task migrate
ACTION=status` reported versions 1–13 applied. All 21 original
column projections still matched their pre-migration counts and fingerprints.

The migrated ledger reported 65,480 logical bytes across four versions
and four attributions, with 22,425 PostgreSQL payload column bytes, zero
staging rows and zero capacity denials. Each version's latest Server
confirmation time and Attempt matched its original attribution. The
four prior archive timestamps remain unknown. The database recomputed all
four SHA-256 values successfully. The retention and capacity mutation
scenarios ran only on isolated fixture databases.

## Real Windows HTTP and offline contract samples

The native Server returned 200 from `/readyz`. Using the configured
Operator principal, the metadata list, storage summary and all four raw
downloads returned successfully. An independent Windows SHA-256 check on
each HTTP body matched the listing digest, and byte lengths matched the
database. The configured Reader received 403 on direct raw download.
These are genuine R0-A Storefront captures. R0-B reused them without new
Steam requests.

| AppID | Locale / store / egress | Archived Evidence ID | Raw bytes | Screenshots / movies |
| --- | --- | --- | ---: | ---: |
| 434000 (Paws) | en / HK / cn | `01a203b9-97a5-7d66-8d99-2c9e7f567b7c` | 16,934 | 12 / 2 |
| 434000 (Paws) | zh-cn / HK / cn | `01a203b9-9820-7e3b-9704-434afb37470c` | 17,001 | 12 / 2 |
| 1332010 (Stray) | en / CN / cn | `01a203b9-9888-7acf-a1e5-0ccf923a4147` | 15,663 | 8 / 3 |
| 1332010 (Stray) | zh-cn / US / cn | `01a203b9-9708-7185-9304-7905103cbf16` | 15,882 | 8 / 3 |

Offline parsing checked each internal AppID and the available movie,
screenshot, image, description, requirements, support, ratings and release
fields. Every movie in these four captures supplied HLS H264, DASH H264 and
DASH AV1. A synthetic contract fixture covered direct MP4/WebM variants and
64-bit identifier precision. The encoded media example was 5,794 bytes
for Paws and 5,358 bytes for Stray. Repeating captured candidate shapes to
30 screenshots and 12 movies yielded 18,034 and 18,414 bytes respectively.
A synthetic long-URL fixture exceeded the 32 KiB limit at the same counts.
R1 must measure the complete encoded segment, including its envelope, for
its actual candidate population.

## Native checks and isolation

- `task check` passed formatting, dependency consistency, vet, regular
  tests, Windows build, Goose validation and sqlc diff. The pre-existing
  tagged integration package also compiled in a zero-test run. Its
  container-backed cases were outside this native run.
- `TestSteamEvidenceNative` used the real Windows Worker and Server HTTP
  code paths, an HTTPS AppDetails fixture, and isolated PostgreSQL 18.
  It checked Completion replay, upload response loss, archive and Completion
  fault injection, recovery, staging expiry, legacy Worker behavior and
  source/Attempt validation.
- The R0-B scenarios covered six distinct versions, A → B → A, delayed
  receipts, concurrent recovery and pruning, a 91-day cutoff, Schedule
  disable and re-enable, inactive Target, multiple markets, locales,
  egresses and namespaces, attribution cascades and restart of the
  maintenance Engine. The alternate pinned egress and routing policy
  are fixture constructs. The historical real samples use the CN exit.
- Quota scenarios exercised exact and near limits, deduplication at
  capacity, competing concurrent admissions, a lower budget beneath
  existing use, absence of eligible versions, staged capacity markers,
  rejection counts, and admission after history expired. The metadata and
  raw routes exercised Operator scope, capability checks, Reader, Writer,
  Worker and unauthenticated requests, stable pagination, staged and
  pruned IDs, and byte-for-byte downloads.
- The archive-failure injection preserved successful Completion, current
  Observation and its Change Event. Retention tests verified that
  Observation contents and the Consumer cursor survived cleanup.
  The evidence changes add no v2 Observation or Consumer events.

Reproduce with explicit private YAMLs pointed at isolated loopback
PostgreSQL 18 databases ending in `_test`. Initialize each with
`task migrate ACTION=up CONFIG=<path>`. The optional samples path holds the
four read-only Operator downloads using filenames `<app_id>-<locale>.json`.

```powershell
$env:HARVEST_NATIVE_EVIDENCE_DB_CONFIG = '<isolated-worker-config>'
go test ./internal/worker -run '^TestSteamEvidenceNative$' -count=1 -v
$env:HARVEST_NATIVE_EVIDENCE_SCALE_DB_CONFIG = '<isolated-scale-config>'
go test ./internal/execution -run '^TestSteamEvidenceQueryScaleNative$' -count=1 -v
$env:HARVEST_STEAM_V2_SAMPLES_DIR = '<private-samples-directory>'
go test ./internal/provider -run '^TestSteamV2' -count=1 -v
```

## Measured performance and storage

The scale test used 101 local PostgreSQL calls per measurement. The p95
times below include pgx round trips. The 50-row list used an immutable UUID
cursor. PostgreSQL's 10,000-row plan applied an indexed page limit before
looking up registered Target metadata.

| Versions | List 50 p95 | AppID list p95 | 4 KiB raw p95 | Budget lookup p95 | Logical bytes | Payload column bytes |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 0.66 ms | 0.59 ms | 0.53 ms | 0.52 ms | 409,600 | 13,400 |
| 1,000 | 1.06 ms | 0.89 ms | 0.52 ms | 0.52 ms | 4,096,000 | 134,000 |
| 10,000 | 0.94 ms | 0.79 ms | 0.53 ms | 0.52 ms | 40,960,000 | 1,340,000 |

The fixture bodies were padded and highly compressible. PostgreSQL
`pg_column_size` excludes other table and storage overhead and depends on
the data. Admission always uses logical payload bytes.

- Two 32-version pruning batches with cascades at 10,000 rows took 5.85 and
  2.82 ms. The separate tracking test removed nine versions and two
  intermediate attributions in 6.39 ms.
- Two recovery passes with concurrent pruning took 77.45 ms for the
  four pending mixed confirmations and rotations, including concurrent
  transactions. The capacity runs took 7.00–19.00 ms per recovery group
  with 256 KiB staged payloads.
- Pruning under deliberate maintenance-lock contention took 30.00 ms
  end to end, including a 13.00 ms intentional hold. A current-results
  Consumer request completed during that hold in 11.00 ms.
- Successful fixture Completion requests after warm-up took 2.13–7.00 ms.
  The first Completion on each of two fixtures took 19.36 and 27.86 ms.
  Archive and prune use separate
  database transactions and never run inside Completion. These timings
  are local smoke measurements. They do not estimate a production
  latency distribution or quantify a before/after performance delta.

## Rollout limits and R1 handoff

The default is five versions per scope, a 90-day window, 256 MiB per
namespace, 1 GiB globally and 32 expiry candidates per regular batch.
Attribution attached to a deleted version is removed. Retained versions
keep first observation and latest receipt lineage while intermediate
attribution ages out. Lowering a budget reports overage and restricts new
admissions. Backlogs can span multiple one-minute maintenance ticks.
Capacity-limited staging keeps the existing 24-hour lifetime and is measured
separately from the archive budgets. Concurrent archive and prune
contention was exercised with independent PostgreSQL connections. The
server rollout uses the existing single active control-plane topology.

R1 can reuse the indexed and authorized Evidence store, the four verified
AppDetails shapes and the frozen field/format matrix. Before enabling a v2
Offer, R1 needs end-to-end verification of full segment sizes, live
candidate variety, explicit v1/v2 Consumer compatibility and the independent
StoreBrowse source policy. The runtime still advertises Steam details
Output Schema v1.
