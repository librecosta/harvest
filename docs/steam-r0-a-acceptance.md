# Steam raw evidence acceptance

Run on 2026-11-22 (Asia/Shanghai) using native Windows Go 1.27.4, Task
3.53.1 and PostgreSQL 18.6. See the [ingestion and persistence design](steam-r0-a-raw-evidence-ingestion-persistence.md)
for the protocol, rollout order and storage semantics.

## Change inventory

| Area | Files |
| --- | --- |
| Capture and identity | `internal/provider/{registry,steam,steam_catalog,steam_client,steam_details,steam_evidence}.go` |
| Worker YAML and upload | `config.worker.example.yaml`, `internal/config/services.go`, `internal/worker/{client,evidence,worker}.go` |
| Routing and control plane | `internal/routers/{router,url}.go`, `internal/handlers/{api,evidence}.go`, `internal/execution/steam_evidence.go`, `internal/server/server.go` |
| Additive schema and generated access | `db/migrations/00012_steam_raw_evidence.sql`, `db/queries/steam_evidence.sql`, `internal/database/schema.go`, `internal/database/queries/{models,steam_evidence.sql}.go` |
| Checks and documentation | `internal/provider/steam_evidence_test.go`, `internal/worker/steam_evidence_native_test.go`, the two Steam raw evidence documents |

## Native checks and isolated PostgreSQL

- `task check` passed, including format, dependency consistency, vet, unit
  tests, Windows build, Goose validation and sqlc diff.
- `TestSteamEvidenceCapture` checked the 512 KiB boundary, exact bytes
  including whitespace and HTML, alternate envelope keys, conflicting or
  ambiguous identities, unsuccessful or incomplete results, duplicate keys,
  malformed JSON and the opt-in switch. Enabled and disabled captures used
  the same Completion representation.
- `TestSteamEvidenceNative` passed on a freshly migrated, isolated loopback
  PostgreSQL 18.6 database. It exercised the Worker execution path, its
  Storefront TLS fixture, the real HTTP upload and Completion routes and
  PostgreSQL staging and archive. Each fixture execution made one Storefront
  request. Readback matched the original bytes and SHA-256.
- The native suite ran upload response loss, duplicate and conflicting bodies,
  authentication and scope denial, incorrect Job, Worker and generation,
  oversize input, invalid AppID, expired lease, interrupted Worker, staging
  expiry, a disabled Worker, Completion replay, A → B → A, out-of-order
  archive and the five-version limit. It checked English and Simplified Chinese, CN, US and HK, and two
  namespaces. A second pinned egress label was simulated in the test Attempt
  for the archive key check. The real upstream run used the configured CN exit.
- A PostgreSQL trigger forced attribution insertion to fail after Completion.
  The current Observation and its Change Event remained queryable, and the
  payload was still staged. Removing the trigger let recovery write one
  version and one attribution. A trigger on Observation insertion separately
  forced Completion to roll back. Recovery left that Attempt unarchived, and
  terminal failure removed its staged row.

To reproduce the isolated run, use an explicit YAML for an empty loopback
PostgreSQL 18 database ending in `_test`. Apply `task migrate ACTION=up
CONFIG=<path>` and point the test at that YAML.

```powershell
$env:HARVEST_NATIVE_EVIDENCE_DB_CONFIG = '<path>'
go test ./internal/worker -run '^TestSteamEvidenceNative$' -count=1 -v
go test ./internal/provider -run '^$' -bench BenchmarkSteamEvidenceCapture -benchtime=300ms -count=1
```

## Shared development database

Verified the configured `harvest_dev` target and Goose version 11 with no
other connected clients. Took a custom-format PostgreSQL 18.6 backup and
restored it into an isolated database. The restored v11 row counts and
content fingerprints matched all 18 existing Harvest tables. The private
backup archive is 386,337 bytes and its SHA-256 is
`a36bbc8d40636025f7afffdcb7be9b60402e50aeb09432aed796086eeac72747`.

Ran `task migrate ACTION=up` explicitly with the existing private Server
YAML. `task migrate ACTION=status` reported 12 applied. The three new tables
were present and all 18 previous table fingerprints still matched. After the
real smoke run, fingerprints still matched for 303 pre-existing Steam
AppDetails Observations, 192 corresponding current rows and all 159
pre-existing schedules.

## Real Steam on Windows

The native Server returned HTTP 200 at `/readyz`. In the initial compatibility
run, a previously running Worker claimed four submitted Storefront Jobs and
sent existing Completion payloads. All four succeeded on the new Server.
For the pilot, used the same private YAML and configured direct CN route at a
temporary loopback control port. A dedicated runtime captured evidence for
only [Paws](https://store.steampowered.com/app/434000/) and
[Stray](https://store.steampowered.com/app/1332010/Stray/). Each request used
a locale and store country without existing observations for that Target.

| AppID | Locale / store / egress | Job | Accepted Attempt | JSON bytes | PostgreSQL column bytes | Upload round trip |
| --- | --- | --- | --- | ---: | ---: | ---: |
| 434000 | en / HK / cn | `01a203b9-3b20-7a14-9056-a3283daa8475` | `01a203b9-6065-7910-8602-9ce587d51117` | 16,934 | 5,389 | 199 ms |
| 434000 | zh-cn / HK / cn | `01a203b9-3c09-7038-bcfd-1a197c189d36` | `01a203b9-707a-7655-a316-66b251d64677` | 17,001 | 5,450 | 129 ms |
| 1332010 | en / CN / cn | `01a203b9-3cc9-7eec-b404-d21e3977e7a5` | `01a203b9-76db-75d6-94fb-e3f392ef824e` | 15,663 | 5,632 | 104 ms |
| 1332010 | zh-cn / US / cn | `01a203b9-3dda-7fdf-aa76-01d6d396fdae` | `01a203b9-5825-7b15-994f-cb3bcbbdc5dc` | 15,882 | 5,954 | 229 ms |

All four Jobs succeeded. Each accepted Attempt used one Storefront permit,
and each had one archive attribution. Three Jobs needed an existing
execution retry after an initial timeout or transport error. The failed
Attempts produced no evidence. No quota or schedule changes were needed.
The Worker byte counts matched PostgreSQL, and both PostgreSQL `sha256`
and independent Windows SHA-256 on the readback bytes matched each stored
digest and the upload digest verified by the Server. All four bodies parsed
as JSON with the correct internal AppID. Paws included detailed descriptions
and 12 screenshot entries plus two movie entries per response. Stray
included detailed descriptions and eight screenshot entries plus three
movie entries per response. The two clock sources differed on this run.
The Worker response times appeared 0.54–0.72 s ahead of the database receipt
times, so the archived timestamps preserve both clocks independently.

Staging was empty after archive. The pilot Worker and Server shut down
and both existing private YAML files were restored byte for byte.

## Performance and capacity

The fixture payloads below use repeated padding to reach exact sizes.
PostgreSQL column sizes describe these compressible fixtures and exclude
index and row overhead. The real responses above ranged from 15,663 to
17,001 logical bytes.

| Fixture bytes | Capture off / on plus hash | SHA-256 alone | Upload round trip | PostgreSQL column bytes |
| ---: | ---: | ---: | ---: | ---: |
| 4,096 | 146 / 116 µs | 2.3 µs | 1.0–2.0 ms | 190–191 |
| 65,536 | 469 / 570 µs | 34.4 µs | 3.0 ms | 895 |
| 262,144 | 1,785 / 1,984 µs | 137 µs | 6.3 ms | 3,135 |
| 524,288 | 3,504 / 2,997 µs | 276 µs | 9.3 ms | 6,140 |

The capture benchmark includes the SDK parse, and the off/on differences
varied across sizes in this short run. Capture reuses the read buffer.
Allocations differed by at most two per operation. The scoped A → B → A
sequence kept two raw payload rows and four Attempt attributions after a
further A. It emitted one initial semantic Change Event. Completion timings
on warm fixture calls were roughly 2–5 ms with upload enabled and 3 ms on
the disabled control. The first fixture Completion took about 18 ms and
created the initial Observation and event. Upload adds its own pre-Completion
wall time. Archive runs on the maintenance tick outside the Completion
transaction. These small samples do not estimate production percentiles.

## R0-B handoff

The pilot uses a five-version admission cap and a 24-hour staging expiry.
Payloads, historical attribution and per-scope versions remain in
PostgreSQL. Before wider enablement, R0-B needs verified age and version
rotation with latest-version protection, namespace storage budgets and
attribution lifecycle, a scoped read interface and a reviewed Steam v2
contract. Keep capture scoped to the approved pilot AppIDs until those
controls have been exercised.
