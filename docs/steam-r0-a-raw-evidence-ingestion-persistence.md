# Steam AppDetails raw evidence ingestion and persistence

## Capture and rollout

Apply PostgreSQL migration 12 explicitly, then deploy the upgraded Server
before enabling capture on any Worker. Configure at most eight distinct,
positive AppIDs in the executing Worker's explicit YAML. Omit the field to
leave capture disabled. The same setting is available under `local_worker`
when using an embedded Worker. Enable only the AppIDs approved for the pilot.

```yaml
steam_evidence_app_ids: [434000, 1332010]
```

An eligible `steam.details` Attempt uses its existing fenced Steam request.
The scoped egress client enforces the existing Storefront origin, request URI,
permit and uncompressed response limit. The transport records the exact JSON
body and the Worker clock at the end of the read. The SDK resolves AppID
identity, including valid alternate envelope keys. Conflicting direct keys,
ambiguous matches, unsuccessful results, invalid JSON and missing required
identity or name yield no candidate. Price and media can be absent in a partial
result. The response bytes stay in a Worker-only sidecar until upload.
Request Schema 1, Output Schema 1, `steam_v1` and the details, price and media
scopes continue to use the existing Completion wire format.

## Worker upload

Send `POST /internal/v1/attempts/{attempt_uuid_v7}/evidence` before Completion
with the Worker Bearer token, `Content-Type: application/octet-stream` and the
original JSON bytes. The endpoint has a 512 KiB body limit at ingress and in
the handler. The Worker sends these headers.

| Header | Value |
| --- | --- |
| `X-Harvest-Evidence-SHA256` | Hex-encoded SHA-256 of the original body |
| `X-Harvest-Observed-At` | UTC response-read time in RFC3339 with fractional seconds |
| `X-Harvest-Job-ID` | Job UUIDv7 |
| `X-Harvest-Worker-ID` | Runtime Worker UUIDv7 |
| `X-Harvest-Generation` | Decimal Attempt fence generation |

The Server locks the Job first and verifies the principal's namespace and
capability grants, the registered Worker, the matching Attempt and generation,
the current execution key and a valid running lease. It recomputes SHA-256 and
uses the SDK's offline identity resolver against the AppID in the stored Steam
Target. The upload supplies no scope metadata. The first upload accepts up to one
minute of Worker clock skew relative to claim and receipt of upload.

HTTP 204 acknowledges durable staging, including a replay of the identical
bytes within the live lease. A replay retains the original observation time.
Different bytes for the same Attempt return 409. Invalid source identity
returns 400 and an expired live lease returns 409. The Worker tries the same
request at most twice on transport or 5xx failures, with a 750 ms limit per try
and a one-second lease budget reserved for Completion. Upload failures are
recorded by Job and Attempt while Completion proceeds.

## Storage and recovery

| Table | Responsibility |
| --- | --- |
| `harvest.steam_evidence_staging` | One immutable payload and digest per live Attempt, with an indexed recovery deadline and upload time |
| `harvest.steam_evidence` | Original `bytea`, logical size, hash, UUIDv7, format version 1, first Job and Attempt and confirmation times |
| `harvest.steam_evidence_attribution` | One record per confirmed Attempt, linking its Job and the archived version |

The version key is namespace, Target, `steam.details`, locale (`en` or
`zh-cn`), store country (`CN`, `US` or `HK`), actual pinned egress region
and raw SHA-256. The Server derives the first five source fields from the
stored Job and Target and reads the egress region from the Attempt. A repeat
with the same scoped hash and bytes reuses the existing payload and advances
its last confirmed observation time monotonically. A later A after A → B
therefore re-confirms A. Each attributed Attempt keeps the response observation
time and the database Completion receipt time. The two clocks can differ.
If recovery archives an earlier observation later, the version's first
observation time and lineage are updated together.

Completion commits Observation, Change Stream and Attempt receipt in its
existing transaction. The Server maintenance loop runs a separate recovery
transaction per staged record. It takes the Job lock, checks the committed
success or partial receipt and execution identity, then serializes version
admission for that Target. Archive or reuse, attribution and staging cleanup
commit together. Archive faults leave staging available for retry and cannot
roll back Completion. Completion replay leaves the attribution unchanged.
Running Attempts can be revisited, and unsuccessful terminal Attempts are
discarded. Staging expires after 24 hours. The existing ten-second maintenance
tick checks at most four due records and expires at most 16 old records.
Recoverable errors postpone the next check by 15 seconds. The staging foreign
key follows Attempt pruning. Archived lineage IDs survive normal execution
history pruning and attribution references the archived evidence by foreign key.

The temporary limit is five distinct versions per source scope. A sixth
distinct version is declined and logged. Earlier versions remain eligible for
confirmation. This bounds pilot payloads per scope without rotating history.

Diagnostic logs contain identifiers, byte sizes and outcomes. Evidence can
contain untrusted HTML and external links. The stored body excludes HTTP
credentials and request headers. It has no Consumer, Admin or public read
route. Restrict SQL access to authorized database operators and the Harvest
runtime. R0-B owns historical rotation, age windows, namespace capacity,
controlled read access and output schema evolution.
