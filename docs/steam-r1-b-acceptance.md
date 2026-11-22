# Steam R1-B acceptance

Verified on 2026-11-23 (Asia/Shanghai) with Windows Go 1.27.4 and
PostgreSQL 18.6. Baseline `57b262e`. The compiled Offer is
`steam.assets:1:1:steam_assets_v1`.
[StoreBrowse implementation and rollout](steam-r1-b-storebrowse-assets.md)

## Real StoreBrowse through Harvest

The native Server and Worker used the already configured direct
CN egress profile shape, an isolated PostgreSQL v13 control plane
and a dedicated `steam_assets_r1b` namespace. Jobs were explicit
Run now submissions from disabled Schedules with one Attempt each.
English/US was used for both games. Every wire request was the
keyless SDK GetItems with one AppID and `include_assets=true`.

| AppID | Attempt | HTTP / item success | Response bytes | Worker ms | Assets | Full Segment bytes | Permits |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 434000 Paws | `01a20402-a8ee-7823-ae14-34337985f645` | 200 / 1 | 1,433 | 3,005 | 9 | 3,539 | 1 |
| 1332010 Stray | `01a20402-ddc3-7f3e-9bb9-aefd4439be9a` | 200 / 1 | 2,171 | 397 | 15 | 7,022 | 1 |
| 434000 Paws confirmation | `01a20404-7cad-7ddf-a91d-b7606beed010` | 200 / 1 | 1,433 | 2,697 | 9 | 3,539 | 1 |
| 1332010 Stray confirmation | `01a20404-b0a0-797b-97ee-39a39fa92698` | 200 / 1 | 2,171 | 421 | 15 | 7,022 | 1 |

Every response carried one visible Application in `store_items`
with the requested AppID and an assets object. TLS and requests
ran through the Worker Permit and egress code. The four Permits
finished and issued four charges against each persisted global,
exit, official provider and official account bucket. Each bucket
reported 7,208 response body bytes. Account policy capped the
pilot at four requests for the UTC day. An initial URI attestation
mismatch was caught before dialing, with zero issued Permits.
The SDK's zero-ID omission policy was then matched and covered by
a request-byte test.

Both games returned Library Capsule, Library Hero, Library Capsule
2x, Store Header and Main/Small Capsule, Page Background and Raw
Background, and Community Icon. Stray additionally returned Library
Hero 2x, Header 2x, Main and Small Capsule 2x, and Store Hero Capsule
with its 2x variant. All persisted candidates fit the budget.
The repeated responses explicitly confirmed absence of all four
recognized Library Logo slots. The two real games supplied no Logo
reference to persist. Native fixtures verified the primary and alias
Logo slots and their distinct asset types. CDN download and
availability checks were outside the pilot.

PostgreSQL retained two present asset observations and two Change
Events across the four successful Attempts. The repeated collections
confirmed existing results. There were zero AppDetails Raw Evidence
rows in this isolated control plane. Shared development stayed on
its existing Goose v13 schema and policy. The live pilot used a
separate control plane precisely to leave the running shared
development services and schedules alone.

## Native and regression checks

- `task check` passed formatting, dependency consistency, vet,
  package tests, Windows build, Goose validation and sqlc diff.
  The tagged integration package compiled in a zero-test run.
- Provider tests covered exact SDK wire attestation, source identity
  through both AppID fields, wrong item type, duplicate rows,
  invisible or unsuccessful items, confirmed empty and missing
  assets, 403 authentication, 429 throttling, 500 service errors,
  invalid JSON and the 512 KiB response limit.
- URL and projection tests covered every mapped slot, Logo aliases,
  2x sizes, same URL across distinct roles, locale and Slot changes,
  query suppression, encoded legal filenames, unapproved domains,
  cross-AppID paths, traversal and controls. The budget fixture
  accepted the exact 32,768-byte segment, dropped one complete
  candidate for a one-byte overflow, and exercised the 32-row
  cap with synthetic candidates. The compiled vendor map has 17
  roles.
- `TestSteamAssetsNative` used Windows Worker execution, TLS fixtures,
  actual HTTP registration and Completion, and isolated PostgreSQL.
  The old Worker registered and could not claim this Offer. Default
  and foreign namespace gates and the six-hour cadence were exercised.
  One fenced Permit per Attempt used the official shared account
  policy. A 403 applied its cooldown and preserved Last Known Good.
  A 500 retried under a new Attempt. Completion replay was
  idempotent. Locale, market and pinned egress changes produced
  independent scope results. The `intl` exit label was a fixture.
- `TestSteamVersionedOffersNative` and `TestSteamEvidenceNative` covered
  existing AppDetails v1/v2, schedules, legacy Consumer isolation
  and the AppDetails Raw Evidence pipeline. Native pilot Server
  and Worker completed graceful shutdown. The isolated test used
  the existing 13 migrations. This change adds no migration.

To reproduce the fixture, use an explicit YAML pointed at an
empty loopback PostgreSQL 18 database ending in `_test`. Apply
`task migrate ACTION=up CONFIG=<file>` first.

```powershell
task check
$env:HARVEST_NATIVE_EVIDENCE_DB_CONFIG = '<isolated-loopback-yaml>'
go test ./internal/worker -run '^TestSteamAssetsNative$' -count=1 -v
go test ./internal/provider -run '^TestSteamAsset' -count=1 -v
```

The real-run Operator result files and local runtime YAML are under
the ignored pilot directory. The public Steamworks authentication
reference describes general methods and key requirements. The
actual keyless result above was measured through Harvest.
[Steamworks Web API](https://partner.steamgames.com/doc/webapi_overview)

## R1-C handoff

GoFurry can build its projection against the stable asset identity,
the distinct Store, Library and Community roles and the scope of
the accepted Observation. R1-C must handle absent Logo and other
optional slots, truncation, and separate market and locale facts.
Keep the preview grant scoped until v1/v2 and assets Consumer
compatibility has been validated. This release ends at R1-B.
