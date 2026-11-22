# Steam details versioned Offers and media v2

## Offer selection

Steam details has two compiled Offers. Each is visible through
`GET /api/v1/providers`.

| Full Offer key | Request Schema | Output Schema |
| --- | ---: | ---: |
| `steam.details:1:1:steam_v1` | 1 | 1 |
| `steam.details:1:2:steam_v2` | 1 | 2 |

A new one-shot Job or Schedule can set `output_schema: 2` in the
existing `request` object. Omission selects the manifest's legacy
default, Steam v1. The compiler requires one unambiguous Offer per
request and output schema pair and rejects duplicate full keys. The
selector is removed from the fingerprint input after resolution. The
same original fingerprint fields and the selected full Execution Key
distinguish v1 and v2, so an explicitly selected v1 has the same
fingerprint as an implicit v1.

Jobs and Schedules keep their exact Execution Key in the existing
PostgreSQL columns. Run now, Retry Job, due Scheduling and Worker
Claims resolve that stored key. Replacing a Schedule request without
changing capability or request schema preserves its stored output
selection when the selector is omitted. An explicit selector can
change the binding through the existing revision-checked edit.
Readback surfaces the non-v1 output selector on Job and Schedule
requests. Existing v1 response shapes remain compatible.

The Worker YAML and registration independently support unique full
Offers. A Worker that announces only v1 can claim only v1. Workers
select the exact executor from their registered Claim Offer and use
its schema for Completion. The Server selects the stored Offer for
dispatch, Permit, lease renewal and Completion. Each Completion
segment must match that schema. Existing namespace grants, egress
verification, quota, lease and Attempt Fence rules still apply.

## Controlled rollout

Upgrade the Server first. For the pilot, add only dedicated namespaces
with separately authorized consumers to the explicit Server YAML.

```yaml
steam_media_preview_namespaces: [steam_media_pilot]
```

The default list is empty. The preview gate covers v2 submission,
Schedule creation and enablement, schedule reconciliation, Run now,
Job retry and claim dispatch. Existing accepted Attempts can finish
or replay a receipt after a gate change. Existing observations remain
readable by their namespace grants. Workers can register both
compiled Offers independently of the preview gate.

Configure the dedicated namespace in the existing network policy and
grant only the necessary principals access. Apply the existing Steam
Storefront quota and egress policy. Preserve the production GoFurry
Consumer's current stream and grant. A reader granted the pilot
namespace can receive its v2 events, so keep production namespaces
out of the preview list until consumer promotion is planned. Enable
the optional v2 entry in the Worker YAML after the Server upgrade.
Create or run v2 work explicitly. Existing v1 schedules continue
under either configuration. This stage creates no schedules on
startup and requires no database migration.

## AppDetails data path

Both versions use the existing scoped Steam transport, bounded to
512 KiB and one Storefront request per Attempt. V1 continues through
the SDK's original typed AppDetails call. V2 uses the SDK's raw call
and preserves the movie and screenshot tokens alongside the other
typed AppDetails facts. It then uses the SDK's existing resolver to
verify the requested AppID and select the original, possibly aliased
envelope entry. The media ID parser reads unsigned decimal numbers
or decimal strings directly. The SDK's native `int` movie ID is
insufficient for the entire uint64 range.

Details and price use the existing fact fields under each explicit
Observation schema. V1 media retains its existing strings, sort
order and limits. V2 media follows the frozen field contract.
[Steam v2 baseline](steam-r0-b-steam-v2-contract.md)

- Named image fields preserve header, capsule, capsule v5, background
  and raw background roles independently.
- A movie with a usable playback source preserves its ID, name,
  thumbnail, optional highlight, zero-based upstream order and all
  valid direct files and manifest variants. A thumbnail-only candidate
  does not represent a playable movie.
- Screenshots preserve ID, upstream order and the available full and
  thumbnail URLs. For duplicate IDs, the first valid physical entry
  wins. Different IDs may share URLs.
- Media links accept HTTPS on approved Steam CDN host boundaries,
  remove query material by default and preserve escaped path meaning.
  URLs with credentials, fragments, non-default ports or over 2,048
  bytes are rejected. Common manifest and direct-file extensions are
  checked against their declared source roles.

Worker budget accounting includes the final JSON segment envelope,
scope and arrays. Base images are reserved first, the first valid
movie and screenshot are considered before the remaining rows, and
each array keeps the upstream order. Each movie is admitted with all
its valid variants. The caps are 12 movies, 30 screenshots and
32 KiB for the whole encoded segment. Candidates beyond a cap set
`truncated: true`. The Server independently checks the normalized
full segment size and shape. An absent usable media projection uses
the existing diagnostic state while preserving useful details and
price.

## Observation and evidence

Current Results, Last Known Good and the Change Stream already key
observations by Output Schema. Each version receives its own
structured facts and history. Hashing reflects scoped semantic data,
excluding observation time, transport-only raw SHA, Evidence ID and
Attempt ID. A replay or a formatting-only upstream change does not
create a new structured result event.

The existing AppID-opt-in Raw Evidence protocol transports original
bytes for either Offer. The server still checks the stored Job, exact
execution key, Attempt, lease and source identity. The evidence scope
and SHA-256 key are unchanged. Identical bytes for v1 and v2 in the
same scope use one raw row with distinct attempt attributions.
Effective v2 schedules also protect the latest valid evidence under
the R0-B retention rules. Evidence archive and Completion retain
their separate transaction and recovery semantics.

[Native and real-sample acceptance](steam-r1-a-acceptance.md)
