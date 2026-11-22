# Steam v2 data contract baseline

Frozen for R1–R3 implementation. This document defines new wire shapes and
release gates. The current Harvest registry serves `steam.details` Request
Schema 1 and Output Schema 1 with the `steam_v1` Offer.

## Shared identity and representation

All Steam facts carry the verified positive uint32 AppID, source surface,
normalized locale, Store Country where applicable, pinned egress region and
observation time. AppID identity comes from the resolved response data. The
Storefront envelope key may be an alias. Language and store market identify
the source independently of the actual network exit. Source evidence can
be linked internally by its scoped ID and digest. Raw SHA-256 reflects exact
source bytes. Semantic change detection compares normalized facts and
excludes acquisition timestamps, evidence IDs and raw digests.

When an ID might exceed JavaScript's safe integer range, encode the exact
unsigned decimal digits as a JSON string. Parse the original number token or
digit string without using a floating-point intermediate. Keep IDs stable across
reordering and locales when the upstream ID is stable. Use original zero-based
array order separately. Preserve field presence when it carries meaning.

| Presence | Representation |
| --- | --- |
| Acquisition could not establish a value | `{"state":"unknown"}` |
| Successful source object omitted the key | `{"state":"missing"}` |
| Source explicitly sent null | `{"state":"null"}` |
| Source explicitly supplied a value | `{"state":"present","value":...}` |
| Explicit empty or negative | Present with `""`, `[]`, `{}`, zero or `false` as allowed by the field |

## R1 AppDetails media

Keep capability `steam.details`, the existing params and the details, price
and media scopes. Introduce the explicit Offer `steam_v2` with Request Schema 1
and Output Schema 2. Details and price may initially carry the v1 fact shape
under the new Offer. The media segment uses the following wire fields.

| Object | Fields and source mapping |
| --- | --- |
| Media segment | `app_id` uint32, `images` object, `movies` array, `screenshots` array, `truncated` boolean. Source and scope use the existing Observation envelope. |
| Images | Optional `header_url` ← `header_image`, `capsule_url` ← `capsule_image`, `capsule_v5_url` ← `capsule_imagev5`, `background_url` ← `background`, `background_raw_url` ← `background_raw`. Each label keeps its upstream role. |
| Movie | `movie_id` positive decimal string from `id`, `name` string, optional `thumbnail_url` from `thumbnail`, optional boolean `highlight`, `order` zero-based unsigned integer, and `sources` object. |
| Movie sources | Optional `mp4_480`, `mp4_max`, `webm_480`, `webm_max` from upstream `mp4` and `webm` keys `480` and `max`. Optional `dash_av1`, `dash_h264`, `hls_h264` retain their manifest roles. |
| Screenshot | `screenshot_id` nonnegative decimal string from `id`, optional `thumbnail_url` from `path_thumbnail`, optional `full_url` from `path_full`, `order` zero-based unsigned integer. |

Use known official HTTPS CDN hosts with checked hostname suffix boundaries,
default port and no user info or fragment. Cap each URL at 2,048 bytes and
movie names at 256 UTF-8 bytes. Remove query material by default. A source
whose playback depends on query parameters needs an explicit reviewed
vendor allowlist and rules for excluding authorization material. Preserve
escaped path meaning. Each variant keeps its field role even when no direct
file is present. Thumbnail, HLS and DASH remain image or manifest fields.
Keep original order, and resolve duplicate upstream IDs by preserving the
first valid entry and its order. Conflicting identities make only the
affected item unavailable.

The R1 pilot targets up to 30 screenshot and 12 movie entries. Measure the
complete normalized wire segment against the existing 32 KiB limit, including
escaped strings and container fields. Preserve full variants for retained
entries. Reserve room for base images and metadata, prioritize the first
movie with a usable source and the first valid screenshot, then admit each
kind in upstream order under the measured budget. Mark `truncated` when a
count or byte limit omits valid data. When the source cannot be validated,
use the existing segment evidence states. R0-B's offline tests measure real
and padded media cases against the unchanged cap.

## R1 extended static assets

`steam.assets` will have its own Offer (Request 1, Output 1), scoped to one
verified AppID, one locale and store country per Job. Its source candidate
is `IStoreBrowseService/GetItems/v1` with one AppID and the asset section
requested. Verify the returned item type and AppID. Each asset records
`asset_id`, `asset_type`, `asset_family`, `url`, `locale`, `source_surface`
and `observed_at`. Use a stable vendor item/asset ID when present. Otherwise
derive identity from AppID, named vendor asset slot, locale and validated
canonical URL. Define `store`, `library`, and `community` families and
retain the vendor slot for the concrete header, capsule, background, logo
or icon type. Assemble an official URL only from an attested host/template
and a bounded validated path. AppDetails keeps its single Storefront request.

The pilot budget is one HTTPS GetItems request per Attempt, at most 512 KiB
of response, at most 32 structured assets and the existing 32 KiB segment
cap, with an independent policy and permit. Verify the public route in R1
before enabling its Offer. Start with a keyless public request. If Steam
requires an approved credential, use a private Worker configuration and
the matching account quota, with credential handling at the existing
verified transport boundary. Steamworks distinguishes public and protected
methods. [Steamworks Web API](https://partner.steamgames.com/doc/webapi_overview?l=english)
· [Steamworks asset families](https://partner.steamgames.com/doc/store/assets)

## R2 full details

Under a versioned details segment, apply the presence table to these facts.

| Field | Contract |
| --- | --- |
| `detailed_description`, `about_the_game` | Bounded source HTML with an explicit untrusted media type. Derive separately bounded display-safe text when needed. Raw source rendering stays outside the display path. |
| `website_url`, `support.url`, `support.email` | Source values with independent URL or address validation. Restrict website and support links to web schemes. |
| `requirements` | `windows`, `macos`, `linux`, each with separately present `minimum` and `recommended` source markup/text. Preserve unsupported-platform evidence and upstream empty-object variations distinctly. |
| `languages` | Bounded rows with normalized language code, source label and independently present `interface`, `subtitles`, `audio` booleans. A marked supported-language string alone cannot assert all three. |
| `required_age`, `ratings`, `content_descriptors` | Integer age when known, region/board-keyed ratings with vendor code and descriptors, source descriptor IDs and bounded notes. |
| `release` | Original localized `date_text` and independent `coming_soon` boolean. Only supply a parsed calendar date when an exact date can be established. |

R2 measures long text and requirements content against the existing Segment
budget before admitting the Offer. When a full source field exceeds the
structured budget, keep its authoritative original in evidence, mark the
structured projection as bounded and keep presence distinct from truncation.
Publish versioned schema fixtures before enabling any shape that extends the
R1 field set.

## R3 Store Events

`steam.events` is an independent Offer (Request 1, Output 1).
An event contains `event_gid`, optional `announcement_gid` and
`forum_topic_id` as exact decimal strings, verified AppID, locale,
`source_surface`, `source_version`, source observation time, UTC
`published_at` and optional `updated_at`, bounded source body and separately
sanitized display body, bounded summary and tags, and presence-aware
`comments` and `interactions` counts. Identity is AppID, event GID and
locale. Order delivery using the existing Harvest cursor semantics and
derive revisions from normalized source facts. Declare a separate endpoint
budget and credential scope after validating the event source contract.
`steam.news` continues to report official News API records.

## Compatibility and promotion

| Participant | Contract |
| --- | --- |
| Existing v1 Worker and Schedule | Use the unchanged v1 Offer and v1 details, price and media schemas. |
| New Worker | Advertises v2 explicitly. The scheduler and Permit path route by execution key. Concurrent v1 and v2 execution requires explicit matching Offers. |
| Existing Observation and streams | Keep schema identity/version and existing compatibility. v1 `string[]` media keeps its type and behavior. |
| GoFurry Consumer in R1 | Add explicit v1/v2 decoding and source precedence. Exercise replay and snapshot with mixed versions before production rollout. Raw hashes remain evidence-only. |
| GoFurry editing and publishing | Treat source media as proposals. Existing editor choices and publication remain explicit operations. |

R1 can reuse R0-A's verified snapshots, R0-B's authorized downloads and
the offline media coverage and sizing tests. Acceptance requires reviewed
schema fixtures, coexistence and consumer tests and measured segment sizes
before enabling v2.
