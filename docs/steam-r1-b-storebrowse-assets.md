# Steam StoreBrowse static assets

## Offer and pilot admission

`steam.assets:1:1:steam_assets_v1` runs against a registered Steam App
Target. It has one `assets` Observation segment. A request contains
`language` and `store_country`. The parameters normalize to
`english` or `schinese`, and `US`, `CN` or `HK`. The scope carries
`en` or `zh-cn`, the lower-case Store Country and the actual egress
region pinned by the Attempt.

Upgrade the Server before advertising the new Worker Offer. Configure
a dedicated pilot namespace, with explicit principal and network
policy grants.

```yaml
steam_assets_preview_namespaces: [steam_assets_pilot]
```

The default list is empty. Admission, Run now, retry, due scheduling
and Worker dispatch respect the preview list. Existing accepted
Attempts can finish after a preview change. The minimum recurring
cadence is six hours. Use a daily or slower cadence for ongoing
refresh. One-shot Jobs use the shared Permit budget. Deployment
creates no Schedules. Existing Steam details, news, players and
catalog Offers keep their execution keys and policies.

## Upstream and Permit

The Worker calls the SDK's `StoreBrowseService.GetItemsRaw` once
through the existing Steam transport and scoped egress client.
The fixed route is
`GET https://api.steampowered.com/IStoreBrowseService/GetItems/v1/`.
The SDK constructs the wire request. The Worker and Server derive
its exact attested URI from a single AppID, country and language and
`include_assets=true`. The Attempt permits one GET and at most
512 KiB of JSON. The SDK retries and redirect following are disabled.
A Job retry receives a new fenced Attempt and a new Permit.

StoreBrowse has its own persisted `Network Source`. The existing
policy validation requires it to share the provider and daily account
buckets with configured Steam official API sources such as news and
players. Grants still debit the global and pinned egress buckets.
Storefront keeps its separate quota family. The current verified
route is keyless. The Worker supplies no Steam credentials to
StoreBrowse. Authentication failures are classified as upstream
auth errors. Steamworks distinguishes public methods and those
requiring an authorized key. [Steamworks Web API](https://partner.steamgames.com/doc/webapi_overview)

The bounded response must have one matching item in `response.store_items`,
`item_type=0` and `success=1`. A supplied `appid` and `id` must both
match the requested AppID. `id` alone is accepted only for the
verified Application item type. Explicit invisibility, failed
business status, mixed or mismatched identities, absent assets,
malformed JSON and HTTP failures stay diagnostic. An explicit empty
assets object on a valid visible item can produce `confirmed_empty`.
A response that supplies only unrecognized fields cannot establish
that the asset set is empty.

## Asset references

The ordered candidate list gives priority to Library Capsule, Hero
and Logo, then their 2x variants, then Store capsules, header, hero,
background and Community Icon. These are distinct vendor roles.
The SDK documents `logo` and `logo_2x` as alternative slots for
`library_logo` and `library_logo_2x`. The original selected Slot is
retained. Store raw background uses the vendor `raw_page_background`
field. Community Icon uses the SDK builder with a verified 40-hex
hash returned in `community_icon`. The current mapping handles 17
roles. Community Logo stays outside the mapping until Steam supplies
a verifiable Slot and path.

Every row contains the SHA-256 asset ID, asset type and family,
vendor Slot, canonical URL, locale, `store_browse` source and
observation time. An optional source digest is extracted from a
verified path component or Community Icon hash. It describes the
source path. The Worker does not retrieve image bytes. The stable ID
hashes AppID, vendor Slot, locale and canonical URL with NUL
separators. Sharing a URL does not merge distinct roles. A changed
Slot or URL changes the corresponding source identity.

The resolver accepts one terminal `${FILENAME}` or `%s` placeholder,
a valid image filename or hash/image pair, and the reviewed SDK CDN
host names. A supplied absolute URL must pass the same check. HTTPS,
the default port, no credentials or fragment, a bounded image path
and the exact AppID segment are mandatory. Path checks reject dot
segments, encoded separators, double escapes, invalid UTF-8 and
controls before preserving legal escaping. Queries are removed.
Community uses the exact SDK Community CDN host and AppID/hash JPG
path. Both acquisition and Completion validate the URL. URL validity
attests the reference and its AppID attribution. There is no image
availability probe or download.

## Observation and compatibility

The Worker admits up to 32 complete assets in priority order and
measures the complete encoded segment against 32 KiB. Excluded
valid candidates set `truncated=true`. The Server validates order,
shape, source ID, locale, URL and the encoded limit independently.
A singular source diagnostic leaves Last Known Good intact. An
explicit empty is a confirmed result in its own scope. Asset
observation time is persisted and excluded from the semantic
projection. Same-content collections confirm an existing result.
A changed source reference writes a new revision and Change Event.

StoreBrowse uses its own Observation capability. AppDetails Raw
Evidence tables, capture and retention continue to handle AppDetails
only. The pilot stream is separate from the existing GoFurry
consumer stream. [Native and real-source acceptance](steam-r1-b-acceptance.md)
