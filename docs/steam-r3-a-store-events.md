# STEAM-R3-A Store Events

`steam.events:1:1:steam_events_v1` is an explicit Steam App Offer with an hourly minimum. It uses the existing single-request Permit, shared AppDetails Storefront quota and egress profile, and the SDK's fixed GET endpoint at `store.steampowered.com/events/ajaxgetadjacentpartnerevents`. Schedules are created explicitly. Add the Namespace to `steam_events_preview_namespaces`, grant the Worker both the permitted scope and its Events Offer, and configure the Events network source with the AppDetails profile, provider bucket and account bucket. Apply Goose v15 before upgrading the server and update Workers after the server.

## Source contract

| Legacy Collector V2 field | Store Events Observation |
| --- | --- |
| AppID, event and announcement GIDs | Verified AppID; separate unsigned decimal string GIDs; the announcement must reference its event |
| Forum topic, clan | Optional decimal strings for the official topic and clan |
| Title, event kind, article body | Event name and type, headline and complete raw body |
| Original URL | Original upstream URL when provided and a canonical Steam URL computed from the verified AppID/Announcement GID |
| Language | Requested `en` or `zh-cn` plus the announcement's optional Steam language value and match/fallback/different/unknown status |
| Published, start/end and update times | Source publication/visibility flags and supplied post, update, start, end, created and last-modified timestamps |
| Tags, votes, comments | Source tags and nullable counters; zero is retained |
| Raw event and announcement | Original successful HTTP response, including both raw records, when the AppID is allowlisted for evidence and identity checks pass |
| Reviewed HTML, plain text, summary | Built from the stored raw body by GoFurry's shared content review boundary |

The request sends `count_before=1`, `count_after=4` and `lang_list=0` or `6_0`. The scope has a requested locale and trusted egress. Store Events does not supply a Store Country. The batch contains up to five events, bounded per-event diagnostics, and an explicit truncation flag. The body limit is 128 KiB, per-article encoded limit 160 KiB, batch data limit 480 KiB, and raw response limit 512 KiB. An over-budget article is skipped in full. Other valid records in the response remain eligible. HTTP errors and unsuccessful Steam envelopes emit unavailable or invalid evidence. Only a successful envelope with an explicit empty events array establishes an empty page.

Evidence uses the existing staging, SHA-256, Attempt attribution and Operator routes. Details and Events have separate evidence identities and share the existing namespace and global budgets. The Operator must have the appropriate capability grant. The same tracking, version and pruning rules apply within the Events scope. Existing Steam News, AppDetails v1–v3, Assets, Players and their schedules retain their Offers and budgets.

## Source proof

On native Windows, direct source-shape checks returned HTTP 200 for AppIDs 730 and 105600 in both requested languages. Responses were 21,331 / 21,316 bytes for CS2 and 113,161 bytes for Terraria in both languages. The CS2 Chinese request contained two articles declared as Chinese with Chinese body text and two English fallback articles. Terraria returned English for both requests and contained a 42,491-byte article body.

A real CS2 `6_0` response completed through the isolated Harvest Server and Worker, using one Permit on the successful Attempt. The 21,316 original bytes match PostgreSQL SHA-256 and are linked to the successful Attempt. Its four complete articles occupied 9,447 bytes in the persisted JSONB segment. A separate native English Attempt completed with one Permit, matching SHA-256 for 21,331 original bytes and a 9,471-byte Observation containing four English announcements. An earlier bounded batch recorded three upstream timeouts. Direct probes and controlled Worker observations are recorded separately in the GoFurry acceptance report.
