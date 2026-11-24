# STEAM-R2-A · Steam AppDetails v3

## Offer and source identity

`steam.details:1:3:steam_v3` is a separate Offer with the same Request Schema 1. A Job or Schedule must specify `output_schema: 3`, and its Namespace must be listed in Server YAML under `steam_rich_details_preview_namespaces`. The list defaults to empty. An eligible Worker must advertise the exact v3 Offer. The Worker uses the existing Steam Permit, AppDetails Origin, egress policy and 512 KiB response bound. One attempt makes one upstream request. The existing AppDetails resolver verifies the inner AppID and handles an outer key alias. Job and Attempt identity still govern optional Raw Evidence attribution.

| Section | Contract |
| --- | --- |
| `details` | Existing facts including name, application type, developers, publishers, release and summary |
| `price` | Existing quote status, currency and amounts |
| `media` | R1 Media v2, with the existing 32 KiB Segment limit |
| `rich_details` | Source text, contacts, requirements and typed Steam facts |

## Legacy field coverage

The audit used Collector V2 on `dev` at `27d4c1db`, including its Steam mapper, scheduled collector, domain specification and the storage contract. It also checked the pinned steam-go AppDetails, language, release and markup APIs.

| Collector V2 field | AppDetails source | R2 projection |
| --- | --- | --- |
| AppID, type, name, developers, publishers | `steam_appid`, `type`, `name`, `developers`, `publishers` | Stable Job/Target identity and the existing `details` facts |
| Localized long description, about and short description | `detailed_description`, `about_the_game`, `short_description` | Private source text, reviewed HTML and optional plain-text editorial replacements |
| Website and support | `website`, `support_info` | Website, support URL and email |
| Minimum and recommended requirements | `pc_requirements`, `mac_requirements`, `linux_requirements` | Separate Windows, macOS and Linux entries |
| Platform flags and controller support | `platforms`, `controller_support` | Optional platform flags and controller text |
| Free flag, release and upcoming status | `is_free`, `release_date` | Free flag, original release date text and optional coming-soon flag |
| Content descriptors and ratings | `content_descriptors`, `ratings`, `required_age` | Descriptor IDs and notes, board keys, rating values and ages |
| Localized language list | `supported_languages` | Original text and SDK-parsed language code, names, API/Web codes, tier, known status and source-backed audio flags |

Interface and subtitles are unknown for AppDetails. An unmarked language has unknown audio when Steam supplies no usable audio support legend. Parsing preserves unknown language names. The requested Store locale is provenance for the request. It supplies no evidence about interface language or the language of the returned prose.

## State, bytes and validation

A missing or null optional field is omitted. A verified empty string stays empty. Steam requirement slots can also return an empty array. A malformed optional field is diagnosed in `invalid`. A field beyond its own limit or beyond the Segment's combined budget is omitted in full and listed under `truncated`. The retained `rich_details` remains independently useful. Upstream failures and identity failures keep the existing unsuccessful Segment states.

`rich_details` is capped at 384 KiB serialized. Its JSON data has a 1 KiB envelope reserve. Each long text has a 192 KiB JSON budget, the full short description and raw language list each have 16 KiB, and each platform requirements object has 20 KiB. Small facts, contacts, requirements and the language list take priority when the combination exceeds the limit. Worker Completion retains its 2 MiB default. Persisted Observation validation repeats the identity, field and encoded-byte checks. The v14 migration raises the Attempt receipt limit from three to four Observation IDs.

Raw Evidence keeps the original response (including optional malformed fields), subject to the existing opt-in size, identity and retention policies. A fallback SDK decode can omit a malformed optional field while the original JSON remains the source of diagnoses. GoFurry applies one HTML review boundary for Admin and approved public snapshots. Harvest emits no public HTML.

## Windows acceptance

Native Worker, internal HTTP, PostgreSQL 18, Permit accounting and exact-byte Raw Evidence attribution succeeded for the three archived response inputs. The test observed one fixture HTTP request per Attempt and confirmed that a v1-only Worker cannot claim a v3 Job.

| Steam source/request sample | Original Body | Details / Price / Media / Rich Details, bytes |
| --- | ---: | --- |
| Paws 434000, en/HK | 16,934 | 1,041 / 204 / 5,794 / 7,101 |
| Stray 1332010, en/CN | 15,663 | 1,121 / 204 / 5,358 / 6,809 |
| Hades 1145360, en/US | 21,692 | 1,223 / 203 / 6,184 / 11,423 |

Paws and Stray used exact bytes from previous successful Steam evidence. Hades used a single new successful public AppDetails HTTP request from the local environment. The native Worker replay used a controlled TLS fixture fed those bytes. The Worker tests and native `task check` passed. The shared development Harvest database was explicitly migrated from 13 to 14 after verifying a restorable backup.

A separate direct native Hades v3 Job (en/US) also succeeded against Steam itself. It issued one Permit, produced four present Observations and captured 21,692 bytes. The archive's SHA-256 matched the captured input, and GoFurry's test publication references the Observation from that attributed Attempt. A separate Stray zh-CN direct Attempt hit an upstream timeout and produced only unavailable states. The prior Stray en/CN evidence was replayed successfully.
