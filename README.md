# Harvest

Harvest is a standalone Go data acquisition engine. Its durable control plane
manages Targets, schedules and Worker leases, meters outbound HTTP across
Workers, and stores typed, scoped observations. Consumers can read the latest
good results and replay changes or recover through a snapshot. The built-in
Web Provider collects homepage metadata, DNS, verified TLS and bounded
discovery data. The Steam Provider collects Storefront details, News and
Current Players. Integration tests use disposable PostgreSQL and local
network fixtures.

## Container packaging

Install Go 1.27.3 on the host PATH. Task sets `GOTOOLCHAIN=local` so a missing
toolchain fails without an automatic download. Run `task build:linux`, then
`docker build -t harvest:local .`. The static artifact is
`bin/linux/amd64/harvest`. For ARM64, build with `ARCH=arm64` and package
with `docker build --platform linux/arm64`. The single-stage image only copies
the binary and licenses into the shared non-root runtime with system CA.
Existing CLI arguments, probes and explicit maintenance commands remain available.
Mount runtime YAML and optional trust files read-only outside the image.
`task check:docker` performs host compilation before packaging.

## Local development

Install Go 1.27.3 and Task 3. Provision a dedicated Harvest database and login
on PostgreSQL 18. Copy [config.example.yaml](config.example.yaml) to
`config.yaml`, set the database endpoint and credentials, and protect the file
with filesystem permissions. The default `verify-full` checks the database
hostname against the system trust store. Set `tls_mode: "disable"` for direct SCRAM on the isolated CN Docker network.

```sh
task install
task migrate ACTION=status
task migrate ACTION=up
task policy:init POLICY_CONFIG=config.policy.yaml
task start
```

`task start` and `task dev` run in the foreground. Use Ctrl+C or run
`task stop` from the same checkout. A direct `harvest serve` run is managed
by its process supervisor unless started with `--local`.

```sh
harvest healthcheck --config config.yaml
task check
```

`task check:containers` runs the isolated Docker integration suite and requires
explicit authorization. Use the configured native services for local integration.

Each runtime reads a single explicit YAML file. Server tasks use
`config.yaml` by default and accept `CONFIG=path/to/file.yaml`. Unknown or
duplicate keys, extra documents and aliases are rejected. YAML secrets stay
local. Each service principal uses a distinct random URL-safe Bearer token
and explicitly lists its namespaces. The example includes an operator,
consumer reader and Worker. Reader principals can read their scopes, writers can also
register Targets and submit work, and operators additionally cancel, retry,
and view operational status.

`GET /livez` checks HTTP responsiveness. `GET /readyz` checks PostgreSQL and
the expected migration version and returns 503 during an outage or drain.
The `healthcheck` CLI probes readiness directly.

## API and workers

The authenticated consumer API is under `/api/v1`. List Targets, Schedules
and Jobs with `namespace` and optional `after` UUID and `limit` (50 default,
200 maximum). Job submission and operator retry require an `Idempotency-Key`
header. The worker protocol is under `/internal/v1`, with separate worker
principals. See the [A1 acceptance guide](docs/harvest-v0.1-a1-acceptance.md)
for the request shapes, schedule rules and test entry points.

A worker uses `harvest worker --config config.worker.yaml` or
`task dev:worker WORKER_CONFIG=path/to/worker.yaml`. Its YAML contains
`node_id`, `control_url`, `token`, `concurrency`, `offers`, and optional `egress`.
Each offer identifies a compiled executable `capability`,
`implementation`, `request_schema` and `output_schema`. The matching server
principal has role `worker` and declares `namespaces`, `capabilities`,
`max_slots`, and optional `egress_profiles`. Plain HTTP control URLs require
a private IP literal or Docker service name. For HTTPS, configure a trusted certificate and private
ingress. Worker readiness is available through
`harvest healthcheck --worker --config config.worker.yaml`. An embedded worker
can use the same YAML shape under `local_worker` with the server's local URL.
For proxy deployment, use the [regional server](config.proxy.example.yaml)
and [regional Worker](config.worker.proxy.example.yaml) examples.

Set a unique, stable `node_id` for every managed Worker. Keep it unchanged
across restarts. It accepts 1–64 lowercase ASCII letters, digits, hyphens and
underscores, starting with a letter or digit. Each start still uses a new
runtime UUID. Omitting `node_id` preserves legacy execution and heartbeat
behavior. Legacy instances do not appear in the node registry. Goose migration
9 creates the registry and audit tables while preserving existing execution rows.

Use `GET /api/v1/operations/worker-nodes` and `GET
/api/v1/operations/worker-nodes/{node_id}` to inspect registered nodes, including
offline nodes. The list returns up to 30 entries and accepts `after` for the
next page. The optional `namespace` restricts access to nodes dedicated to that
namespace. Inspection and control of a shared node require grants for every
namespace and capability it can execute. GoFurry uses the dedicated-namespace
view. For control, an operator posts `state` (`active`, `paused` or `draining`),
`revision` and `request_id` to `/api/v1/operations/worker-nodes/{node_id}/state`.
Pause and drain stop new claims and let existing leases finish. Drain is
complete when the active lease count reaches zero. The desired state survives
offline periods and restarts.

The registry HTTP regression can run on loopback PostgreSQL 18 with an explicit
YAML that points to a migrated, isolated database whose name ends in `_test`.
Set `HARVEST_NATIVE_TEST_DB_CONFIG` to its path and run
`go test ./internal/routers -run '^TestWorkerNodesNative$'`.

For a compiled HTTP capability, grant the named profile to the Worker
principal and initialize the PostgreSQL policy once. After `task migrate
ACTION=up` and before starting the control plane, run `task policy:init`
with a dedicated `POLICY_CONFIG=config.policy.yaml` input copied from
[config.policy.example.yaml](config.policy.example.yaml) (or the proxy variant).
It contains database, principals and network only; the Server rejects network.
Existing initialized databases keep their persisted policy: do not reinitialize.
Subsequent starts use PostgreSQL and never overwrite policy from YAML.

Operators use `GET /api/v1/operations/network-policy` and `PUT` on the same
path. Replacement takes `expected_revision` and the complete `policy`
object returned by GET. The operator must cover all source namespaces and
capabilities in the old and new policies. See the
[A4 acceptance record](docs/harvest-v0.1-a4-acceptance.md) for details.

Worker-local proxy URLs and credentials stay in the
[Worker YAML](config.worker.example.yaml). Workers advertise only logical
paths, regions, health and capacity. Direct fallback requires
`prefer_proxy` with `allow_direct: true` on both sides and a capability that
permits direct access. Supported transports include direct HTTP(S), HTTP(S)
CONNECT and SOCKS5.

Use `GET /api/v1/observations/current` and `/observations/history` for scoped
reads. Operators enroll consumers explicitly. Consumers poll `/changes`,
durably apply the feed, then ACK. See the [A3 acceptance and recovery guide](docs/harvest-v0.1-a3-acceptance.md)
for cursor recovery, retention limits and the integration checks.

## Web acquisition

Configure explicit PostgreSQL source policies for the namespace and each
capability you want to enable. Workers offer implementation `web_v1`,
request schema 1 and output schema 1. Use `scope.egress_region` to select
the configured egress region. For a homepage Job, register a `web.origin`
Target, then submit a request such as the following inside the Job's
`request` object.

```json
{
  "target_id": "<registered-target-uuid>",
  "capability": "web.http",
  "request_schema": 1,
  "params": {},
  "scope": {"egress_region": "cn", "locale": "en"}
}
```

| Capability | Target resource | Parameters | Minimum schedule interval |
| --- | --- | --- | --- |
| `web.http` | HTTP(S) `web.origin` | `{}`. Optional `scope.locale` sets the homepage language preference. | 15 minutes |
| `web.dns` | `web.domain` | `{}` for all eight supported record types, or `{"types":["A","MX"]}`. | 1 hour |
| `web.tls` | HTTPS `web.origin` | `{}` for a verified HTTPS HEAD at the root. | 6 hours |
| `web.robots` | HTTP(S) `web.origin` | `{}` for `/robots.txt`. | 24 hours |
| `web.sitemap` | HTTP(S) `web.origin` | `{}` for `/sitemap.xml`. | 24 hours |
| `web.assets` | HTTP(S) `web.origin` | `{}` for `/favicon.ico`, or `{"kind":"icon","observation_id":"<uuid>","index":0}` for one accepted candidate. `kind` may also be `manifest`. | 24 hours |
| `web.rdap` | `web.domain` | `{}` for the ICANN registrable domain. | 7 days |

For an initial schedule, use 24 hours for HTTP and DNS and two to seven
days for TLS. Start with seven days for robots, sitemap and recurring assets,
and 14–30 days for RDAP. Schedules are created explicitly.

Homepage acquisition starts at the registered origin's root on port 80 or
443. A Job follows up to three same-origin redirects, with one permit per
hop. External redirects are returned as links. Metadata includes bounded
HTML, Open Graph, Twitter Card, JSON-LD and passive fingerprints. HTTPS
homepage responses supply a TLS scope from the same connection. Use
`web.tls` when a certificate-only check is needed.

DNS uses the fixed [Google Public DNS DoH endpoint](https://developers.google.com/speed/public-dns/docs/doh).
Run it only for names your policy permits sending to that recursive
resolver. See its [privacy terms](https://developers.google.com/speed/public-dns/privacy).
Each type consumes one HTTP permit. TTL and AD describe the resolver's
answer. DNS record types, HTML, HTTP and TLS have independently retained
last-known-good values. See the [B1 acceptance record](docs/harvest-v0.1-b1-acceptance.md)
for payload semantics and measured costs.

B2 requires explicit migration 6 and source policies for the capabilities you
enable. Sitemap and assets use a successful, fresh Robots result for the same
Target, namespace and scope. Run `web.robots` first. An observed Robots 404
allows these paths. The control plane defers unknown policy and refuses blocked
paths. Each of these capabilities uses one initial GET and at most two
same-origin redirects, each with its own permit. Sitemap results retain a
bounded sample of links as hints. Homepage RSS and Atom hints use the original
HTML response and require zero extra GETs.

Select an asset by its zero-based index within `kind` in the stored homepage
HTML Observation's `asset_candidates` list. Use the same Target, namespace,
locale and egress scope. Only eligible same-origin candidates can be selected.
The control plane checks the immutable Observation again before issuing a
permit. Run a fresh homepage Job if an older Observation has no candidate data
or has aged out of history. Assets return metadata and digests.

RDAP has one registry GET per Attempt and at most one additional IANA bootstrap
GET on a cold or expired cache. All RDAP sources share one PostgreSQL account
bucket with a maximum of 60 requests/minute, burst 4 and concurrency 2. The
control plane shares its 24-hour bootstrap cache across Workers. See the
[B2 acceptance record](docs/harvest-v0.1-b2-acceptance.md) for bounds and measured costs.

## Steam acquisition

Register a positive uint32 AppID as a `steam.app` Target. Enable the
`steam_v1` Worker offers with request and output schema 1. Select an
`egress_region` and configure the PostgreSQL source policy for each namespace
and capability.

| Capability | Parameters | Suggested explicit schedule |
| --- | --- | --- |
| `steam.details` | `{"language":"schinese","store_country":"CN"}`. Languages are `english` and `schinese`, and store countries are `CN`, `US`, and `HK`. | 24 hours per language and store country |
| `steam.news` | `{}` for ten items or `{"count":5}` for a smaller recent page. | 6–12 hours |
| `steam.players` | `{}`. Use a `point_in_time` schedule for selected apps. | 30–60 minutes |

Store country and locale identify the result independently of the proxy exit.
Details, price and media links have separate current values. Each Attempt
uses at most one GET. News and Players use the public keyless API methods.
The Provider offers a bounded News page and point-in-time Players samples.

Use one shared Storefront provider bucket with `per_minute: 30`, `burst: 1`
and `concurrency: 1` initially. Give News and Players a separate shared
provider bucket and a shared account bucket with a positive `daily_limit`.
The account ledger counts issued permits from midnight UTC independently of
its minute rate. Size the account limit for the upstream allowance and any
use outside this Harvest instance. See the [C acceptance record](docs/harvest-v0.1-c-acceptance.md).

## Operations and verification

Use the [operations runbook](docs/operations.md) for Task and Compose
setup, policy updates, outages, consumer recovery and optional public
smoke testing. `task check:soak` runs the 15-minute local mixed workload.
See the [D acceptance record](docs/harvest-v0.1-d-acceptance.md) for results.

## Database workflow

Goose applies the SQL embedded in the binary through the explicit migration
command. The server checks schema compatibility on startup. See the
[database contract](contracts/database.md) for table ownership, transaction
boundaries and SQL generation.
