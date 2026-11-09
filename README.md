# Harvest

Harvest is a standalone Go data acquisition engine. Its durable control plane
manages Targets, schedules and Worker leases, meters outbound HTTP across
Workers, and stores typed, scoped observations. Consumers can read the latest
good results and replay changes or recover through a snapshot. The built-in
Web Provider collects homepage, DNS and verified TLS evidence. Steam remains
planned. Integration tests use isolated PostgreSQL and mock network services.

## Local development

Install Go 1.27.3 and Task 3. Provision a dedicated Harvest database and login
on PostgreSQL 18. Copy [config.example.yaml](config.example.yaml) to
`config.yaml`, set the database endpoint and credentials, and protect the file
with filesystem permissions. The default `verify-full` checks the database
hostname against the system trust store. Set `tls_mode: "disable"` for a
disposable connection when it lacks TLS.

```sh
task install
task migrate ACTION=status
task migrate ACTION=up
task start
```

`task start` and `task dev` run in the foreground. Use Ctrl+C or run
`task stop` from the same checkout. A direct `harvest serve` run is managed
by its process supervisor unless started with `--local`.

```sh
harvest healthcheck --config config.yaml
task check
task check:containers
```

Each runtime reads a single explicit YAML file. Server tasks use
`config.yaml` by default and accept `CONFIG=path/to/file.yaml`. Unknown or
duplicate keys, extra documents and aliases are rejected. YAML secrets stay
local. Each service principal uses a distinct random URL-safe Bearer token
and explicitly lists its namespaces. See the commented example for a
consumer operator. Reader principals can read their scopes, writers can also
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
`control_url`, `token`, `concurrency`, `offers`, and optional `egress`.
Each offer identifies a compiled executable `capability`,
`implementation`, `request_schema` and `output_schema`. The matching server
principal has role `worker` and declares `namespaces`, `capabilities`,
`max_slots`, and optional `egress_profiles`. Plain HTTP control URLs require
a private IP literal. For HTTPS, configure a trusted certificate and private
ingress. Worker readiness is available through
`harvest healthcheck --worker --config config.worker.yaml`. An embedded worker
can use the same YAML shape under `local_worker` with the server's local URL.

For a compiled HTTP capability, grant the named profile to the Worker
principal and initialize the PostgreSQL policy once. After `task migrate
ACTION=up` and before starting the control plane, run `task policy:init`
against server YAML containing a complete `network` block. On upgrade,
import the effective A3 policy to preserve its accounting baseline.
Subsequent starts use PostgreSQL. The legacy block can be removed after
initialization.

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

For an initial schedule, use 24 hours for HTTP and DNS and two to seven
days for TLS. Schedules are created explicitly.

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

## Docker

```sh
task check:docker
docker compose run --rm harvest migrate up --config /etc/harvest/config.yaml
docker compose up -d --wait
docker compose stop
```

Set `http.listen` to `"0.0.0.0:8080"` in the mounted server YAML. The
existing PostgreSQL 18 server must be reachable from the container.
For PostgreSQL on the Docker host, Compose supplies `host.docker.internal`.
Compose publishes server HTTP on host loopback.

`HARVEST_CONFIG`, `HARVEST_IMAGE`, and `HARVEST_HTTP_PORT` configure the bind
mount, image and published port. The optional `worker` profile uses the same
image and `HARVEST_WORKER_CONFIG` (default `./config.worker.yaml`). Mounted
configuration is read-only and must be readable by container UID 65532.
Use an immutable image reference for deployment.

## Database workflow

Goose applies the SQL embedded in the binary through the explicit migration
command. The server checks schema compatibility on startup. See the
[database contract](contracts/database.md) for table ownership, transaction
boundaries and SQL generation.
