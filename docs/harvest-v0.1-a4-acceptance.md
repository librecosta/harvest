# Harvest V0.1-A4 acceptance

## Operator flow

Apply the bundled migrations with `task migrate ACTION=up`. For network
execution, run `task policy:init` once while the control plane is stopped,
using server YAML with the effective legacy `network` block. Use
`harvest policy init --config config.yaml` directly or through Compose for
the same one-time import. The DB login authorizes initialization. The command
preserves existing quota accounting and rejects a second initialization.

`GET /api/v1/operations/network-policy` returns the canonical document,
revision, routing revision, checksum, update time and actor. To edit, send
`PUT` to the same path with `expected_revision` and the full `policy` object.
A stale revision returns 409 `policy_revision_conflict`. Reordering entries
leaves the revision unchanged. Only an operator covering the old and new
source scopes can replace the global document. Scoped operations status
includes the revision and status.

Server startup reads the policy from PostgreSQL. The legacy YAML block is
migration input. With no policy row, the control API reports
`not_configured` and network-only Worker readiness returns 503.
Worker proxy endpoint URLs, credentials, pool behavior and principal
grants remain in local YAML.

Logical source and proxy identities keep their quota bindings after removal.
A routing edit advances a shared routing revision, so in-flight network
Attempts from the earlier revision must reclaim before obtaining another
permit. Existing permits can finish. Numeric edits retain the routing
revision and apply the updated limits to the next permit.

## Executed acceptance

Tests used Go 1.27.3 and PostgreSQL 18.6 in disposable Docker projects.
The fixtures used two independent Worker processes, local origins,
authenticated HTTP CONNECT and SOCKS5. The A2 transport tests also cover
HTTPS CONNECT certificate validation.

- Upgrading populated A3 preserved Target, Job, Attempt, Worker, current
  Observation, Change Stream and live quota accounting. An A3-issued
  permit finished through the new server after initialization.
- Server startup with the legacy block left the policy absent. Explicit
  init preserved the existing cooldown and counters. Restart with the
  unchanged legacy YAML retained the edited revision, limits and SOCKS5
  route. Both remote Workers stayed running.
- Reader, writer, scoped operator and Worker mutations were denied.
  Invalid references, unsupported capabilities, secret-bearing fields and
  quota reassignment were rejected. A canonical no-op retained its
  timestamp. Two concurrent CAS writers produced one success and one
  conflict. A deferred PostgreSQL error at commit preserved the prior
  policy and balance.
- A rate increase settled exactly at the old rate up to the database
  timestamp. A burst reduction clamped existing tokens. Lowering
  concurrency with two live requests deferred a racing permit while both
  requests finished. Compatible Attempts continued under new accounting.
  A database gate exercised policy, status, claim, register, heartbeat,
  permit and finish concurrently under the Go race detector.
- A CONNECT Attempt held across a route edit obtained zero fresh grants
  on the old route. A bounded retry completed through the other Worker via
  SOCKS5. Removing the matching path left the Job waiting with zero
  attempts and zero direct requests. Restoring the path resumed safely.
  The A2 regression also verified zero direct requests during required
  proxy outage and cooldown on 429/401/403.

`task check`, `go test -race ./...`, the A4 race integration test, the
A0–A4 `task check:containers` suite, and the Linux ARM64 image build
passed locally.

## Measured impact

The A2-shaped run used the same two Worker roles, 96 round trips,
`GOMAXPROCS=4`, and a PostgreSQL 4 CPU / 8 GiB container. The A2 numbers
come from its recorded run. Each column is one run on Windows with Docker
Desktop.

| Metric | A2 record | A4 run |
| --- | ---: | ---: |
| Idle / loaded SQL statements per second | 26.59 / 558.98 | 24.79 / 570.81 |
| Permits per second | 18.73 | 18.86 |
| Permit RPC p95 | 11.28 ms | 12.20 ms |
| Open / max pool connections | 3 / 4 | 3 / 4 |
| Additional pool waits / sampled lock waits | 0 / 0 | 1 / 14 |
| WAL insert bytes | 315,632 | 315,976 |

The policy table received zero queries during idle and loaded phases.
One uncontended quota-changing PUT took 5.88 ms in the live A4 test.
The 10 ms lock sampler can miss shorter waits. These short runs measure
the fixture, with sustained mixed load and whole-host contention remaining
unmeasured.

V0.1 assumes a single active control plane. Routing edits deliberately
invalidate the prior routing revision across network Attempts. The central
binding ledger relies on stable logical endpoint IDs and can store up to
2,048 bindings.
