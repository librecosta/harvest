# Harvest operations

## Bring up one control plane

Use Go 1.27.3, Task 3, a dedicated PostgreSQL 18 database and a dedicated
login. Keep one active control plane for that database. Run one or more
independent Workers. Start with one namespace and a small number of
authorized targets.

1. Copy the [direct server](../config.example.yaml) and
   [direct Worker](../config.worker.example.yaml) examples to *config.yaml*
   and *config.worker.yaml*. For an operator-owned proxy, use the
   [regional server](../config.proxy.example.yaml) and
   [regional Worker](../config.worker.proxy.example.yaml) pair. Set the
   actual endpoint and semantic exit region. In a private network, direct
   mode describes the Worker-to-source route. Web targets still must pass
   public IP and destination checks.
2. Replace every credential placeholder. Use distinct random URL-safe
   operator, reader and Worker tokens, and give each runtime only its own
   YAML. Use mode 0600 on Unix or an equivalent restricted Windows ACL.
   Keep server and Worker control traffic on loopback or private ingress.
   Use trusted HTTPS for remote control traffic and verify PostgreSQL
   TLS hostname and trust. The examples use `verify-full`. Use
   `tls_mode: disable` only for an isolated disposable database.
3. Check `task migrate ACTION=status` and explicitly apply
   `task migrate ACTION=up`. On a fresh database, run `task policy:init`
   once. Run `task start` and, in a second terminal, `task dev:worker`.
   A separately supervised Worker can use `harvest worker --config
   config.worker.yaml`. A local deployment can also embed one Worker by
   putting the Worker configuration under the server's `local_worker` key
   with the same loopback control URL and Worker grant.
4. Check `/livez`, `/readyz` and `harvest healthcheck --worker --config
   config.worker.yaml`. Register one Web Origin and one Steam App, submit
   a Job for each and inspect their scoped Current results before
   enabling explicit schedules. Use the [request and cadence tables](../README.md#web-acquisition)
   and [Target, Job and schedule API examples](harvest-v0.1-a1-acceptance.md).

`/livez` answers for the HTTP process. `/readyz` verifies PostgreSQL and
the expected schema and changes to 503 during drain or database loss.
A successful readiness response does not check upstream reachability.
A Worker also needs a matching policy and healthy eligible path to claim
network work. Use the same probes for uptime monitoring.

### Docker Compose

Build with `task check:docker` or set `HARVEST_IMAGE` to an immutable
image reference. Set the mounted server YAML listener to
`0.0.0.0:8080`. Compose binds the host port to loopback. It expects an
existing PostgreSQL 18 instance. If the database runs on the Docker host,
`host.docker.internal` can address it when the database bind settings,
firewall and certificate allow that name. A containerized Worker needs
a private control URL reachable from its own network namespace. Use an
authenticated TLS ingress or a private IP literal for plaintext HTTP.
The server loopback address points back to the Worker in a separate
container.

```sh
docker compose run --rm harvest migrate up --config /etc/harvest/config.yaml
docker compose run --rm harvest policy init --config /etc/harvest/config.yaml
docker compose up -d --wait harvest
docker compose --profile worker up -d --wait
docker compose --profile worker stop
```

Run the initialization line only on a fresh database. The optional Worker
profile mounts `HARVEST_WORKER_CONFIG` (default `./config.worker.yaml`).
`HARVEST_CONFIG` and `HARVEST_HTTP_PORT` select the server YAML and host
port. Keep the mounts read-only and readable by UID 65532 without
granting unrelated users access. Use Ctrl+C or `task stop` for a local
Task-owned server. Compose sends SIGTERM to its services. Leave the
configured grace periods so the control plane can stop admission, finish
in-flight receipts and close PostgreSQL.

## Policy, quotas and outages

After initialization, use an operator token covering all affected source
namespaces and capabilities. GET `/api/v1/operations/network-policy`,
edit the full returned `policy` and PUT
`{"expected_revision":<current revision>,"policy":<complete policy>}`.
On 409, GET the new revision and reconcile the change. PostgreSQL
supplies the policy at startup. The bootstrap YAML can be removed once
initialization succeeds.

- Keep proxy URLs and userinfo in the Worker YAML. The central policy
  contains path IDs, modes, regions and bucket references. Keep the same
  logical path and bucket IDs when a credential rotates. `require_proxy`
  fails closed. An explicit `prefer_proxy` fallback requires
  `allow_direct: true` in the policy and Worker and a compatible capability.
  Use a trusted proxy that dials the approved numeric address. Enforce the
  corresponding destination restrictions on the proxy host.
- Allocate Web, Storefront and official Steam quotas separately, with a
  shared global and actual-exit budget. For Storefront start at
  `per_minute: 30`, `burst: 1`, `concurrency: 1`. AppDetails is an
  observed Storefront interface. That setting is an operator choice
  informed by one historical experiment. Start with daily Details,
  News every 6–12 hours, and Players every 30–60 minutes for selected
  apps. Count each store country and language as a separate request.
- The two official Steam methods are public and keyless. Set
  `daily_limit` on their common account bucket from the actual upstream
  allowance and other traffic using it. PostgreSQL counts issued permits
  since UTC midnight, including grants with no successful response.
  A separate token bucket controls the minute rate. Harvest measures only
  its own traffic. Allow for callers outside Harvest and proxies sharing
  an outbound IP. [Valve Web API terms](https://steamcommunity.com/dev/apiterms)
- Inspect `/api/v1/operations/status?namespace=...`, the Job waiting reason
  and Worker's eligible paths. `daily_quota_exhausted` waits for the UTC
  budget. `upstream_cooldown` follows upstream 429, authorization refusal
  or a recognized block. Respect the cooldown. `no_healthy_egress` and
  `egress_policy_unavailable` warrant checking grants, configured routes,
  proxy reachability and policy revision. `retry_backoff` keeps a bounded
  retry deferred. Check the Job deadline before using explicit retry.
- Every on-wire request uses a Permit of at most five seconds, including
  the response body. Slow origins or proxies can produce timeouts and
  incomplete segments. Keep independently good scopes and inspect the
  attempt reason before adjusting capacity. Web DNS uses a fixed Google
  DoH resolver. RDAP uses a 24-hour in-memory IANA bootstrap cache that
  starts cold after restart. Web fingerprints are passive evidence.
  Storefront prices and release text can be absent or localized, News is
  a bounded page, and Players is a point-in-time sample.
- When PostgreSQL is down, leave liveness alone and wait for readiness
  after restoration. Monitor queued Jobs and expired Attempts. A restarted
  Worker claims fresh leases and old completions are fenced. After a
  route or numeric quota edit, check revision, active permits, cooldown
  and pending reasons before increasing load.

Back up the Harvest database and policy before changes. Stop Workers
and the control plane, check the backup, migrate explicitly with the
intended build, then restart and check readiness, policy revision and
consumer position. Practice restoration to a separate database first.
Restoring behind a previously delivered stream head requires rotating
the affected stream epochs while the control plane is offline. Use a fresh
UUIDv7 for each affected head and preserve its restored head and floor.
After restarting, verify old cursors receive 410 and rebuild each consumer
from a fresh reset before normal delivery.

## Consumer recovery and storage

Enroll a reader-owned consumer as the operator with `POST /api/v1/consumers`.
The reader starts a reset, stages `/snapshots/current` pages, and replays
`/changes` from the reset baseline through the final page's head. Store
the version by target, capability, output schema and scope key. Apply
only newer positions, including tombstones. Publish the projection and
its cursor together in the consumer database, then ACK. Persist any
pending reset token so a restart can retry the ACK. On HTTP 410, start a
fresh reset and replace the old projection only after staging and replay
succeed. See the [A3 protocol walkthrough](harvest-v0.1-a3-acceptance.md).

By default, terminal Job/Attempt and Observation history age at 30 days
and Change Events at 14 days. Per-stream defaults are 100,000 events or
256 MiB and 200,000 history rows or 512 MiB of estimated data. A reset has
10 minutes and up to 2,048 events or 32 MiB of replay protection. Current
values and retained event references pin Observations. Pressure can move
the replay floor earlier. Check `max_consumer_lag`, `min_available`,
`expired_consumers` and `replay_age_cutoff`. The
`consumer_replay_window_low` warning means the oldest unacked event is
within 24 hours of age eligibility or lag has reached 80% of the event
row budget. Catch up and ACK promptly. `retention_pressure` takes
priority when limits are already exceeded. Monitor physical table,
index, TOAST and WAL sizes separately from logical budgets, and review
autovacuum and backup retention on the database host.

## Local checks and public smoke

`task check:containers` uses disposable PostgreSQL, two Worker processes
and local Web, DoH, Steam and proxy fixtures. `task check:soak` is an
opt-in 15-minute local run. Both work without public acquisition or
production credentials.

`task check:public-smoke` skips by default. Only an operator with approval
to use the target and proxy should run it. Supply one authorized target,
a protected Worker YAML using one enabled regional `require_proxy`
endpoint, and `CONFIRM=authorized`. The test provisions its own
disposable database and generated service tokens. It reads the proxy
settings from the supplied file and runs one Job with one Attempt.
A Web Job can use up to four permits including same-origin redirects.
Storefront can use one. The fixture allows 40 seconds for the Job and
two minutes for the invocation. It reports sanitized state, reason,
latency and permit count. Stop and review any refusal, throttle or
incomplete result before authorizing another run.

```sh
task check:public-smoke CONFIRM=authorized SOURCE=web TARGET="<approved-https-origin>" WORKER_CONFIG="<protected-worker.yaml>"
task check:public-smoke CONFIRM=authorized SOURCE=steam TARGET="<approved-appid>" LANGUAGE=english COUNTRY=US WORKER_CONFIG="<protected-worker.yaml>"
```

Use a purpose-built test database for a follow-up comparison of locale
and region or LKG across runs. Preselect the exact few scopes, keep
global and source ceilings in place, confirm Host/SNI and region with
the approved proxy and review stored result scopes. A small successful
run establishes reachability for that sample. Public rate ceilings,
production proxy failure modes and sustained behavior require separate
validation.
