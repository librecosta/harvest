# V0.1-D acceptance

## Readiness checklist

- [x] Direct and regional proxy example pairs pass server policy and Worker
  registration validation for all ten Web and Steam capabilities.
  Use the [operations runbook](operations.md) for credentials, migration,
  one-time policy initialization, a single control plane and supervised
  shutdown. Start with a limited, authorized target set.
- [x] The disposable PostgreSQL 18.6 integration suite covers startup
  rollback, Worker crash and takeover fencing, proxy failure with zero
  direct fallback, PostgreSQL outage and recovery, and two Compose
  stop/start cycles with clean shutdown. A4 and C exercise active policy
  CAS, Permit replay, UTC quota reset, and Storefront 429/403 cooldown.
- [x] The test consumer uses the authenticated HTTP API and a different
  disposable PostgreSQL database with its own login. The fixture verified
  that login was denied a connection to Harvest's database. The adapter
  writes scoped versions and tombstones and activates the staged
  projection and cursor in one transaction before ACK.
- [x] A paged reset converged while results were inserted, updated and
  withdrawn. The proof includes independent Web locales, two Steam
  language/country pairs, a News page changing to confirmed empty and
  repeated zero Players samples. Partial scopes preserved prior good
  values. Closing and reopening the consumer pool before ACK, duplicate
  application and retrying the durable cursor all converged. The adapter
  retains the latest sample per scope.
- [x] Age cleanup and hard row and estimated-byte pressure preserved
  current and retained-event pins. Aged cursors returned 410 and a fresh
  reset converged. Status warned when the next unacked event was within
  24 hours of age eligibility. The A3 regressions also exercise reset
  deadline and byte exhaustion.
- [x] The public smoke Task skipped without opt-in. The same one-Attempt
  harness used exactly one Storefront Permit through a local fake CONNECT
  proxy. Public Web and Steam requests were **not run**.
- [ ] Real public-source availability, production proxy behavior and exit
  IP sharing, upstream allowance, shared-host capacity and backup/PITR
  recovery drills on a deployment environment remain unverified.

D is ready for a **limited operator-controlled deployment** with the
runbook's budgets and monitoring. The measurements describe isolated local
fixtures.

## Executed verification

Windows passed `task check`, `go test -race ./...`, the D fixture with
`-race`, the C-to-D binary transition, `task check:soak`, and Compose
validation plus Linux amd64 and arm64 builds. The A0–D container suite
passed in 557 s. Its time budget is 12 minutes with the added fixture.

The C upgrade proof seeds the same disposable database using an integration
binary compiled from `2c8e0a25788960356f42774812dbdeeaaadbe7c2`, switches to
the D control plane after an explicit no-op `migrate up`, and checks Targets,
Schedules, Jobs, Attempts, observations, current rows, events, Permit
counters and policy revision. The original C Workers continue against D.
There is no new migration. Reproduce by passing the absolute C integration
binary path as `HARVEST_D_C_FIXTURE` to `TestD`.

The production binary dependency list includes `steam-go v1.3.12` and
`x/time v0.14.0`. SDK-only modules such as A2S, VDF and Bluemonday are
outside that compiled dependency list. The source tree and Docker image
carry the SDK MIT and x/time BSD notices. Go module sums, the toolchain
and image digests remain pinned. The existing GitHub Actions workflow
tracks major Action tags.

## Mixed workload and retention

Windows amd64 (Ryzen 7 5800H), Go 1.27.3, disposable PostgreSQL 18.6
capped at 4 CPUs and 8 GiB, two one-slot Worker processes, and an idle
embedded fixture Worker. Other isolated checks shared the host during
part of the paced run. The run sent one batch every 15 seconds.

| Local measurement | Short final fixture with race detector | Paced native soak |
| --- | ---: | ---: |
| Mixed phase | 13.79 s | 15 min 4.38 s |
| Jobs / issued Permits / deferrals | 21 / 24 / 0 | 427 / 488 / 0 |
| Idle before load / loaded SQL statements per second | 25.25 / 270.23 | 26.15 / 91.29 |
| Permit / claim / completion RPC p95 | 13.60 / 11.70 / 21.71 ms | 61.31 / 61.25 / 64.62 ms |
| Queue / Attempt p95 | 4.31 s / 53.31 ms | 4.25 s / 192.63 ms |
| WAL insert bytes | 535,272 | 7,659,272 |
| Heap / index / TOAST growth, KiB | 72 / 8 / 0 | 504 / 328 / 0 |
| PostgreSQL connections / max consumer lag | 5 / 5 events | 5 / 5 events |

The soak issued 61 permits each for Web HTTP, TLS, Robots, Steam Details,
News and Players, plus 122 DoH permits. HTTP CONNECT and SOCKS5 each
carried 244 permits. Their tunnels sent/received 150,429/425,970 and
119,824/130,433 bytes respectively. The short fixture moved Players to
SOCKS5 so two Workers could publish into one stream. It observed zero
additional control-plane pool waits and zero lock waits in 137 samples
at 100 ms intervals. The soak observed zero locks at its batch checkpoints.

| Paced resource snapshot | Before load | After load | After idle |
| --- | ---: | ---: | ---: |
| Server / Worker A / Worker B RSS, KiB | 23,360 / 21,932 / 17,420 | 25,304 / 23,392 / 22,664 | 25,308 / 23,392 / 22,664 |
| Server / Worker A / Worker B Go heap, MiB | 2.42 / 1.54 / 1.51 | 2.65 / 2.77 / 2.62 | 2.74 / 1.40 / 1.04 |
| PostgreSQL Docker CPU / memory | 0.96% / 118.1 MiB | 6.27% / 141.8 MiB | 6.31% / 141.6 MiB |

The paced window added 16.45/0.69/0.55 OS CPU seconds to the three Go
processes. After aging disposable rows, bounded prune passes took 153 ms.
They reduced 447 Jobs and 447 Attempts to zero, 99 Observations to 21
pinned current rows, and 99 Change Events to 8 retained events. Targeted
`VACUUM ANALYZE` took 12 ms and left allocated pages available for reuse.
No hot-path tuning was supported by these samples.

Run `task check:containers` for the short fixture or `task check:soak`
for the paced 15-minute workload. The load issues Web HTTP, two DoH
questions, TLS, Robots, and Steam Details, News and Players, with
consumer poll and ACK. SQL includes the scheduling and polling
background activity and excludes observer queries. WAL is a PostgreSQL
insert-LSN delta. Table heap, indexes and TOAST are physical allocation
deltas. Container CPU/RSS and Go process memory are snapshots. Lock
sampling can miss short waits. The paced results do not establish a
saturation limit.

No production services, keys or data were used. The separately
guarded [public smoke procedure](operations.md#local-checks-and-public-smoke)
requires an approved source and operator-owned proxy.
