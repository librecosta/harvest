# V0.1-A2 acceptance

Implementation commit `e0188b1bc7c36c05baa2a475882c1798d88b9346`. Run `task check` and
`task check:containers`. The container suite runs A0 through A2 against
separate disposable PostgreSQL 18 projects. A2 runs the compiled fixture in
two independent Worker processes with local HTTP origins and authenticated
HTTP CONNECT and SOCKS5 proxies. HTTPS CONNECT with a trusted test
certificate and refusal of an untrusted certificate are covered by a
transport test.

| Gate | Executed evidence |
| --- | --- |
| Schema and compatibility | Fresh migration, explicit A1 upgrade preserving a Target, refusal before migration, Goose/sqlc reproducibility and the A0/A1 Docker and shutdown regression suites. |
| Route authority | Direct, CN HTTP CONNECT and CN SOCKS5 traffic reached the expected paths. Wrong-region work stayed queued, unauthorized advertisement was rejected, and disabled endpoints could not request permits. Required proxy sent zero direct requests on outage. Preferred direct fallback required both Worker and server opt-in. |
| Safety and connection reuse | Mock proxies accepted only numeric checked destinations. Mixed public/private DNS, metadata targets and redirects were refused. Sensitive headers were stripped on the cross-origin hop. A repeated SDK request reused its tunnel and obtained a new permit. The HTTP/1 transport did not silently replay a failed request on a reused connection. |
| Cross-Worker accounting | Two Workers shared one open response slot and one token per four seconds. The next source request waited at least 3.8 seconds, and deferrals produced no partial debits. The benchmark checked 96 upstream requests against 96 debits. A redirect and an SDK retry each obtained distinct permits. Issue and finish replays were idempotent, changed payloads conflicted, and expiry released concurrency without refunding rate. |
| Failure and lifecycle | 429/401/403 produced account cooldown with a single source request and no alternate proxy. 429 honored bounded Retry-After. Eligible connect failure switched paths. The database outage produced zero source requests. Cancellation and leaked bodies closed connections and acknowledged permits. Killing a Worker mid-request released capacity by expiry, then the surviving Worker recovered the Job. Server drain and Linux Worker SIGTERM preserved active work. |

The server derives global, provider/capability, upstream account and shared
egress buckets from YAML and the stored Job. The compiled
manifest limits requests to approved fixed origins and can require a semantic
region. A source operation is a bounded GET or HEAD. HTTP(S) CONNECT carries a
validated numeric address and preserves origin Host and TLS verification.
SOCKS5 receives the same numeric destination. Active checks use bounded
TCP/TLS reachability to the configured proxy authority. The first request
after a successful check is a single quota-gated trial.

Each redirect, retry or alternative route spends its own permit before
dialing. An individual attempt may try up to three paths for connect failures
when replay is allowed. A permit lasts up to five seconds and cannot outlive
the Attempt lease. Short quota contention waits for up to six seconds in the
current Attempt, then defers through the Job retry budget. Permit rows are
pruned in batches six minutes after expiry. The UTC-day bucket counters
retain outcomes, body bytes and a fixed latency histogram. CONNECT is part of
the permitted round trip, and the Worker scopes reused transports by proxy,
profile, namespace and capability.

## Measured reference

Windows with Docker Desktop, PostgreSQL capped at four vCPUs and 8 GiB,
`GOMAXPROCS=4` per Go process, two network Workers plus the existing idle
embedded Worker. The loaded sample alternated 12-request Jobs through direct
and SOCKS5 routes. Numbers include the control RPCs and Job lifecycle.

| Metric | Observed |
| --- | --- |
| A1/A2 idle PostgreSQL statements per second | 22.49 / 26.59 |
| Loaded SQL and permits | 558.98 statements/s, 96 permits in 5.13 s (18.73/s) |
| Permit RPC latency | p95 11.28 ms, measured at the fixture control-plane relay |
| Pool and locks | 3/4 open, 2 sampled SQL-active, zero additional pool waits and no lock waits in 512 samples |
| WAL and quota tables including indexes | 315,632 WAL bytes, 81,920 bytes of table/index growth |
| Network body bytes | 46,572 permit/finish request bytes, 192 source response bytes |
| RSS and Go heap | Server 22,348 KiB RSS, Workers 20,312 and 18,696 KiB. HeapAlloc idle → loaded was 2.57 → 1.46 MB, 2.49 → 1.54 MB and 1.35 → 2.28 MB respectively. |
| PostgreSQL snapshot after the burst | 6.09% CPU, 47.57 MiB memory |

The PostgreSQL statement count and WAL writes are the main incremental cost
in this sample. Lock sampling can miss waits shorter than 10 ms. The traffic
counts cover control request bodies and consumed source response bodies.
The Worker-local byte budget also reserves space for HTTP metadata. These
counts do not measure physical TLS, CONNECT or SOCKS5 framing. Sustained
mixed load, cold cache, and whole-host 4C8G co-tenancy remain unmeasured.
Production Web and Steam remain `not_implemented`.
