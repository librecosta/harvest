# Harvest

Harvest is a standalone Go data acquisition engine. V0.1-A0 provides the
server, PostgreSQL connectivity, operations CLI and verification workflow.
The design for collection and durable execution lives in the
[A1 architecture baseline](docs/harvest-a1-architecture-engineering-baseline.md).

## Local development

Install Go 1.27.3 and Task 3. Provision a dedicated Harvest database and login
on PostgreSQL 18. Copy [config.example.yaml](config.example.yaml) to
`config.yaml`, then set the endpoint, database name, user and password.
Protect the YAML with your filesystem permissions. TLS verifies the database
hostname using the system trust store by default. Use `tls_mode: "disable"`
only for a disposable or otherwise trusted connection.

```sh
task install
task migrate ACTION=status
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

Commands that use application settings require an explicit `--config` path.
Task supplies `config.yaml` by default and accepts `CONFIG=path/to/file.yaml`.
Only a single YAML mapping is accepted. Keys are case-sensitive and unknown
keys, duplicate keys, extra documents and aliases are rejected. Environment
variables and dotenv files cannot override application settings.

`GET /livez` checks HTTP responsiveness. `GET /readyz` checks the database
with a one-second deadline and returns 503 during an outage or drain. The
`healthcheck` CLI probes readiness directly, with no proxy or redirects.

## Docker

```sh
task check:docker
docker compose up -d --wait
docker compose stop
```

Set `http.listen` to `"0.0.0.0:8080"` in the mounted YAML. Use the existing
PostgreSQL 18 server's reachable address from the container. For PostgreSQL on
the Docker host, Compose supplies `host.docker.internal`. The host must allow
connections from the Docker network. Compose publishes HTTP on host loopback.

Compose uses `./config.yaml` by default. `HARVEST_CONFIG`, `HARVEST_IMAGE`
and `HARVEST_HTTP_PORT` customize the bind mount, image and published port.
The mount is read-only and the file must be readable by container UID 65532.
Use an immutable image reference for deployment.

## Database workflow

`task migrate ACTION=up` explicitly applies the Goose migrations embedded in
that build. A0 ships an empty migration set. See the
[database contract](contracts/database.md) for schema ownership and SQL
generation, and the [A0 acceptance checklist](docs/harvest-v0.1-a0-acceptance.md)
for what the disposable verification environment exercises.
