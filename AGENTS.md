# Harvest

Work on `develop`. Harvest is an independent, single-module Go data
acquisition engine. Start with the [architecture map](.agents/architecture.md)
and the accepted [A1 baseline](docs/harvest-a1-architecture-engineering-baseline.md).
The design contracts remain in `docs/` and the runtime database rules are in
[contracts/database.md](contracts/database.md).

- Keep GoFurry business models, dependencies, repositories and databases outside Harvest.
- Use one explicit YAML configuration. Keep credentials out of logs, CLI arguments, fixtures derived from real environments and commits.
- Keep Fiber route registration and handlers separate. Give the runtime one shutdown owner and test failed startup and clean shutdown.
- Apply reviewed migrations only with the explicit CLI. Use native Go `uuid.NewV7()` for application entity IDs when introducing entities.
- For migration or GoFurry Management, Admin or Harvest integration changes, record isolated test results, actual development database migration status, and native Windows Server/Worker verification separately. Reuse the existing Task commands and private YAML. Verify the target and a restorable backup before applying a development migration. Startup never applies migrations.
- Preserve Job-first fencing and the separate worker authentication scope. Register synthetic executors only in the integration test binary.
- Route Provider HTTP through the scoped Worker client. Authorize routes and debit shared permits centrally. Keep proxy credentials in Worker YAML.
- Run native `task check` before committing. For destructive verification, use an explicitly isolated test database.

## Native-First Development

- Use native Windows Go/Node and existing explicit private YAML with the configured development infrastructure. Keep ordinary development commands and unit tests native.
- Codex must never invoke Docker, Docker Compose, Podman, Testcontainers or tasks that invoke them on its own. Containers are reserved for an explicitly authorized deployment, production build or container acceptance task. Container fixtures may never substitute for native development integration.
- Apply migrations explicitly after checking the database target and backup. Startup never migrates. Keep fixture resets, destructive tests and batch rebuilds away from persistent development data.
- Report native tests, actual development database status and real local HTTP verification separately. Identify any checks that could not run.
