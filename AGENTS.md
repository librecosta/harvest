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
- Preserve Job-first fencing and the separate worker authentication scope. Register synthetic executors only in the integration test binary.
- Run `task check` before committing. Run `task check:containers` when changing config, database connectivity, lifecycle or Docker. Use the disposable fixture for destructive verification.
