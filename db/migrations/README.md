# Database sources

Add reviewed, zero-padded Goose SQL migrations here and named sqlc queries in
`db/queries/`. Commit generated pgx code from `task sql:generate`. The binary
embeds these migrations so the operator applies the SQL shipped with that
build. The [database contract](../../contracts/database.md) defines the DDL
and locking boundaries.
