# Database sources

Add reviewed, zero-padded Goose SQL migrations here and named sqlc queries in
`db/queries/` when a feature needs them. Commit generated pgx code from
`task sql:generate`. The binary embeds these migrations so the operator
applies the SQL shipped with that build.

A0 has an empty migration and query set. See the
[database contract](../../contracts/database.md) for the DDL boundary.
