-- name: NetworkPolicy :one
SELECT * FROM harvest.operational_policies WHERE name = 'network';

-- name: LockNetworkPolicy :one
SELECT * FROM harvest.operational_policies WHERE name = 'network' FOR UPDATE;

-- name: InitNetworkPolicy :execrows
INSERT INTO harvest.operational_policies (name,revision,routing_revision,policy,bindings,checksum,updated_at,updated_by)
VALUES ('network',1,1,$1,$2,$3,clock_timestamp(),'cli_init')
ON CONFLICT (name) DO NOTHING;

-- name: ReplaceNetworkPolicy :one
UPDATE harvest.operational_policies
SET revision = revision + 1, routing_revision = sqlc.arg(routing_revision),
    policy = sqlc.arg(policy), bindings = sqlc.arg(bindings), checksum = sqlc.arg(checksum),
    updated_at = clock_timestamp(), updated_by = sqlc.arg(updated_by)
WHERE name = 'network' RETURNING *;

-- name: SettleQuotaBalances :exec
UPDATE harvest.quota_buckets b
SET balance = (sqlc.arg(balances)::bigint[])[d.ord], refilled_at = greatest(b.refilled_at, sqlc.arg(now_at)::timestamptz)
FROM unnest(sqlc.arg(keys)::text[]) WITH ORDINALITY AS d(key, ord)
WHERE b.key = d.key;
