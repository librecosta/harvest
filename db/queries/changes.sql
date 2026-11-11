-- name: EnsureChangeHead :exec
INSERT INTO harvest.change_heads (stream,epoch) VALUES ($1,$2) ON CONFLICT (stream) DO NOTHING;

-- name: LockChangeHead :one
SELECT * FROM harvest.change_heads WHERE stream = $1 FOR UPDATE;

-- name: TryLockChangeHead :one
SELECT * FROM harvest.change_heads WHERE stream = $1 FOR UPDATE SKIP LOCKED;

-- name: ChangeHead :one
SELECT * FROM harvest.change_heads WHERE stream = $1;

-- name: AddChangeTotals :exec
UPDATE harvest.change_heads SET last_position = sqlc.arg(position),
    event_bytes = event_bytes + sqlc.arg(event_bytes_delta),
    total_event_bytes = total_event_bytes + sqlc.arg(event_bytes_delta),
    history_rows = history_rows + sqlc.arg(history_rows_delta),
    history_bytes = history_bytes + sqlc.arg(history_bytes_delta)
WHERE stream = sqlc.arg(stream);

-- name: InsertChangeEvent :exec
INSERT INTO harvest.change_events
    (stream,position,id,action,target_id,capability,output_schema,scope_key,tombstone_scope,observation_id,committed_at,stored_bytes)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12);

-- name: ChangeEventPage :many
SELECT * FROM harvest.change_events
WHERE stream = sqlc.arg(stream) AND position > sqlc.arg(after_position)::bigint
  AND position <= sqlc.arg(through_position)::bigint
  AND (coalesce(cardinality(sqlc.arg(capabilities)::text[]),0) = 0 OR capability = ANY(sqlc.arg(capabilities)))
ORDER BY position LIMIT sqlc.arg(page_size);

-- name: DueChangeHeads :many
SELECT stream FROM harvest.change_heads
WHERE last_pruned_at < clock_timestamp() - interval '1 minute' OR history_scan_at IS NOT NULL
  OR last_position-min_position+1 > sqlc.arg(event_rows)::bigint OR event_bytes > sqlc.arg(event_bytes)::bigint
  OR history_rows > sqlc.arg(history_rows)::bigint OR history_bytes > sqlc.arg(history_bytes)::bigint
ORDER BY last_pruned_at,stream LIMIT 16;

-- name: OldestChangeEvents :many
SELECT position,committed_at,stored_bytes FROM harvest.change_events
WHERE stream = $1 ORDER BY position LIMIT 128;

-- name: DeleteChangePrefix :exec
DELETE FROM harvest.change_events WHERE stream = $1 AND position <= $2;

-- name: RecordChangePrune :exec
UPDATE harvest.change_heads
SET min_position = sqlc.arg(min_position),event_bytes = event_bytes - sqlc.arg(event_bytes_removed),
    history_rows = history_rows - sqlc.arg(history_rows_removed),
    history_bytes = history_bytes - sqlc.arg(history_bytes_removed),last_pruned_at = clock_timestamp(),
    history_scan_at = sqlc.narg(scan_at),history_scan_id = sqlc.narg(scan_id)
WHERE stream = sqlc.arg(stream);

-- name: InsertConsumer :one
INSERT INTO harvest.consumers (id,stream,name,principal,epoch,policy)
VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (stream,name) DO NOTHING RETURNING *;

-- name: ConsumerByName :one
SELECT * FROM harvest.consumers WHERE stream = $1 AND name = $2;

-- name: Consumer :one
SELECT * FROM harvest.consumers WHERE id = $1 AND stream = $2 AND principal = $3;

-- name: LockConsumer :one
SELECT * FROM harvest.consumers WHERE id = $1 AND stream = $2 AND principal = $3 FOR UPDATE;

-- name: ChangeEventCommittedAt :one
SELECT committed_at FROM harvest.change_events WHERE stream = $1 AND position = $2;

-- name: StreamConsumers :many
SELECT * FROM harvest.consumers WHERE stream = $1 ORDER BY id;

-- name: StartConsumerReset :one
UPDATE harvest.consumers SET ack_position = CASE WHEN epoch = $2 THEN ack_position ELSE 0 END,
    epoch = $2, policy = $3, reset_id = $4, reset_base = $5,
    reset_bytes = $6, reset_deadline = $7, reset_required = true,reset_reason = 'reset_in_progress',last_activity = clock_timestamp()
WHERE id = $1 RETURNING *;

-- name: AckConsumer :one
UPDATE harvest.consumers SET ack_position = greatest(ack_position,sqlc.arg(position)::bigint),
    reset_required = false,reset_reason = '',reset_id = NULL,reset_deadline = NULL,last_activity = clock_timestamp()
WHERE id = sqlc.arg(id) RETURNING *;

-- name: ExpireConsumers :exec
UPDATE harvest.consumers SET reset_required = true,reset_id = NULL,reset_deadline = NULL,
    reset_reason = CASE
      WHEN epoch <> sqlc.arg(epoch) THEN 'epoch_changed'
      WHEN reset_id IS NOT NULL AND reset_deadline <= sqlc.arg(now_at)::timestamptz THEN 'reset_deadline'
      WHEN reset_id IS NOT NULL AND (reset_base < sqlc.arg(reset_floor)::bigint OR reset_bytes < sqlc.arg(reset_bytes_floor)::bigint) THEN 'reset_budget'
      ELSE 'retention_floor'
    END
WHERE stream = sqlc.arg(stream)
  AND ((epoch <> sqlc.arg(epoch) AND reset_reason <> 'epoch_changed') OR
    (reset_id IS NULL AND NOT reset_required AND ack_position < sqlc.arg(floor_position)::bigint) OR
    (reset_id IS NOT NULL AND (reset_base < sqlc.arg(floor_position) OR reset_deadline <= sqlc.arg(now_at)::timestamptz
      OR reset_base < sqlc.arg(reset_floor)::bigint OR reset_bytes < sqlc.arg(reset_bytes_floor)::bigint)));
