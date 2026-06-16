package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// OutboxRow is a message awaiting publication to the broker.
type OutboxRow struct {
	ID      int64
	Stream  string
	Key     string
	Payload []byte
}

// EnqueueOutbox records a message to publish, within the current transaction.
// Producers call this inside the same WithinTx as their state change, so the
// state and the "intent to publish" commit atomically — there is no window in
// which the state exists without its message.
func (t *Tx) EnqueueOutbox(ctx context.Context, stream, key string, payload []byte) error {
	if _, err := t.tx.Exec(ctx,
		`INSERT INTO outbox (stream, msg_key, payload) VALUES ($1, $2, $3)`,
		stream, key, payload); err != nil {
		return fmt.Errorf("enqueue outbox: %w", err)
	}
	return nil
}

// ClaimOutboxBatch locks and returns up to limit unpublished rows in id order,
// skipping rows already locked by another relay (FOR UPDATE SKIP LOCKED). The
// caller publishes and deletes them within the same transaction.
func (t *Tx) ClaimOutboxBatch(ctx context.Context, limit int) ([]OutboxRow, error) {
	rows, _ := t.tx.Query(ctx,
		`SELECT id, stream, msg_key, payload FROM outbox
		 ORDER BY id
		 FOR UPDATE SKIP LOCKED
		 LIMIT $1`, limit)
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (OutboxRow, error) {
		var o OutboxRow
		return o, r.Scan(&o.ID, &o.Stream, &o.Key, &o.Payload)
	})
	if err != nil {
		return nil, fmt.Errorf("claim outbox batch: %w", err)
	}
	return out, nil
}

// DeleteOutbox removes a published row.
func (t *Tx) DeleteOutbox(ctx context.Context, id int64) error {
	if _, err := t.tx.Exec(ctx, `DELETE FROM outbox WHERE id=$1`, id); err != nil {
		return fmt.Errorf("delete outbox %d: %w", id, err)
	}
	return nil
}
