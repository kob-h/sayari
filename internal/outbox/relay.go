// Package outbox implements the relay side of the transactional outbox. It
// drains messages that producers wrote (atomically with their state changes)
// from the Postgres `outbox` table and publishes them to the broker, deleting
// each after a successful publish.
//
// The relay publishes based on "not yet published" (an outbox row exists), not on
// "not yet processed" (state still pending). So it never re-publishes work that is
// merely in flight, and there is no interval to tune against a backlog — draining
// the outbox is cheap and self-limiting. Delivery is at-least-once (a crash
// between publish and delete republishes the row), which idempotent consumers
// already tolerate.
package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/kob-h/docpipeline/internal/queue"
	"github.com/kob-h/docpipeline/internal/store"
)

// Relay polls the outbox and publishes pending messages.
type Relay struct {
	store    *store.Store
	broker   queue.Broker
	interval time.Duration
	batch    int
	log      *slog.Logger
}

// New constructs a Relay that polls every interval.
func New(s *store.Store, b queue.Broker, interval time.Duration, log *slog.Logger) *Relay {
	return &Relay{store: s, broker: b, interval: interval, batch: 200, log: log}
}

// Run drains on a ticker until ctx is cancelled. Each tick drains the backlog to
// empty so a burst of messages is published promptly within one cycle.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	r.log.Info("outbox relay started", "interval", r.interval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.drainAll(ctx)
		}
	}
}

// drainAll publishes batches until fewer than a full batch remains (queue empty).
func (r *Relay) drainAll(ctx context.Context) {
	for {
		n, err := r.drainBatch(ctx)
		if err != nil {
			r.log.Warn("outbox drain failed; will retry next tick", "err", err)
			return
		}
		if n > 0 {
			r.log.Debug("published outbox messages", "count", n)
		}
		if n < r.batch { // drained
			return
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// drainBatch claims a batch, publishes each message, and deletes it — all in one
// transaction. If publishing fails mid-batch the transaction rolls back and the
// rows are retried, so no message is lost (at-least-once).
func (r *Relay) drainBatch(ctx context.Context) (int, error) {
	var n int
	err := r.store.WithinTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		rows, err := tx.ClaimOutboxBatch(ctx, r.batch)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := r.broker.Publish(ctx, row.Stream, queue.Message{Key: row.Key, Payload: row.Payload}); err != nil {
				return err
			}
			if err := tx.DeleteOutbox(ctx, row.ID); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}
