// Package reconciler closes the only consistency gap in the system: the window
// between a Postgres write and the matching Redis publish. If a process crashes
// in that window, the durable state (a PENDING document or token) exists but no
// message is in flight to advance it. The reconciler periodically scans Postgres
// for such orphaned work and re-publishes the jobs. Because every consumer is
// idempotent, re-enqueuing work that is actually still in flight is harmless.
package reconciler

import (
	"context"
	"log/slog"
	"time"

	"github.com/kob-h/docpipeline/internal/pipeline"
	"github.com/kob-h/docpipeline/internal/queue"
	"github.com/kob-h/docpipeline/internal/store"
)

// Reconciler periodically re-enqueues orphaned work.
type Reconciler struct {
	store    *store.Store
	broker   queue.Broker
	interval time.Duration
	batch    int
	log      *slog.Logger
}

// New constructs a Reconciler that scans every interval.
func New(s *store.Store, b queue.Broker, interval time.Duration, log *slog.Logger) *Reconciler {
	return &Reconciler{store: s, broker: b, interval: interval, batch: 200, log: log}
}

// Run scans on a ticker until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	r.log.Info("reconciler started", "interval", r.interval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if n, err := r.reconcileOnce(ctx); err != nil {
				r.log.Warn("reconcile pass failed", "err", err)
			} else if n > 0 {
				r.log.Info("reconciled orphaned jobs", "count", n)
			}
		}
	}
}

// reconcileOnce re-enqueues one batch of orphaned jobs and returns how many.
func (r *Reconciler) reconcileOnce(ctx context.Context) (int, error) {
	orphans, err := r.store.FindOrphans(ctx, r.batch)
	if err != nil {
		return 0, err
	}
	var n int
	for _, o := range orphans {
		switch o.Kind {
		case "extract":
			err = pipeline.PublishExtract(ctx, r.broker, pipeline.ExtractJob{
				DocumentID: o.DocumentID, RunVersion: o.RunVersion,
			})
		case "classify":
			err = pipeline.PublishClassify(ctx, r.broker, pipeline.ClassifyJob{
				TokenID: o.TokenID, DocumentID: o.DocumentID, RunVersion: o.RunVersion,
			})
		}
		if err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}
