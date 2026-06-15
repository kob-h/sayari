package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"golang.org/x/sync/errgroup"

	"github.com/kob-h/docpipeline/internal/queue"
)

// worker runs a single broker handler across `concurrency` consumer goroutines,
// each joining the same consumer group under a distinct consumer name so Redis
// load-balances messages among them. This is the unit of horizontal scaling:
// run more goroutines per process, or more processes — the consumer group
// distributes work either way.
type worker struct {
	broker      queue.Broker
	stream      string
	group       string
	concurrency int
	handler     queue.Handler
	log         *slog.Logger
}

func (w worker) run(ctx context.Context) error {
	host, _ := os.Hostname()
	if host == "" {
		host = "worker"
	}
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < w.concurrency; i++ {
		consumer := fmt.Sprintf("%s-%s-%d", host, w.group, i)
		g.Go(func() error {
			if err := w.broker.Consume(ctx, w.stream, w.group, consumer, w.handler); err != nil {
				return fmt.Errorf("consumer %s: %w", consumer, err)
			}
			return nil
		})
	}
	w.log.Info("worker started", "stream", w.stream, "group", w.group, "concurrency", w.concurrency)
	return g.Wait()
}
