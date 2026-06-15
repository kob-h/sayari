package pipeline

import (
	"context"
	"errors"
	"log/slog"

	"github.com/kob-h/docpipeline/internal/domain"
	"github.com/kob-h/docpipeline/internal/llm"
	"github.com/kob-h/docpipeline/internal/queue"
	"github.com/kob-h/docpipeline/internal/store"
)

// ClassificationWorker consumes classify jobs, runs the LLM classifier, and
// atomically persists the result while advancing document progress.
type ClassificationWorker struct {
	store       *store.Store
	broker      queue.Broker
	classifier  llm.Classifier
	concurrency int
	log         *slog.Logger
}

// NewClassificationWorker constructs a ClassificationWorker.
func NewClassificationWorker(s *store.Store, b queue.Broker, c llm.Classifier, concurrency int, log *slog.Logger) *ClassificationWorker {
	return &ClassificationWorker{store: s, broker: b, classifier: c, concurrency: concurrency, log: log}
}

// Run blocks until ctx is cancelled, processing classify jobs.
func (w *ClassificationWorker) Run(ctx context.Context) error {
	return worker{
		broker:      w.broker,
		stream:      queue.StreamClassify,
		group:       queue.GroupClassify,
		concurrency: w.concurrency,
		handler:     w.handle,
		log:         w.log,
	}.run(ctx)
}

// handle processes one classify job. It is idempotent: an already-classified
// token is a no-op (so a redelivered message never double-counts progress), and
// writes from a superseded run are dropped.
func (w *ClassificationWorker) handle(ctx context.Context, msg queue.Message) error {
	job, err := decode[ClassifyJob](msg.Payload)
	if err != nil {
		w.log.Error("undecodable classify job; dropping", "err", err)
		return nil
	}

	tok, err := w.store.GetToken(ctx, job.TokenID)
	if errors.Is(err, domain.ErrNotFound) {
		// The token was removed by a full rerun; the job is obsolete.
		return nil
	}
	if err != nil {
		return err
	}
	if tok.Status == domain.TokenClassified {
		return nil // idempotent: already classified
	}

	result, err := w.classifier.Classify(ctx, tok)
	if err != nil {
		// Includes LLM rate-limit/timeout after retries: leave the message
		// pending so it is redelivered and retried later.
		return err
	}

	err = w.store.ApplyClassification(ctx, job.TokenID, job.RunVersion, result)
	switch {
	case errors.Is(err, domain.ErrStaleWrite), errors.Is(err, domain.ErrNotFound):
		return nil // superseded by a newer run, or token gone: drop
	case err != nil:
		return err
	}
	return nil
}
