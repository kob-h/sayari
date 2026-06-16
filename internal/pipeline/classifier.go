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

// handle is the broker adapter: decode, run the service logic (Process), and map
// domain errors to delivery decisions (drop vs retry).
func (w *ClassificationWorker) handle(ctx context.Context, msg queue.Message) error {
	job, err := decode[ClassifyJob](msg.Payload)
	if err != nil {
		w.log.Error("undecodable classify job; dropping", "err", err)
		return nil
	}
	err = w.Process(ctx, job)
	switch {
	case errors.Is(err, domain.ErrNotFound), errors.Is(err, domain.ErrStaleWrite):
		return nil // token gone (full rerun) or superseded run: drop
	case err != nil:
		return err
	}
	return nil
}

// Process classifies one token and records the result, advancing the document's
// progress. It owns the stage decisions (idempotent skip, stale-run fencing, and
// completion when the last token is classified); the store only provides
// primitives. Persisting the result and advancing the counter happen in one
// transaction, so progress can never diverge from the classified rows.
func (w *ClassificationWorker) Process(ctx context.Context, job ClassifyJob) error {
	// Cheap pre-check before the (potentially slow) LLM call: skip work already done.
	tok, err := w.store.GetToken(ctx, job.TokenID)
	if err != nil {
		return err // ErrNotFound (token removed by full rerun) bubbles to handle
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
	return w.apply(ctx, job.TokenID, job.RunVersion, result)
}

// apply persists the classification and advances progress in one transaction,
// owning the completion decision. It re-checks under row locks so concurrent or
// redelivered attempts are safe: an already-classified token is a no-op (no
// double count), and writes from a superseded run are fenced.
func (w *ClassificationWorker) apply(ctx context.Context, tokenID int64, runVersion int, result domain.Classification) error {
	return w.store.WithinTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		tok, err := tx.GetTokenForUpdate(ctx, tokenID)
		if err != nil {
			return err
		}
		if tok.RunVersion != runVersion {
			return domain.ErrStaleWrite
		}
		if tok.Status == domain.TokenClassified {
			return nil // idempotent under lock: do not double-count
		}

		// Lock the document and fence a superseded run at the document level too.
		doc, err := tx.GetDocumentForUpdate(ctx, tok.DocumentID)
		if err != nil {
			return err
		}
		if doc.RunVersion != runVersion {
			return domain.ErrStaleWrite
		}

		if err := tx.SetTokenClassified(ctx, tokenID, result); err != nil {
			return err
		}
		newCount, err := tx.IncrementClassifiedCount(ctx, tok.DocumentID)
		if err != nil {
			return err
		}
		if newCount >= doc.TotalTokens {
			if err := tx.MarkDocumentCompleted(ctx, tok.DocumentID); err != nil {
				return err
			}
		}
		return nil
	})
}
