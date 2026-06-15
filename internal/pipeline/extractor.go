package pipeline

import (
	"context"
	"errors"
	"log/slog"

	"github.com/kob-h/docpipeline/internal/domain"
	"github.com/kob-h/docpipeline/internal/nlp"
	"github.com/kob-h/docpipeline/internal/queue"
	"github.com/kob-h/docpipeline/internal/store"
)

// ExtractionWorker consumes extract jobs, runs the NLP extractor, persists the
// resulting tokens, and fans out one classify job per token.
type ExtractionWorker struct {
	store       *store.Store
	broker      queue.Broker
	extractor   nlp.Extractor
	concurrency int
	log         *slog.Logger
}

// NewExtractionWorker constructs an ExtractionWorker.
func NewExtractionWorker(s *store.Store, b queue.Broker, e nlp.Extractor, concurrency int, log *slog.Logger) *ExtractionWorker {
	return &ExtractionWorker{store: s, broker: b, extractor: e, concurrency: concurrency, log: log}
}

// Run blocks until ctx is cancelled, processing extract jobs.
func (w *ExtractionWorker) Run(ctx context.Context) error {
	return worker{
		broker:      w.broker,
		stream:      queue.StreamExtract,
		group:       queue.GroupExtract,
		concurrency: w.concurrency,
		handler:     w.handle,
		log:         w.log,
	}.run(ctx)
}

// handle processes one extract job. It is idempotent: redelivery of an already
// extracted document is a no-op, and re-extraction upserts tokens on their
// natural key so no duplicates are produced.
func (w *ExtractionWorker) handle(ctx context.Context, msg queue.Message) error {
	job, err := decode[ExtractJob](msg.Payload)
	if err != nil {
		w.log.Error("undecodable extract job; dropping", "err", err)
		return nil // poison message: ack to avoid an infinite retry loop
	}

	doc, _, err := w.store.BeginExtraction(ctx, job.DocumentID)
	if errors.Is(err, domain.ErrNotFound) {
		w.log.Warn("extract job for missing document; dropping", "doc", job.DocumentID)
		return nil
	}
	if err != nil {
		return err // transient (e.g. DB unavailable): leave pending for retry
	}

	switch doc.Status {
	case domain.DocCompleted:
		return nil // already done
	case domain.DocClassifying:
		// Extraction already finished; the reconciler ensures classify jobs exist
		// for any still-pending tokens. Nothing to do here.
		return nil
	}

	entities, err := w.extractor.Extract(ctx, doc)
	if err != nil {
		return err
	}

	tokens, err := w.store.SaveExtraction(ctx, doc.ID, doc.RunVersion, entities)
	if errors.Is(err, domain.ErrStaleWrite) {
		w.log.Info("extraction superseded by newer run; dropping", "doc", doc.ID)
		return nil
	}
	if err != nil {
		return err
	}

	for _, t := range tokens {
		if t.Status == domain.TokenClassified {
			continue // resumed run: already classified
		}
		if err := PublishClassify(ctx, w.broker, ClassifyJob{
			TokenID:    t.ID,
			DocumentID: doc.ID,
			RunVersion: doc.RunVersion,
		}); err != nil {
			// Some classify jobs may already be enqueued; returning an error
			// redelivers the extract job. On redelivery the document is in
			// CLASSIFYING and we no-op, while the reconciler enqueues any
			// remaining tokens — so no work is lost.
			return err
		}
	}
	w.log.Info("extracted document", "doc", doc.ID, "tokens", len(tokens), "run", doc.RunVersion)
	return nil
}
