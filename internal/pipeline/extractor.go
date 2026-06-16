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

// handle is the broker adapter: it decodes the message, runs the service logic
// (Process), and maps domain errors to delivery decisions — a dropped/superseded
// job is acked (return nil), a transient error is retried (return err).
func (w *ExtractionWorker) handle(ctx context.Context, msg queue.Message) error {
	job, err := decode[ExtractJob](msg.Payload)
	if err != nil {
		w.log.Error("undecodable extract job; dropping", "err", err)
		return nil // poison message: ack to avoid an infinite retry loop
	}
	err = w.Process(ctx, job)
	switch {
	case errors.Is(err, domain.ErrNotFound):
		w.log.Warn("extract job for missing document; dropping", "doc", job.DocumentID)
		return nil
	case errors.Is(err, domain.ErrStaleWrite):
		w.log.Info("extraction superseded by newer run; dropping", "doc", job.DocumentID)
		return nil
	case err != nil:
		return err // transient (e.g. DB unavailable): leave pending for retry
	}
	return nil
}

// Process runs the extraction stage for one document: claim it, run the NLP
// extractor, persist the tokens, and fan out one classify job per token. It owns
// the stage-transition decisions (the store only provides primitives). It is
// idempotent — a redelivered job for an already-extracted document is a no-op,
// and re-extraction converges to the same tokens.
func (w *ExtractionWorker) Process(ctx context.Context, job ExtractJob) error {
	doc, err := w.claim(ctx, job.DocumentID)
	if err != nil {
		return err
	}
	switch doc.Status {
	case domain.DocCompleted:
		return nil // already done
	case domain.DocClassifying:
		// Extraction already finished; the classify messages were written to the
		// outbox in the same transaction as the tokens, so they already exist.
		// Nothing to do here.
		return nil
	}

	entities, err := w.extractor.Extract(ctx, doc)
	if err != nil {
		return err
	}

	count, err := w.persist(ctx, doc, entities)
	if err != nil {
		return err
	}
	w.log.Info("extracted document", "doc", doc.ID, "tokens", count, "run", doc.RunVersion)
	return nil
}

// claim transitions a PENDING document to EXTRACTING (stamping the start time) in
// one transaction and returns the current document. A document already past
// PENDING is returned unchanged — its status tells Process whether to proceed.
func (w *ExtractionWorker) claim(ctx context.Context, id string) (domain.Document, error) {
	var doc domain.Document
	err := w.store.WithinTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		d, err := tx.GetDocumentForUpdate(ctx, id)
		if err != nil {
			return err
		}
		doc = d
		if d.Status == domain.DocPending {
			if err := tx.SetExtractionStarted(ctx, id); err != nil {
				return err
			}
			doc.Status = domain.DocExtracting
		}
		return nil
	})
	return doc, err
}

// persist writes the extracted tokens, advances the document's stage, AND
// enqueues one classify job per pending token into the transactional outbox —
// all in one transaction. So the tokens and the messages that will classify them
// commit atomically; the relay publishes the jobs. It recomputes the total from
// the actual rows: zero tokens completes the document immediately, otherwise it
// moves to CLASSIFYING. Writes from a superseded run are fenced with
// domain.ErrStaleWrite. Returns the token count.
func (w *ExtractionWorker) persist(ctx context.Context, doc domain.Document, entities []domain.Entity) (int, error) {
	var count int
	err := w.store.WithinTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		cur, err := tx.GetDocumentForUpdate(ctx, doc.ID)
		if err != nil {
			return err
		}
		if cur.RunVersion != doc.RunVersion {
			return domain.ErrStaleWrite
		}
		if err := tx.UpsertTokens(ctx, doc.ID, doc.RunVersion, entities); err != nil {
			return err
		}
		total, err := tx.CountTokens(ctx, doc.ID, doc.RunVersion)
		if err != nil {
			return err
		}
		count = total
		if total == 0 {
			return tx.MarkExtractionCompletedEmpty(ctx, doc.ID)
		}
		if err := tx.AdvanceToClassifying(ctx, doc.ID, total); err != nil {
			return err
		}
		tokens, err := tx.ListTokensForRun(ctx, doc.ID, doc.RunVersion)
		if err != nil {
			return err
		}
		for _, t := range tokens {
			if t.Status == domain.TokenClassified {
				continue // resumed run: already classified
			}
			if err := EnqueueClassify(ctx, tx, ClassifyJob{
				TokenID:    t.ID,
				DocumentID: doc.ID,
				RunVersion: doc.RunVersion,
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return count, err
}
