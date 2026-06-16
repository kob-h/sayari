// Package service is the application (business) layer for the document API. It
// orchestrates the repository (store) and the broker (queue) and owns the
// business decisions — what to do when a document is submitted, and whether to
// enqueue work — keeping that logic out of both the HTTP handlers and the
// persistence layer.
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/kob-h/docpipeline/internal/domain"
	"github.com/kob-h/docpipeline/internal/pipeline"
	"github.com/kob-h/docpipeline/internal/queue"
	"github.com/kob-h/docpipeline/internal/store"
)

// DocumentService coordinates submission, status, and token queries.
type DocumentService struct {
	store  *store.Store
	broker queue.Broker
	log    *slog.Logger
}

// NewDocumentService constructs a DocumentService.
func NewDocumentService(s *store.Store, b queue.Broker, log *slog.Logger) *DocumentService {
	return &DocumentService{store: s, broker: b, log: log}
}

// SubmitResult is the business outcome of a submit. Accepted reports whether the
// document was (re)queued for processing; FullRerun reports whether a full
// reprocess was performed. These are application concepts, not persistence ones.
type SubmitResult struct {
	Document  domain.Document
	Accepted  bool
	FullRerun bool
}

// Submit applies the business rules for POST /process and, when work is needed,
// publishes the first extract job. The persistence decision runs in a single
// transaction (Unit-of-Work) so the read-modify-write is atomic:
//
//   - new document          -> insert PENDING, accept
//   - mode=full             -> reset (bump run_version), accept, FullRerun
//   - partial on COMPLETED  -> no-op, not accepted (idempotent resubmit)
//   - partial on FAILED     -> move back to PENDING, accept (resume)
//   - partial otherwise      -> accept (resume in place; completed work is kept)
func (s *DocumentService) Submit(ctx context.Context, id, text string, mode domain.RerunMode) (SubmitResult, error) {
	var res SubmitResult

	err := s.store.WithinTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		existing, err := tx.GetDocumentForUpdate(ctx, id)
		switch {
		case errors.Is(err, domain.ErrNotFound):
			doc, err := tx.InsertDocument(ctx, id, text)
			if err != nil {
				return err
			}
			res = SubmitResult{Document: doc, Accepted: true}
		case err != nil:
			return err
		case mode == domain.RerunFull:
			doc, err := tx.ResetDocument(ctx, id, text)
			if err != nil {
				return err
			}
			res = SubmitResult{Document: doc, Accepted: true, FullRerun: true}
		case existing.Status == domain.DocCompleted:
			res = SubmitResult{Document: existing, Accepted: false}
		case existing.Status == domain.DocFailed:
			if err := tx.SetDocumentPending(ctx, id); err != nil {
				return err
			}
			existing.Status = domain.DocPending
			res = SubmitResult{Document: existing, Accepted: true}
		default:
			res = SubmitResult{Document: existing, Accepted: true}
		}

		// Enqueue the first extract job in the SAME transaction as the state
		// change (transactional outbox): if the document is persisted, the message
		// to process it is too — the relay will publish it. No write-then-publish
		// gap.
		if res.Accepted {
			return pipeline.EnqueueExtract(ctx, tx, pipeline.ExtractJob{
				DocumentID: res.Document.ID,
				RunVersion: res.Document.RunVersion,
			})
		}
		return nil
	})
	if err != nil {
		return SubmitResult{}, fmt.Errorf("submit document %s: %w", id, err)
	}
	return res, nil
}

// Status returns a document's manifest, or domain.ErrNotFound.
func (s *DocumentService) Status(ctx context.Context, id string) (domain.Document, error) {
	return s.store.GetDocument(ctx, id)
}

// Tokens returns a document's tokens matching the filter. It returns
// domain.ErrNotFound if the document does not exist, so callers can distinguish
// "no such document" from "no matching tokens".
func (s *DocumentService) Tokens(ctx context.Context, id string, f domain.TokenFilter) ([]domain.Token, error) {
	if _, err := s.store.GetDocument(ctx, id); err != nil {
		return nil, err
	}
	return s.store.ListTokens(ctx, id, f)
}

// Health verifies the backing dependencies are reachable.
func (s *DocumentService) Health(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	if err := s.broker.Ping(ctx); err != nil {
		return fmt.Errorf("broker: %w", err)
	}
	return nil
}
