// Package pipeline contains the stage orchestration: the extraction and
// classification workers, their job contracts, and the worker runner that scales
// each stage across goroutines and processes. It wires together the store
// (state), the broker (transport), and the NLP/LLM components (compute).
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/kob-h/docpipeline/internal/queue"
	"github.com/kob-h/docpipeline/internal/store"
)

// ExtractJob is the message that asks a worker to extract a document.
type ExtractJob struct {
	DocumentID string `json:"document_id"`
	RunVersion int    `json:"run_version"`
}

// ClassifyJob is the message that asks a worker to classify a single token.
type ClassifyJob struct {
	TokenID    int64  `json:"token_id"`
	DocumentID string `json:"document_id"`
	RunVersion int    `json:"run_version"`
}

// EnqueueExtract writes an extraction job to the transactional outbox using the
// caller's transaction, so it commits atomically with the state change that
// produced it. The relay publishes it to the broker.
func EnqueueExtract(ctx context.Context, tx *store.Tx, job ExtractJob) error {
	payload, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal extract job: %w", err)
	}
	return tx.EnqueueOutbox(ctx, queue.StreamExtract, job.DocumentID, payload)
}

// EnqueueClassify writes a classification job to the transactional outbox using
// the caller's transaction.
func EnqueueClassify(ctx context.Context, tx *store.Tx, job ClassifyJob) error {
	payload, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal classify job: %w", err)
	}
	return tx.EnqueueOutbox(ctx, queue.StreamClassify, strconv.FormatInt(job.TokenID, 10), payload)
}

func decode[T any](payload []byte) (T, error) {
	var v T
	if err := json.Unmarshal(payload, &v); err != nil {
		return v, fmt.Errorf("decode job: %w", err)
	}
	return v, nil
}
