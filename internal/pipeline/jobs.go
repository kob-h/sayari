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

// PublishExtract enqueues an extraction job.
func PublishExtract(ctx context.Context, b queue.Broker, job ExtractJob) error {
	payload, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal extract job: %w", err)
	}
	return b.Publish(ctx, queue.StreamExtract, queue.Message{Key: job.DocumentID, Payload: payload})
}

// PublishClassify enqueues a classification job.
func PublishClassify(ctx context.Context, b queue.Broker, job ClassifyJob) error {
	payload, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal classify job: %w", err)
	}
	return b.Publish(ctx, queue.StreamClassify, queue.Message{
		Key:     strconv.FormatInt(job.TokenID, 10),
		Payload: payload,
	})
}

func decode[T any](payload []byte) (T, error) {
	var v T
	if err := json.Unmarshal(payload, &v); err != nil {
		return v, fmt.Errorf("decode job: %w", err)
	}
	return v, nil
}
