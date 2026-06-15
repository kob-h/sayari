// Package queue defines the message-broker seam between pipeline stages and a
// Redis Streams implementation of it.
//
// The Broker interface is deliberately tiny — publish a message to a stream, and
// consume a stream as part of a consumer group with per-message acknowledgement.
// That is the whole contract the pipeline depends on, which keeps the broker
// swappable: a Kafka or NATS JetStream backing could implement the same
// interface without any change to worker code. Redis Streams is chosen for the
// POC because it provides real broker semantics (consumer groups, acks, and a
// Pending Entries List for crash redelivery) while running as a single trivial
// docker-compose service.
package queue

import "context"

// Stream names used by the pipeline.
const (
	// StreamExtract carries "extract this document" jobs.
	StreamExtract = "pipeline:extract"
	// StreamClassify carries "classify this token" jobs.
	StreamClassify = "pipeline:classify"
)

// Consumer group names. Each stage has exactly one group so that work is
// load-balanced across all replicas of that stage.
const (
	GroupExtract  = "extractors"
	GroupClassify = "classifiers"
)

// Message is a single unit of work on a stream. Key is an optional idempotency /
// debugging hint; Payload is the application-defined body (JSON in this POC).
type Message struct {
	Key     string
	Payload []byte
}

// Handler processes one message. Returning nil acknowledges the message
// (removing it from the stream's pending list). Returning a non-nil error leaves
// the message pending so it is redelivered later — the basis of at-least-once
// delivery and crash recovery.
type Handler func(ctx context.Context, msg Message) error

// Broker is the transport between pipeline stages.
type Broker interface {
	// Publish appends a message to a stream. It is safe for concurrent use.
	Publish(ctx context.Context, stream string, msg Message) error

	// Consume joins consumer group `group` as member `consumer` on `stream` and
	// delivers messages to handler until ctx is cancelled. It also reclaims
	// messages abandoned by crashed consumers (idle longer than the broker's
	// configured threshold) so no work is lost. Consume blocks; run it in its
	// own goroutine.
	Consume(ctx context.Context, stream, group, consumer string, handler Handler) error

	// Ping verifies connectivity to the broker.
	Ping(ctx context.Context) error

	// Close releases broker resources.
	Close() error
}
