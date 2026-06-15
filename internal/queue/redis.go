package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// payloadField is the single stream field under which we store a message body.
const payloadField = "payload"

// RedisBroker implements Broker on top of Redis Streams.
//
// Delivery semantics: at-least-once. A message read by a consumer enters that
// consumer's Pending Entries List (PEL) until XACK'd. If the consumer crashes
// before acking, the message stays in the PEL and is reclaimed by another
// consumer via XAUTOCLAIM once it has been idle past minIdle. Handlers must
// therefore be idempotent.
type RedisBroker struct {
	rdb       *redis.Client
	minIdle   time.Duration
	readCount int64
	blockDur  time.Duration
	log       *slog.Logger
}

// NewRedisBroker connects to Redis at addr and returns a ready Broker.
func NewRedisBroker(addr, password string, minIdle time.Duration, log *slog.Logger) *RedisBroker {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
	})
	return &RedisBroker{
		rdb:       rdb,
		minIdle:   minIdle,
		readCount: 16,
		blockDur:  2 * time.Second,
		log:       log,
	}
}

// Ping verifies connectivity.
func (b *RedisBroker) Ping(ctx context.Context) error {
	return b.rdb.Ping(ctx).Err()
}

// Close closes the underlying client.
func (b *RedisBroker) Close() error {
	return b.rdb.Close()
}

// Publish appends a message to a stream via XADD.
func (b *RedisBroker) Publish(ctx context.Context, stream string, msg Message) error {
	err := b.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{payloadField: msg.Payload, "key": msg.Key},
	}).Err()
	if err != nil {
		return fmt.Errorf("xadd %s: %w", stream, err)
	}
	return nil
}

// ensureGroup creates the consumer group (and the stream, via MKSTREAM) if it
// does not already exist. A pre-existing group is not an error.
func (b *RedisBroker) ensureGroup(ctx context.Context, stream, group string) error {
	err := b.rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("xgroup create %s/%s: %w", stream, group, err)
	}
	return nil
}

// Consume runs the read/reclaim/ack loop until ctx is cancelled.
func (b *RedisBroker) Consume(ctx context.Context, stream, group, consumer string, handler Handler) error {
	if err := b.ensureGroup(ctx, stream, group); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// First, try to reclaim work abandoned by a crashed consumer. This is
		// what makes partial reruns "just work": un-acked messages from a killed
		// worker are picked up here.
		if reclaimed, err := b.reclaim(ctx, stream, group, consumer, handler); err != nil {
			b.log.Warn("reclaim failed", "stream", stream, "err", err)
		} else if reclaimed > 0 {
			continue // prioritise draining the backlog before reading new work
		}

		// Then read newly-arrived messages.
		streams, err := b.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Count:    b.readCount,
			Block:    b.blockDur,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
				continue // no new messages within the block window
			}
			if ctx.Err() != nil {
				return nil
			}
			b.log.Warn("xreadgroup failed", "stream", stream, "err", err)
			time.Sleep(500 * time.Millisecond) // back off on transient broker errors
			continue
		}
		for _, s := range streams {
			for _, m := range s.Messages {
				b.handleOne(ctx, stream, group, m, handler)
			}
		}
	}
}

// reclaim pulls up to readCount idle pending messages from the group's PEL and
// processes them. Returns the number of messages handled.
func (b *RedisBroker) reclaim(ctx context.Context, stream, group, consumer string, handler Handler) (int, error) {
	msgs, _, err := b.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  b.minIdle,
		Start:    "0",
		Count:    b.readCount,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || ctx.Err() != nil {
			return 0, nil
		}
		return 0, fmt.Errorf("xautoclaim %s: %w", stream, err)
	}
	for _, m := range msgs {
		b.handleOne(ctx, stream, group, m, handler)
	}
	return len(msgs), nil
}

// handleOne runs the handler for a single message and acks on success. On
// handler failure the message is left pending for later redelivery.
func (b *RedisBroker) handleOne(ctx context.Context, stream, group string, m redis.XMessage, handler Handler) {
	raw, _ := m.Values[payloadField].(string)
	key, _ := m.Values["key"].(string)
	msg := Message{Key: key, Payload: []byte(raw)}

	if err := handler(ctx, msg); err != nil {
		// Leave the message in the PEL; it will be retried/reclaimed. We do not
		// ack so at-least-once delivery is preserved.
		b.log.Warn("handler failed; leaving message pending",
			"stream", stream, "id", m.ID, "err", err)
		return
	}
	if err := b.rdb.XAck(ctx, stream, group, m.ID).Err(); err != nil {
		// Failure to ack is non-fatal: the message is idempotent and will simply
		// be redelivered and skipped.
		b.log.Warn("xack failed", "stream", stream, "id", m.ID, "err", err)
	}
}
