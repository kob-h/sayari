package integration_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"github.com/kob-h/docpipeline/internal/outbox"
	"github.com/kob-h/docpipeline/internal/queue"
	"github.com/kob-h/docpipeline/internal/store"
)

func outboxCount(t *testing.T, ctx context.Context) int {
	t.Helper()
	conn, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n
}

func streamLen(t *testing.T, ctx context.Context, stream string) int64 {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = c.Close() }()
	n, err := c.XLen(ctx, stream).Result()
	if err != nil {
		t.Fatalf("xlen: %v", err)
	}
	return n
}

// The relay publishes outbox rows to the broker and drains them. A message
// written transactionally is delivered exactly once to the stream and removed
// from the outbox.
func TestOutbox_RelayPublishesAndDrains(t *testing.T) {
	ctx := context.Background()
	st := newStore(t) // truncates documents/tokens/outbox
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	broker := queue.NewRedisBroker(redisAddr, "", time.Second, log)
	if err := flushRedis(ctx, broker); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })

	// Enqueue two messages transactionally.
	err := st.WithinTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		if err := tx.EnqueueOutbox(ctx, queue.StreamExtract, "doc-1", []byte(`{"document_id":"doc-1","run_version":1}`)); err != nil {
			return err
		}
		return tx.EnqueueOutbox(ctx, queue.StreamExtract, "doc-2", []byte(`{"document_id":"doc-2","run_version":1}`))
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if c := outboxCount(t, ctx); c != 2 {
		t.Fatalf("expected 2 outbox rows, got %d", c)
	}

	// Run the relay until the outbox drains.
	relay := outbox.New(st, broker, 50*time.Millisecond, log)
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = relay.Run(rctx) }()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && outboxCount(t, ctx) > 0 {
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	<-done

	if c := outboxCount(t, ctx); c != 0 {
		t.Errorf("outbox should be drained, got %d rows", c)
	}
	if l := streamLen(t, ctx, queue.StreamExtract); l != 2 {
		t.Errorf("expected 2 messages published to the stream, got %d", l)
	}
}
