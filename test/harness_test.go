// Package integration_test exercises the whole pipeline end-to-end against real
// Postgres and Redis containers (via testcontainers-go). The API is driven over
// HTTP exactly as a client would, and the extraction/classification workers plus
// the reconciler run in-process. These tests are the executable proof of the six
// required scenarios.
//
// They are skipped under `go test -short` and require a working Docker daemon.
package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/kob-h/docpipeline/internal/api"
	"github.com/kob-h/docpipeline/internal/llm"
	"github.com/kob-h/docpipeline/internal/nlp"
	"github.com/kob-h/docpipeline/internal/pipeline"
	"github.com/kob-h/docpipeline/internal/queue"
	"github.com/kob-h/docpipeline/internal/reconciler"
	"github.com/kob-h/docpipeline/internal/store"
)

// stExec runs a raw SQL statement against the test database using a throwaway
// connection (the store's pool is intentionally unexported).
func stExec(ctx context.Context, _ *store.Store, sql string) (any, error) {
	conn, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, sql)
	return nil, err
}

// flushRedis clears all streams/groups so each test starts clean.
func flushRedis(ctx context.Context, _ *queue.RedisBroker) error {
	c := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = c.Close() }()
	return c.FlushAll(ctx).Err()
}

// shared container endpoints, set up once in TestMain.
var (
	pgDSN     string
	redisAddr string
)

func TestMain(m *testing.M) {
	flag.Parse() // required before testing.Short() is valid
	if testing.Short() {
		// Nothing to set up; individual tests skip themselves.
		os.Exit(m.Run())
	}
	ctx := context.Background()

	pg, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("docpipe"),
		tcpostgres.WithUsername("docpipe"),
		tcpostgres.WithPassword("docpipe"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		panic("start postgres: " + err.Error())
	}
	pgDSN, err = pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		panic("postgres dsn: " + err.Error())
	}

	rd, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		panic("start redis: " + err.Error())
	}
	redisAddr, err = rd.Endpoint(ctx, "")
	if err != nil {
		panic("redis endpoint: " + err.Error())
	}

	code := m.Run()

	_ = pg.Terminate(ctx)
	_ = rd.Terminate(ctx)
	os.Exit(code)
}

// env is a fully wired pipeline for one test.
type env struct {
	t           *testing.T
	store       *store.Store
	broker      *queue.RedisBroker
	server      *httptest.Server
	rootCancel  context.CancelFunc
	classCancel context.CancelFunc
	classDone   chan struct{}
	classDelay  time.Duration
}

func newEnv(t *testing.T, classifyDelay time.Duration) *env {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: needs Docker, skipped with -short")
	}
	ctx, cancel := context.WithCancel(context.Background())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	st, err := store.New(ctx, pgDSN)
	if err != nil {
		cancel()
		t.Fatalf("store: %v", err)
	}
	if err := st.Migrate(ctx); err != nil {
		cancel()
		t.Fatalf("migrate: %v", err)
	}
	// Clean slate so tests are independent.
	if _, err := stExec(ctx, st, `TRUNCATE tokens, documents RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// Short reclaim/reconcile windows so recovery is fast in tests.
	broker := queue.NewRedisBroker(redisAddr, "", 300*time.Millisecond, log)
	if err := flushRedis(ctx, broker); err != nil {
		t.Fatalf("flush redis: %v", err)
	}

	e := &env{t: t, store: st, broker: broker, rootCancel: cancel, classDelay: classifyDelay}

	// Extraction worker + reconciler run for the whole test under the root ctx.
	ext := pipeline.NewExtractionWorker(st, broker, nlp.NewMockExtractor(), 4, log)
	go func() { _ = ext.Run(ctx) }()
	rec := reconciler.New(st, broker, 500*time.Millisecond, log)
	go func() { _ = rec.Run(ctx) }()

	e.startClassifier()

	e.server = httptest.NewServer(api.NewServer(st, broker, log).Handler())

	t.Cleanup(func() {
		e.server.Close()
		cancel()
		st.Close()
		_ = broker.Close()
	})
	return e
}

// startClassifier launches a classification worker under its own context so a
// test can stop it independently (to simulate a crash).
func (e *env) startClassifier() {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	e.classCancel = cancel
	e.classDone = make(chan struct{})
	w := pipeline.NewClassificationWorker(e.store, e.broker,
		llm.NewMockClassifier(llm.WithDelay(e.classDelay)), 4, log)
	go func() {
		defer close(e.classDone)
		_ = w.Run(ctx)
	}()
}

// stopClassifier simulates a classifier crash: cancel it and wait for exit.
func (e *env) stopClassifier() {
	e.classCancel()
	<-e.classDone
}

// --- HTTP helpers -----------------------------------------------------------

type statusBody struct {
	DocumentID string `json:"document_id"`
	Status     string `json:"status"`
	RunVersion int    `json:"run_version"`
	Progress   struct {
		Classified int     `json:"classified"`
		Total      int     `json:"total"`
		Percent    float64 `json:"percent"`
	} `json:"progress"`
	Durations struct {
		ExtractionSeconds     float64 `json:"extraction_seconds"`
		ClassificationSeconds float64 `json:"classification_seconds"`
	} `json:"durations"`
}

func (e *env) process(docID, text, mode string) {
	e.t.Helper()
	body, _ := json.Marshal(map[string]string{"document_id": docID, "text": text, "mode": mode})
	resp, err := http.Post(e.server.URL+"/process", "application/json", bytes.NewReader(body))
	if err != nil {
		e.t.Fatalf("POST /process: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		e.t.Fatalf("POST /process status %d: %s", resp.StatusCode, b)
	}
}

func (e *env) status(docID string) statusBody {
	e.t.Helper()
	resp, err := http.Get(e.server.URL + "/documents/" + docID + "/status")
	if err != nil {
		e.t.Fatalf("GET status: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var s statusBody
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		e.t.Fatalf("decode status: %v", err)
	}
	return s
}

func (e *env) tokenCount(docID, query string) int {
	e.t.Helper()
	resp, err := http.Get(e.server.URL + "/documents/" + docID + "/tokens?" + query)
	if err != nil {
		e.t.Fatalf("GET tokens: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		e.t.Fatalf("decode tokens: %v", err)
	}
	return body.Count
}

// waitForStatus polls until the document reaches want or the timeout elapses.
func (e *env) waitForStatus(docID, want string, timeout time.Duration) statusBody {
	e.t.Helper()
	deadline := time.Now().Add(timeout)
	var last statusBody
	for time.Now().Before(deadline) {
		last = e.status(docID)
		if last.Status == want {
			return last
		}
		time.Sleep(100 * time.Millisecond)
	}
	e.t.Fatalf("timeout waiting for %s to reach %s (last status=%s, %d/%d)",
		docID, want, last.Status, last.Progress.Classified, last.Progress.Total)
	return last
}
