// Package config loads process configuration from the environment. Every service
// (api, extractor, classifier) reads the same Config so wiring stays uniform and
// nothing is hardcoded.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all runtime settings, populated from environment variables.
type Config struct {
	// PostgresDSN is the libpq connection string for the state store.
	PostgresDSN string
	// RedisAddr is the host:port of the Redis broker.
	RedisAddr string
	// RedisPassword is optional; empty means no auth.
	RedisPassword string

	// HTTPAddr is the listen address for the API server.
	HTTPAddr string

	// LLMProvider selects the classifier implementation: "mock" or "ollama".
	LLMProvider string
	// MockClassifyDelay adds artificial per-token latency to the mock classifier.
	// Zero by default; set it (e.g. 50ms) to simulate real LLM latency so
	// progress and partial-rerun behaviour are observable in a demo.
	MockClassifyDelay time.Duration
	// Ollama* are only consulted when LLMProvider == "ollama".
	OllamaBaseURL string
	OllamaAPIKey  string
	OllamaModel   string

	// WorkerConcurrency is how many in-flight messages a single worker process
	// handles concurrently.
	WorkerConcurrency int
	// ClaimMinIdle is how long a pending (un-acked) message must be idle before
	// another consumer may reclaim it via XAUTOCLAIM.
	ClaimMinIdle time.Duration
	// ReconcileInterval is how often the reconciler re-enqueues orphaned work.
	ReconcileInterval time.Duration
	// ShutdownTimeout bounds graceful shutdown.
	ShutdownTimeout time.Duration
}

// Load reads configuration from the environment, applying sane local-development
// defaults so the stack runs with zero required env vars (mock LLM).
func Load() (Config, error) {
	c := Config{
		PostgresDSN:       env("POSTGRES_DSN", "postgres://docpipe:docpipe@localhost:5432/docpipe?sslmode=disable"),
		RedisAddr:         env("REDIS_ADDR", "localhost:6379"),
		RedisPassword:     env("REDIS_PASSWORD", ""),
		HTTPAddr:          env("HTTP_ADDR", ":8080"),
		LLMProvider:       env("LLM_PROVIDER", "mock"),
		MockClassifyDelay: envDuration("MOCK_CLASSIFY_DELAY", 0),
		OllamaBaseURL:     env("OLLAMA_BASE_URL", "https://ollama.com"),
		OllamaAPIKey:      env("OLLAMA_API_KEY", ""),
		OllamaModel:       env("OLLAMA_MODEL", "gpt-oss:20b"),
		WorkerConcurrency: envInt("WORKER_CONCURRENCY", 8),
		ClaimMinIdle:      envDuration("CLAIM_MIN_IDLE", 30*time.Second),
		ReconcileInterval: envDuration("RECONCILE_INTERVAL", 15*time.Second),
		ShutdownTimeout:   envDuration("SHUTDOWN_TIMEOUT", 20*time.Second),
	}
	if c.LLMProvider == "ollama" && c.OllamaAPIKey == "" {
		// Hosted Ollama requires a key; fail fast rather than 401 on every token.
		return Config{}, fmt.Errorf("LLM_PROVIDER=ollama requires OLLAMA_API_KEY")
	}
	if c.WorkerConcurrency < 1 {
		return Config{}, fmt.Errorf("WORKER_CONCURRENCY must be >= 1, got %d", c.WorkerConcurrency)
	}
	return c, nil
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
