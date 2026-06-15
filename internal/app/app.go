// Package app provides shared process bootstrap: structured logging, dependency
// construction (store, broker, classifier) from config, and signal-driven
// graceful shutdown. The three service binaries (api, extractor, classifier)
// each compose these helpers, keeping their main functions tiny.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kob-h/docpipeline/internal/config"
	"github.com/kob-h/docpipeline/internal/llm"
	"github.com/kob-h/docpipeline/internal/queue"
	"github.com/kob-h/docpipeline/internal/store"
)

// NewLogger returns a JSON structured logger tagged with the service name.
func NewLogger(service string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})).
		With("service", service)
}

// SignalContext returns a context cancelled on SIGINT/SIGTERM for graceful
// shutdown.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// NewStore connects to Postgres and runs migrations, retrying briefly so a
// service started alongside its database (docker-compose) waits for it to be
// ready instead of crash-looping.
func NewStore(ctx context.Context, cfg config.Config, log *slog.Logger) (*store.Store, error) {
	var s *store.Store
	err := retry(ctx, 10, time.Second, log, "postgres", func() error {
		st, err := store.New(ctx, cfg.PostgresDSN)
		if err != nil {
			return err
		}
		s = st
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := s.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

// NewBroker connects to Redis, retrying briefly for startup ordering.
func NewBroker(ctx context.Context, cfg config.Config, log *slog.Logger) (*queue.RedisBroker, error) {
	b := queue.NewRedisBroker(cfg.RedisAddr, cfg.RedisPassword, cfg.ClaimMinIdle, log)
	err := retry(ctx, 10, time.Second, log, "redis", func() error {
		return b.Ping(ctx)
	})
	if err != nil {
		return nil, err
	}
	return b, nil
}

// NewClassifier builds the configured classifier: the deterministic mock, or the
// real Ollama-backed implementation.
func NewClassifier(cfg config.Config, log *slog.Logger) llm.Classifier {
	if cfg.LLMProvider == "ollama" {
		log.Info("using Ollama classifier", "model", cfg.OllamaModel, "base_url", cfg.OllamaBaseURL)
		return llm.NewOllamaClassifier(cfg.OllamaBaseURL, cfg.OllamaAPIKey, cfg.OllamaModel, log)
	}
	log.Info("using mock classifier", "delay", cfg.MockClassifyDelay)
	return llm.NewMockClassifier(llm.WithDelay(cfg.MockClassifyDelay))
}

func retry(ctx context.Context, attempts int, delay time.Duration, log *slog.Logger, what string, fn func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		log.Warn("dependency not ready; retrying", "dependency", what, "attempt", i+1, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return fmt.Errorf("%s not ready after %d attempts: %w", what, attempts, err)
}
