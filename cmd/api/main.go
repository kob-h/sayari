// Command api serves the HTTP API and runs the outbox relay. It only accepts
// documents (writing the doc and its first job to the outbox in one transaction);
// all processing happens in the worker services, so the API scales independently
// of throughput.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/kob-h/docpipeline/internal/api"
	"github.com/kob-h/docpipeline/internal/app"
	"github.com/kob-h/docpipeline/internal/config"
	"github.com/kob-h/docpipeline/internal/outbox"
	"github.com/kob-h/docpipeline/internal/service"
)

func main() {
	log := app.NewLogger("api")
	cfg, err := config.Load()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, stop := app.SignalContext()
	defer stop()

	st, err := app.NewStore(ctx, cfg, log)
	if err != nil {
		log.Error("store", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	broker, err := app.NewBroker(ctx, cfg, log)
	if err != nil {
		log.Error("broker", "err", err)
		os.Exit(1)
	}
	defer func() { _ = broker.Close() }()

	// The outbox relay runs in the API (control-plane) process: it publishes
	// messages that producers wrote transactionally with their state changes. At
	// higher replica counts it can move to its own deployment; FOR UPDATE SKIP
	// LOCKED lets multiple relays share the outbox safely.
	relay := outbox.New(st, broker, cfg.OutboxPollInterval, log)
	go func() { _ = relay.Run(ctx) }()

	svc := service.NewDocumentService(st, broker, log)
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.NewServer(svc, log).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("api listening", "addr", cfg.HTTPAddr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server", "err", err)
		os.Exit(1)
	}
	log.Info("api stopped")
}
