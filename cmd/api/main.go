// Command api serves the HTTP API and runs the reconciler control loop. It only
// accepts documents and publishes the first job; all processing happens in the
// worker services, so the API scales independently of throughput.
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
	"github.com/kob-h/docpipeline/internal/reconciler"
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

	// The reconciler runs in the API (control-plane) process. At higher API
	// replica counts it would move to its own deployment; re-enqueues are
	// idempotent so duplicate passes are harmless.
	rec := reconciler.New(st, broker, cfg.ReconcileInterval, log)
	go func() { _ = rec.Run(ctx) }()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.NewServer(st, broker, log).Handler(),
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
