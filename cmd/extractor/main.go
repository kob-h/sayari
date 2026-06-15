// Command extractor runs the NLP extraction stage. It can be scaled to N
// replicas; the Redis consumer group load-balances documents across them.
package main

import (
	"os"

	"github.com/kob-h/docpipeline/internal/app"
	"github.com/kob-h/docpipeline/internal/config"
	"github.com/kob-h/docpipeline/internal/nlp"
	"github.com/kob-h/docpipeline/internal/pipeline"
)

func main() {
	log := app.NewLogger("extractor")
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

	w := pipeline.NewExtractionWorker(st, broker, nlp.NewMockExtractor(), cfg.WorkerConcurrency, log)
	if err := w.Run(ctx); err != nil {
		log.Error("extraction worker", "err", err)
		os.Exit(1)
	}
	log.Info("extractor stopped")
}
