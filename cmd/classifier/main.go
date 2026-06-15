// Command classifier runs the LLM classification stage. It can be scaled to N
// replicas; the Redis consumer group load-balances tokens across them. The
// backing classifier (mock or Ollama) is selected via LLM_PROVIDER.
package main

import (
	"os"

	"github.com/kob-h/docpipeline/internal/app"
	"github.com/kob-h/docpipeline/internal/config"
	"github.com/kob-h/docpipeline/internal/pipeline"
)

func main() {
	log := app.NewLogger("classifier")
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

	classifier := app.NewClassifier(cfg, log)
	w := pipeline.NewClassificationWorker(st, broker, classifier, cfg.WorkerConcurrency, log)
	if err := w.Run(ctx); err != nil {
		log.Error("classification worker", "err", err)
		os.Exit(1)
	}
	log.Info("classifier stopped")
}
