#!/usr/bin/env bash
# Start the full pipeline stack locally and wait until the API is healthy.
#
#   ./start.sh
#
# Honors a local .env file (copy from .env.example) for the classification
# backend and other settings.
set -euo pipefail

cd "$(dirname "$0")"

echo "==> Building and starting services (Postgres, Redis, api, extractor, classifier)…"
docker compose up --build -d

echo "==> Waiting for the API to become healthy…"
for i in $(seq 1 60); do
  if curl -fsS http://localhost:8080/healthz >/dev/null 2>&1; then
    echo "==> API is up at http://localhost:8080"
    echo
    echo "Try it:"
    echo "  curl -X POST http://localhost:8080/process -H 'Content-Type: application/json' \\"
    echo "    -d '{\"document_id\":\"doc-1\",\"text\":\"John Smith works at Acme Corp.\"}'"
    echo "  curl http://localhost:8080/documents/doc-1/status"
    echo "  curl 'http://localhost:8080/documents/doc-1/tokens?classification=PERSON'"
    echo
    echo "Run the full demo with: ./scripts/demo.sh"
    exit 0
  fi
  sleep 1
done

echo "!! API did not become healthy in time. Check logs with: docker compose logs" >&2
exit 1
