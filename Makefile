.PHONY: help build up down logs ps run demo test test-unit test-integration lint fmt vet tidy clean

# Integration tests (testcontainers) need to reach the Docker daemon. Derive the
# socket from the active Docker context if DOCKER_HOST isn't already set, so this
# works with Docker Desktop, OrbStack, Colima, etc. Ryuk is disabled because the
# tests terminate their own containers (avoids socket-mount issues on some VMs).
DOCKER_HOST ?= $(shell docker context inspect --format '{{.Endpoints.docker.Host}}' 2>/dev/null)
export DOCKER_HOST
export TESTCONTAINERS_RYUK_DISABLED = true

# Default target: list available commands.
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

build: ## Build all three binaries into ./bin
	@mkdir -p bin
	go build -o bin/api ./cmd/api
	go build -o bin/extractor ./cmd/extractor
	go build -o bin/classifier ./cmd/classifier

up: ## Start the full stack (Postgres, Redis, api, extractor, classifier)
	docker compose up --build -d

run: up ## Alias for `up`

down: ## Stop the stack and remove volumes
	docker compose down -v

logs: ## Tail logs from all services
	docker compose logs -f

ps: ## Show running services
	docker compose ps

demo: ## Run the end-to-end demo of all six scenarios
	./scripts/demo.sh

test: ## Run all tests with the race detector (needs Docker for integration tests)
	go test -race ./...

test-unit: ## Run only fast unit tests (no Docker required)
	go test -race -short ./...

test-integration: ## Run only the integration tests (testcontainers: Postgres + Redis)
	go test -race -run Integration ./test/...

lint: ## Run golangci-lint (if installed)
	golangci-lint run ./...

vet: ## Run go vet
	go vet ./...

fmt: ## Format all Go code
	gofmt -w cmd internal test

tidy: ## Tidy go.mod / go.sum
	go mod tidy

clean: ## Remove build artifacts
	rm -rf bin
