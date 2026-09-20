# RaceStream developer commands.
#
# Every target here is something a new contributor will need on day one.
# Run `make help` for the list.

SHELL := /bin/bash
COMPOSE := docker compose
PY := .venv/Scripts/python.exe          # Windows layout; see PY_UNIX below
ifeq ($(OS),)
PY := .venv/bin/python
endif

.DEFAULT_GOAL := help
.PHONY: help dev up down logs ps build restart clean nuke \
        api frontend test test-py test-js lint format typecheck \
        db psql topics migrate-check ingest replay benchmark chaos \
        metrics health venv

## ---------------------------------------------------------------- lifecycle

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

dev: ## Build and start the whole stack
	$(COMPOSE) up --build -d
	@echo ""
	@echo "  Frontend    http://localhost:5173"
	@echo "  API docs    http://localhost:8000/docs"
	@echo "  Redpanda    http://localhost:8080"
	@echo "  Prometheus  http://localhost:9090"
	@echo "  Grafana     http://localhost:3002"

up: ## Start the stack without rebuilding
	$(COMPOSE) up -d

infra: ## Start only the infrastructure tier (db, broker, observability)
	$(COMPOSE) up -d timescaledb redpanda redpanda-console prometheus grafana

down: ## Stop all containers, keeping volumes
	$(COMPOSE) down

ps: ## Show container status
	$(COMPOSE) ps

logs: ## Tail logs (make logs SERVICE=api)
	$(COMPOSE) logs -f $(SERVICE)

restart: ## Restart one service (make restart SERVICE=processor)
	$(COMPOSE) restart $(SERVICE)

build: ## Rebuild images without starting
	$(COMPOSE) build

clean: ## Stop containers and remove volumes. Destroys ingested data.
	$(COMPOSE) down -v

nuke: clean ## clean, plus local build artefacts
	rm -rf frontend/dist frontend/node_modules .pytest_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

## ------------------------------------------------------------------ quality

test: test-py test-js ## Run the whole test suite

test-py: ## Python unit and integration tests
	$(PY) -m pytest tests -v

test-js: ## Frontend tests
	cd frontend && npm test

lint: ## Lint Python and TypeScript
	$(PY) -m ruff check services tests
	cd frontend && npm run lint

format: ## Auto-format
	$(PY) -m ruff format services tests
	$(PY) -m ruff check --fix services tests

typecheck: ## Type-check both sides
	$(PY) -m mypy services/common/racestream_common
	cd frontend && npm run typecheck

## --------------------------------------------------------------- operations

health: ## Print API health
	@curl -s http://localhost:8000/api/system/health | $(PY) -m json.tool

metrics: ## Print RaceStream Prometheus metrics
	@curl -s http://localhost:8000/metrics | grep -E '^racestream_' | grep -v '^#'

psql: ## Open a psql shell
	$(COMPOSE) exec timescaledb psql -U racestream -d racestream

db: ## Summarise row counts per table
	@$(COMPOSE) exec -T timescaledb psql -U racestream -d racestream -c "\
	  SELECT 'sessions' t, count(*) FROM sessions \
	  UNION ALL SELECT 'drivers', count(*) FROM drivers \
	  UNION ALL SELECT 'laps', count(*) FROM laps \
	  UNION ALL SELECT 'car_telemetry', count(*) FROM car_telemetry \
	  UNION ALL SELECT 'positions', count(*) FROM positions \
	  UNION ALL SELECT 'timing', count(*) FROM timing \
	  UNION ALL SELECT 'weather', count(*) FROM weather \
	  UNION ALL SELECT 'race_control_events', count(*) FROM race_control_events;"

topics: ## List Kafka topics with partition counts
	$(COMPOSE) exec redpanda rpk topic list

lag: ## Show consumer group lag
	$(COMPOSE) exec redpanda rpk group list
	$(COMPOSE) exec redpanda rpk group describe racestream-processor 2>/dev/null || true

ingest: ## Ingest a historical session (make ingest SESSION=9158)
	$(COMPOSE) run --rm ingestion python -m racestream_ingestion.main --session $(SESSION)

replay: ## Replay a session through the pipeline (make replay SESSION=9158 SPEED=1)
	curl -s -X POST http://localhost:8000/api/replay/start \
	  -H 'Content-Type: application/json' \
	  -d '{"session_id": $(SESSION), "speed": $(or $(SPEED),1)}' | $(PY) -m json.tool

benchmark: ## Run the load test and write docs/benchmarks.md figures
	$(PY) load/run_benchmark.py

chaos: ## Stop the telemetry consumer to demonstrate recovery
	curl -s -X POST http://localhost:8000/api/chaos/consumer/stop | $(PY) -m json.tool

venv: ## Create the local Python environment
	python -m venv .venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -e services/common
	$(PY) -m pip install -r services/api/requirements.txt
	$(PY) -m pip install -r services/ingestion/requirements.txt
	$(PY) -m pip install -r requirements-dev.txt
