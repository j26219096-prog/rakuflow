# RakuFlow Makefile
# Usage: make <target>
# Requires: Docker, Docker Compose, Python 3.11+

.PHONY: up down run simple-run dbt-run dashboard logs reset lint test help

COMPOSE = docker-compose
PYTHON  = python

# ── Load .env if present ───────────────────────────────────────────────────────
ifneq (,$(wildcard .env))
    include .env
    export
endif

# ── Docker Compose commands ────────────────────────────────────────────────────

## Start all RakuFlow services in detached mode
up:
	@echo "🚀 Starting RakuFlow services..."
	docker network create rakuflow-network 2>/dev/null || true
	$(COMPOSE) up -d
	@echo "✅ Services started. Airflow UI: http://localhost:8080 | Dashboard: http://localhost:8501"

## Stop all services (keeps volumes)
down:
	@echo "🛑 Stopping RakuFlow services..."
	$(COMPOSE) down
	@echo "✅ Services stopped."

## Stream live logs from all services
logs:
	$(COMPOSE) logs -f

## Tear down everything including volumes (destructive!)
reset:
	@echo "⚠️  Resetting RakuFlow — this will delete all data volumes!"
	$(COMPOSE) down -v --remove-orphans
	docker network rm rakuflow-network 2>/dev/null || true
	@echo "✅ All containers and volumes removed."

# ── Pipeline commands ──────────────────────────────────────────────────────────

## Run Kafka producer + Spark consumer (full ingestion pipeline)
run:
	@echo "📤 Running Kafka producer..."
	$(PYTHON) ingestion/producer.py
	@echo "⚡ Running Spark consumer..."
	spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
		--jars /opt/jars/postgresql-42.7.3.jar \
		processing/spark_consumer.py
	@echo "✅ Ingestion pipeline complete."

## Run Kafka producer + simple_consumer (no Spark/Java required — for local dev)
simple-run:
	@echo "📤 Running Kafka producer..."
	$(PYTHON) ingestion/producer.py
	@echo "⚡ Running simple consumer (psycopg2 — no Spark required)..."
	$(PYTHON) processing/simple_consumer.py
	@echo "✅ Ingestion pipeline complete (simple mode)."

## Run dbt models and then dbt tests
dbt-run:
	@echo "🔧 Running dbt models..."
	cd dbt_project && dbt run --profiles-dir . --target dev
	@echo "🧪 Running dbt tests..."
	cd dbt_project && dbt test --profiles-dir . --target dev
	@echo "✅ dbt pipeline complete."

## Open the Streamlit dashboard in your browser
dashboard:
	@echo "📊 Opening RakuFlow Analytics Dashboard on http://localhost:8501"
	start http://localhost:8501

## Run Great Expectations validation suite
quality:
	@echo "🔍 Running data quality checks..."
	$(PYTHON) quality/expectations.py
	@echo "✅ Quality checks complete."

# ── Development commands ───────────────────────────────────────────────────────

## Run all Python unit tests
test:
	@echo "🧪 Running unit tests..."
	pytest tests/ -v --tb=short
	@echo "✅ Tests complete."

## Run linting checks (requires ruff)
lint:
	@echo "🔍 Running linter..."
	ruff check ingestion/ processing/ dags/ quality/ dashboard/ tests/
	@echo "✅ Lint complete."

## Install Python dependencies locally
install:
	@echo "📦 Installing dependencies..."
	pip install -r requirements-dev.txt
	@echo "✅ Dependencies installed."

## Copy .env.example to .env (first-time setup)
init:
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "✅ .env created from .env.example — please fill in your values."; \
	else \
		echo "⚠️  .env already exists, skipping."; \
	fi

## Show help
help:
	@echo ""
	@echo "RakuFlow — Makefile commands:"
	@echo ""
	@echo "  make up           Start all Docker services"
	@echo "  make down         Stop all Docker services"
	@echo "  make logs         Stream live service logs"
	@echo "  make reset        Destroy all containers + volumes"
	@echo "  make run          Run producer + Spark consumer (requires Spark)"
	@echo "  make simple-run   Run producer + simple consumer (no Spark needed)"
	@echo "  make dbt-run      Run dbt models + tests"
	@echo "  make dashboard    Open Streamlit dashboard"
	@echo "  make quality      Run Great Expectations suite"
	@echo "  make test         Run Python unit tests"
	@echo "  make lint         Run code linting"
	@echo "  make install      Install Python dependencies"
	@echo "  make init         Initialize .env from .env.example"
	@echo ""
