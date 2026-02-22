.PHONY: help up down logs shell simulate ingest transform dbt-run quality \
        pipeline-run lint test test-cov format pre-commit dbt-docs seed \
        backfill reset check-env

# ── Colors ────────────────────────────────────────────────────────────────────
CYAN  := \033[0;36m
GREEN := \033[0;32m
YELLOW:= \033[0;33m
RED   := \033[0;31m
NC    := \033[0m  # No Color

# ── Config ────────────────────────────────────────────────────────────────────
COMPOSE_FILE := docker/docker-compose.yml
PYTHON       := python3
DAYS         ?= 7

# ══════════════════════════════════════════════════════════════════════════════
help:  ## Show all available commands
	@echo ""
	@echo "$(CYAN)Social Media Analytics Platform — Developer Commands$(NC)"
	@echo "──────────────────────────────────────────────────────────────"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""

# ══════════════════════════════════════════════════════════════════════════════
# INFRASTRUCTURE
# ══════════════════════════════════════════════════════════════════════════════
up: check-env  ## Start all Docker services
	@echo "$(CYAN)Starting infrastructure...$(NC)"
	docker compose -f $(COMPOSE_FILE) up -d --wait
	@echo "$(GREEN)All services up!$(NC)"
	@echo "  Airflow UI  : http://localhost:8090  (admin/admin)"
	@echo "  MinIO       : http://localhost:9001  (minioadmin/minioadmin)"
	@echo "  Spark UI    : http://localhost:8080"

down:  ## Stop all Docker services
	@echo "$(YELLOW)Stopping services...$(NC)"
	docker compose -f $(COMPOSE_FILE) down
	@echo "$(GREEN)Done.$(NC)"

logs:  ## Tail logs from all services
	docker compose -f $(COMPOSE_FILE) logs -f

shell:  ## Open a Python shell inside the Spark container
	docker compose -f $(COMPOSE_FILE) exec spark-master bash

ps:  ## Show running containers and health status
	docker compose -f $(COMPOSE_FILE) ps

# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE STEPS
# ══════════════════════════════════════════════════════════════════════════════
simulate:  ## Run data simulators — generates ~100K events to Kafka + landing zone
	@echo "$(CYAN)Running data simulators...$(NC)"
	$(PYTHON) src/simulators/twitter_simulator.py --events 50000
	$(PYTHON) src/simulators/youtube_simulator.py --videos 500
	$(PYTHON) src/simulators/reddit_simulator.py  --posts 2000
	@echo "$(GREEN)Simulation complete.$(NC)"

ingest:  ## Run batch ingestion — landing zone → Bronze layer
	@echo "$(CYAN)Running batch ingestion...$(NC)"
	$(PYTHON) src/ingestion/batch_ingestion.py
	@echo "$(GREEN)Ingestion complete.$(NC)"

transform:  ## Run PySpark transformation — Bronze → Silver
	@echo "$(CYAN)Running PySpark Bronze → Silver...$(NC)"
	docker compose -f $(COMPOSE_FILE) exec spark-master \
		spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.memory=2g \
		/opt/src/transformation/spark_jobs/bronze_to_silver.py
	@echo "$(GREEN)Transformation complete.$(NC)"

dbt-run:  ## Run dbt models — Silver → Gold (Star Schema)
	@echo "$(CYAN)Running dbt models...$(NC)"
	cd src/transformation/dbt && dbt run --profiles-dir .
	@echo "$(GREEN)dbt run complete.$(NC)"

dbt-test:  ## Run dbt tests — schema + data quality tests
	@echo "$(CYAN)Running dbt tests...$(NC)"
	cd src/transformation/dbt && dbt test --profiles-dir .

dbt-docs:  ## Generate and serve dbt documentation
	@echo "$(CYAN)Generating dbt docs...$(NC)"
	cd src/transformation/dbt && dbt docs generate --profiles-dir .
	cd src/transformation/dbt && dbt docs serve --port 8085 --profiles-dir .

quality:  ## Run all Great Expectations data quality checkpoints
	@echo "$(CYAN)Running data quality checks...$(NC)"
	$(PYTHON) -m great_expectations checkpoint run bronze_quality_check
	$(PYTHON) -m great_expectations checkpoint run silver_quality_check
	$(PYTHON) -m great_expectations checkpoint run gold_quality_check
	@echo "$(GREEN)Quality checks complete. See quality/expectations/uncommitted/data_docs/$(NC)"

seed:  ## Load static seed data into Gold layer dimensions
	@echo "$(CYAN)Seeding dimension tables...$(NC)"
	cd src/transformation/dbt && dbt seed --profiles-dir .
	@echo "$(GREEN)Seed complete.$(NC)"

backfill:  ## Backfill pipeline for last N days (default: 7). Usage: make backfill DAYS=30
	@echo "$(CYAN)Backfilling last $(DAYS) days...$(NC)"
	$(PYTHON) scripts/backfill.py --days $(DAYS)
	@echo "$(GREEN)Backfill complete.$(NC)"

pipeline-run:  ## Run complete end-to-end pipeline
	@echo "$(CYAN)Running full pipeline...$(NC)"
	@$(MAKE) simulate
	@$(MAKE) ingest
	@$(MAKE) transform
	@$(MAKE) seed
	@$(MAKE) dbt-run
	@$(MAKE) dbt-test
	@$(MAKE) quality
	@echo ""
	@echo "$(GREEN)Full pipeline complete!$(NC)"
	@echo "$(GREEN)Gold layer data is ready for analysis.$(NC)"

# ══════════════════════════════════════════════════════════════════════════════
# CODE QUALITY
# ══════════════════════════════════════════════════════════════════════════════
lint:  ## Run flake8 linter
	@echo "$(CYAN)Running linter...$(NC)"
	flake8 src/ tests/ --max-line-length=120 --exclude=src/transformation/dbt/

format:  ## Auto-format code with black + isort
	@echo "$(CYAN)Formatting code...$(NC)"
	black src/ tests/ --line-length=120
	isort src/ tests/ --profile=black

test:  ## Run pytest unit tests
	@echo "$(CYAN)Running unit tests...$(NC)"
	pytest tests/unit/ -v

test-cov:  ## Run tests with coverage report (min 80%)
	@echo "$(CYAN)Running tests with coverage...$(NC)"
	pytest tests/ -v --cov=src --cov-report=term-missing --cov-fail-under=80

pre-commit:  ## Run pre-commit hooks on all files
	pre-commit run --all-files

# ══════════════════════════════════════════════════════════════════════════════
# UTILITIES
# ══════════════════════════════════════════════════════════════════════════════
check-env:  ## Check that required tools are installed
	@echo "$(CYAN)Checking environment...$(NC)"
	@docker --version > /dev/null 2>&1 || (echo "$(RED)ERROR: Docker not found$(NC)" && exit 1)
	@docker compose version > /dev/null 2>&1 || (echo "$(RED)ERROR: Docker Compose not found$(NC)" && exit 1)
	@echo "$(GREEN)Environment OK.$(NC)"

reset:  ## ⚠️  DROP ALL DATA and restart from scratch
	@echo "$(RED)WARNING: This will delete all data in Bronze, Silver, and Gold layers!$(NC)"
	@read -p "Are you sure? [y/N] " ans && [ $${ans:-N} = y ]
	docker compose -f $(COMPOSE_FILE) down -v
	@echo "$(YELLOW)All volumes removed. Run 'make up' to restart.$(NC)"
