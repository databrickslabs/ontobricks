# Makefile for OntoBricks (FastAPI)
#
# All deployment values (app names, DAB target, registry coords, SQL
# warehouse, Lakebase project/branch/database, app.yaml runtime
# fallbacks) are centralised in `scripts/deploy.config.sh`. Edit that
# file to change deployment behaviour, then `make deploy`.
#
# `scripts/deploy.sh` sources the config; the bootstrap targets below
# do the same so `make bootstrap-perms` / `make bootstrap-lakebase`
# stay aligned with the rest of the workflow.

CONFIG := scripts/deploy.config.sh

.PHONY: help install test test-cov run dev prod setup format lint clean \
        deploy deploy-volume deploy-no-run \
        bootstrap-perms bootstrap-lakebase \
        bundle-validate bundle-summary deploy-check \
        render-app-yaml

help:
	@echo "OntoBricks (FastAPI) - Available commands:"
	@echo ""
	@echo "  Development:"
	@echo "    make install      - Install dependencies"
	@echo "    make run          - Run the application locally"
	@echo "    make dev          - Run in development mode with auto-reload"
	@echo "    make setup        - Complete setup (install + configure)"
	@echo ""
	@echo "  Testing:"
	@echo "    make test         - Run tests"
	@echo "    make test-cov     - Run tests with coverage"
	@echo ""
	@echo "  Code Quality:"
	@echo "    make format       - Format code with black"
	@echo "    make lint         - Lint code with flake8"
	@echo ""
	@echo "  Deployment (Databricks Asset Bundles — dev sandbox only):"
	@echo "    Edit values in: $(CONFIG)"
	@echo "    make deploy              - Deploy + start the dev sandbox app (Lakebase backend)"
	@echo "    make deploy-volume       - Deploy + start the dev sandbox app (Volume-only backend)"
	@echo "    make deploy-no-run       - Deploy without starting the app (Lakebase target)"
	@echo "    make render-app-yaml     - Re-render app.yaml from template + config"
	@echo "    make bootstrap-perms     - Grant the app SP CAN_MANAGE on itself (first-run fix)"
	@echo "    make bootstrap-lakebase  - Grant the app SP USAGE/DML on the Lakebase registry schema"
	@echo "    make bundle-validate     - Validate the bundle config (Lakebase target)"
	@echo "    make bundle-summary      - Show bundle summary (Lakebase target)"
	@echo ""
	@echo "  Maintenance:"
	@echo "    make clean        - Remove generated files"
	@echo ""

install:
	@echo "Installing dependencies..."
	uv venv
	. .venv/bin/activate && uv pip install -e .

setup:
	@echo "Running setup..."
	chmod +x scripts/setup.sh
	scripts/setup.sh

run:
	@echo "Starting OntoBricks (FastAPI)..."
	. .venv/bin/activate && python run.py

test:
	@echo "Running tests..."
	. .venv/bin/activate && pytest

test-cov:
	@echo "Running tests with coverage..."
	. .venv/bin/activate && pytest --cov=src --cov-report=html --cov-report=term

format:
	@echo "Formatting code..."
	. .venv/bin/activate && black src/ tests/

lint:
	@echo "Linting code..."
	. .venv/bin/activate && flake8 src/ tests/ --max-line-length=100

clean:
	@echo "Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache htmlcov .coverage
	rm -rf flask_session fastapi_session
	@echo "Clean complete!"

dev:
	@echo "Starting development server with auto-reload..."
	. .venv/bin/activate && python run.py

prod:
	@echo "Starting production server..."
	. .venv/bin/activate && uvicorn app.fastapi.main:app --host 0.0.0.0 --port 8000

# ── Deployment (DAB — Databricks Asset Bundles) ──────────────
# `scripts/deploy.sh` is the single orchestrator: it sources
# `$(CONFIG)`, renders app.yaml from app.yaml.template, runs
# `databricks bundle deploy` with --var= overrides composed from the
# config, then bootstraps app SP perms (and Lakebase schema GRANTs on
# *-lakebase targets). The DAB target defaults to `dev-lakebase` from
# `$(CONFIG)`; the `deploy-volume` target overrides on the CLI.

deploy:
	chmod +x scripts/deploy.sh
	scripts/deploy.sh

deploy-volume:
	chmod +x scripts/deploy.sh
	scripts/deploy.sh -t dev

deploy-no-run:
	chmod +x scripts/deploy.sh
	scripts/deploy.sh --no-run

render-app-yaml:
	@echo "Rendering app.yaml from app.yaml.template + $(CONFIG)..."
	@. ./$(CONFIG) && python3 scripts/_render-app-yaml.py

bootstrap-perms:
	@echo "Bootstrapping app self-permissions (config: $(CONFIG))..."
	chmod +x scripts/bootstrap-app-permissions.sh
	@. ./$(CONFIG) && scripts/bootstrap-app-permissions.sh

bootstrap-lakebase:
	@echo "Granting Lakebase schema USAGE/DML to sandbox apps (config: $(CONFIG))..."
	chmod +x scripts/bootstrap-lakebase-perms.sh
	@. ./$(CONFIG) && \
	  scripts/bootstrap-lakebase-perms.sh \
	    -i "$$LAKEBASE_BOOTSTRAP_INSTANCE" \
	    -d "$$LAKEBASE_BOOTSTRAP_DATABASE" \
	    -s "$$LAKEBASE_BOOTSTRAP_SCHEMA" \
	    -a "$$APP_NAME" -a "$$MCP_APP_NAME"

bundle-validate:
	@echo "Validating Databricks Asset Bundle (target: dev-lakebase)..."
	@. ./$(CONFIG) && databricks bundle validate -t dev-lakebase \
	    --var=warehouse_id="$$WAREHOUSE_ID" \
	    --var=registry_catalog="$$REGISTRY_CATALOG" \
	    --var=registry_schema="$$REGISTRY_SCHEMA" \
	    --var=registry_volume="$$REGISTRY_VOLUME" \
	    --var=lakebase_project="$$LAKEBASE_PROJECT" \
	    --var=lakebase_branch="$$LAKEBASE_BRANCH" \
	    --var=lakebase_database_resource_segment="$$LAKEBASE_DATABASE_RESOURCE_SEGMENT" \
	    --var=lakebase_registry_schema="$$LAKEBASE_REGISTRY_SCHEMA"

bundle-summary:
	@echo "Bundle summary (target: dev-lakebase)..."
	databricks bundle summary -t dev-lakebase

# Check deployment prerequisites
deploy-check:
	@echo "Checking deployment prerequisites..."
	@command -v databricks >/dev/null 2>&1 || { echo "ERROR: Databricks CLI not installed"; exit 1; }
	@echo "  Databricks CLI: OK"
	@test -f databricks.yml || { echo "ERROR: databricks.yml not found"; exit 1; }
	@echo "  databricks.yml: OK"
	@test -f app.yaml.template || { echo "ERROR: app.yaml.template not found"; exit 1; }
	@echo "  app.yaml.template: OK"
	@test -f $(CONFIG) || { echo "ERROR: $(CONFIG) not found"; exit 1; }
	@echo "  $(CONFIG): OK"
	@test -f run.py || { echo "ERROR: run.py not found"; exit 1; }
	@echo "  run.py: OK"
	@databricks current-user me >/dev/null 2>&1 || { echo "ERROR: Not authenticated. Run: databricks auth login"; exit 1; }
	@echo "  CLI auth: OK"
	@echo "All prerequisites met!"
