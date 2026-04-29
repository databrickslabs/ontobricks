# Makefile for OntoBricks (FastAPI)

.PHONY: help install test run clean format lint deploy deploy-volume deploy-no-run bundle-validate bundle-summary bootstrap-perms bootstrap-lakebase

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
	@echo "    make deploy              - Deploy + start ontobricks-dev (Lakebase backend)"
	@echo "    make deploy-volume       - Deploy + start ontobricks-dev (Volume-only backend)"
	@echo "    make deploy-no-run       - Deploy without starting the app (Lakebase target)"
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
# This bundle (databricks.yml) only manages the dev sandbox app
# `ontobricks-dev`. The `dev-lakebase` target binds the Lakebase
# Postgres database the registry relies on; the bare `dev` target
# is the Volume-only fallback. The default below is `dev-lakebase`
# because deploying with `dev` would strip the postgres binding
# from the live app.
deploy:
	@echo "Deploying + starting ontobricks-dev (target: dev-lakebase)..."
	chmod +x scripts/deploy.sh
	scripts/deploy.sh -t dev-lakebase

deploy-volume:
	@echo "Deploying + starting ontobricks-dev (target: dev, Volume-only)..."
	chmod +x scripts/deploy.sh
	scripts/deploy.sh -t dev

deploy-no-run:
	@echo "Deploying without starting the app (target: dev-lakebase)..."
	chmod +x scripts/deploy.sh
	scripts/deploy.sh -t dev-lakebase --no-run

bootstrap-perms:
	@echo "Bootstrapping app self-permissions..."
	chmod +x scripts/bootstrap-app-permissions.sh
	scripts/bootstrap-app-permissions.sh ontobricks-dev

bootstrap-lakebase:
	@echo "Granting Lakebase schema USAGE/DML to ontobricks-dev..."
	chmod +x scripts/bootstrap-lakebase-perms.sh
	scripts/bootstrap-lakebase-perms.sh -a ontobricks-dev

bundle-validate:
	@echo "Validating Databricks Asset Bundle (target: dev-lakebase)..."
	databricks bundle validate -t dev-lakebase

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
	@test -f app.yaml || { echo "ERROR: app.yaml not found"; exit 1; }
	@echo "  app.yaml: OK"
	@test -f run.py || { echo "ERROR: run.py not found"; exit 1; }
	@echo "  run.py: OK"
	@databricks current-user me >/dev/null 2>&1 || { echo "ERROR: Not authenticated. Run: databricks auth login"; exit 1; }
	@echo "  CLI auth: OK"
	@databricks bundle validate >/dev/null 2>&1 || { echo "ERROR: Bundle validation failed"; exit 1; }
	@echo "  Bundle validation: OK"
	@echo "All prerequisites met!"
