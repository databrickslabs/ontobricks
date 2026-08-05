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

.PHONY: help install test test-cov scenario-campaign run dev prod setup format lint clean \
        deploy deploy-dry-run deploy-volume deploy-no-run \
        bootstrap-perms bootstrap-lakebase \
        bundle-validate bundle-summary deploy-check \
        render-app-yaml

# Output dir for the live scenario campaign reports (JUnit + HTML).
SCENARIO_ARTIFACTS := artifacts/scenarios
# Target app for the campaign; override to hit a deployed instance:
#   make scenario-campaign ONTOBRICKS_LIVE_BASE=https://<app-url>
ONTOBRICKS_LIVE_BASE ?= http://localhost:8000

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
	@echo "    make test              - Run tests"
	@echo "    make test-cov          - Run tests with coverage"
	@echo "    make scenario-campaign - Run the live E2E scenario campaign (opt-in, billable)"
	@echo "                             → JUnit + HTML reports in $(SCENARIO_ARTIFACTS)/"
	@echo "                             App must be running (make dev); override target with"
	@echo "                             ONTOBRICKS_LIVE_BASE=<url>"
	@echo ""
	@echo "  Code Quality:"
	@echo "    make format       - Format code with black"
	@echo "    make lint         - Lint code with flake8"
	@echo ""
	@echo "  Deployment (Databricks Asset Bundles — dev sandbox only):"
	@echo "    Edit values in: $(CONFIG)"
	@echo "    make deploy              - Deploy + start the dev sandbox app (Lakebase backend)"
	@echo "    make deploy-dry-run      - Run ALL pre-deploy checks (preflight/validate/resources), no changes"
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
	uv sync --frozen --extra lakebase --extra pitfalls

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

# ── Integration test campaign (live, opt-in scenario suites) ─────────────
# Runs every `scenario`-marked suite under tests/e2e/scenarios/ end-to-end,
# in filename order (test_scenario_1 → 2 → 3 → … → test_scenario_validation),
# against a RUNNING app and writes machine + human reports to
# $(SCENARIO_ARTIFACTS)/ (campaign.xml for CI, campaign.html to open).
#
# These are billable (warehouse + LLM) and mutate the registry the app reads,
# so they stay opt-in: this target sets ONTOBRICKS_SCENARIO_LIVE=1 for you.
# Point at another instance with `ONTOBRICKS_LIVE_BASE=<url>`. Preflight the
# app health before spending money.
scenario-campaign:
	@echo "Scenario campaign → $(ONTOBRICKS_LIVE_BASE)"
	@curl -sf "$(ONTOBRICKS_LIVE_BASE)/health" >/dev/null 2>&1 \
	  || curl -sf "$(ONTOBRICKS_LIVE_BASE)/healthz" >/dev/null 2>&1 \
	  || { echo "ERROR: no app reachable at $(ONTOBRICKS_LIVE_BASE) — start it (make dev) or set ONTOBRICKS_LIVE_BASE"; exit 1; }
	@mkdir -p $(SCENARIO_ARTIFACTS)
	@echo "Running live scenarios (JUnit + HTML → $(SCENARIO_ARTIFACTS)/)..."
	. .venv/bin/activate && \
	  ONTOBRICKS_SCENARIO_LIVE=1 ONTOBRICKS_SCENARIO_CHAIN=1 \
	  ONTOBRICKS_LIVE_BASE="$(ONTOBRICKS_LIVE_BASE)" \
	  pytest tests/e2e/scenarios -m scenario -v -s --no-cov -p no:randomly \
	    --junitxml=$(SCENARIO_ARTIFACTS)/campaign.xml \
	    --html=$(SCENARIO_ARTIFACTS)/campaign.html --self-contained-html
	@echo "Reports: $(SCENARIO_ARTIFACTS)/campaign.html  (JUnit: $(SCENARIO_ARTIFACTS)/campaign.xml)"
	@echo "         $(SCENARIO_ARTIFACTS)/campaign_report.md  (validation summary, if it ran)"

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
	rm -rf $(SCENARIO_ARTIFACTS) artifacts
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
	unset APP_NAME MCP_APP_NAME REGISTRY_SCHEMA LAKEBASE_REGISTRY_SCHEMA LAKEBASE_REGISTRY_DATABASE APP_LAKEBASE_SCHEMA APP_LAKEBASE_DATABASE; scripts/deploy.sh

deploy-dry-run:
	chmod +x scripts/deploy.sh
	unset APP_NAME MCP_APP_NAME REGISTRY_SCHEMA LAKEBASE_REGISTRY_SCHEMA LAKEBASE_REGISTRY_DATABASE APP_LAKEBASE_SCHEMA APP_LAKEBASE_DATABASE; scripts/deploy.sh --dry-run

deploy-volume:
	chmod +x scripts/deploy.sh
	unset APP_NAME MCP_APP_NAME REGISTRY_SCHEMA LAKEBASE_REGISTRY_SCHEMA LAKEBASE_REGISTRY_DATABASE APP_LAKEBASE_SCHEMA APP_LAKEBASE_DATABASE; scripts/deploy.sh -t dev

deploy-no-run:
	chmod +x scripts/deploy.sh
	unset APP_NAME MCP_APP_NAME REGISTRY_SCHEMA LAKEBASE_REGISTRY_SCHEMA LAKEBASE_REGISTRY_DATABASE APP_LAKEBASE_SCHEMA APP_LAKEBASE_DATABASE; scripts/deploy.sh --no-run

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
	    -i "$$LAKEBASE_PROJECT" \
	    -b "$$LAKEBASE_BRANCH" \
	    -d "$$LAKEBASE_REGISTRY_DATABASE" \
	    -s "$$LAKEBASE_REGISTRY_SCHEMA" \
	    -a "$$APP_NAME" -a "$$MCP_APP_NAME"

bundle-validate:
	@echo "Validating Databricks Asset Bundle (target: dev-lakebase)..."
	@. ./$(CONFIG) && databricks bundle validate -t dev-lakebase \
	    --var=app_name="$$APP_NAME" \
	    --var=mcp_app_name="$$MCP_APP_NAME" \
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
