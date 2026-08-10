#!/usr/bin/env bash
# Start script for OntoBricks (Local Development)
# Usage: scripts/start.sh [--background|--restart|--help]
#
# NOTE: This script is for LOCAL development only.
# For Databricks Apps deployment, use: scripts/deploy.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo ""
echo -e "${GREEN}====================================${NC}"
echo -e "${GREEN}  OntoBricks - Starting Application ${NC}"
echo -e "${GREEN}====================================${NC}"
echo ""

# ── Check if running in Databricks Apps context ────────────────────────────
if [ -n "$DATABRICKS_APP_PORT" ] && [ -n "$DATABRICKS_RUNTIME_VERSION" ]; then
    echo -e "${YELLOW}⚠️  Detected Databricks Apps environment.${NC}"
    echo "This script is for local development."
    echo "In Databricks Apps, the platform runs 'python run.py' automatically."
    echo ""
    echo "Proceeding anyway (DATABRICKS_APP_PORT=$DATABRICKS_APP_PORT)..."
    echo ""
fi

# ── Virtual environment ────────────────────────────────────────────────────
if [ ! -d ".venv" ]; then
    echo -e "${YELLOW}Virtual environment not found. Running setup...${NC}"
    scripts/setup.sh
fi
if [ ! -f ".venv/bin/python" ]; then
    echo -e "${RED}Error: Python not found in virtual environment.${NC}"
    echo "Please run scripts/setup.sh to set up the environment."
    exit 1
fi

echo "Using virtual environment Python..."
uv sync --frozen --extra lakebase --extra pitfalls --quiet 2>/dev/null || true

# ── Load .env ──────────────────────────────────────────────────────────────
if [ -f ".env" ]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
    echo -e "  ${GREEN}✓${NC} .env loaded"
else
    echo -e "${YELLOW}Warning: .env file not found.${NC}"
    echo "Create one from .env.example or configure via the web UI."
    echo ""
fi

# ── Databricks auth pre-flight ─────────────────────────────────────────────
# If no PAT is configured, try to obtain a short-lived OAuth token from the
# CLI profile and inject it as DATABRICKS_TOKEN so the app never has to
# touch the CLI at runtime (which fails with expired sessions mid-request).

if [ -z "$DATABRICKS_TOKEN" ]; then
    PROFILE="${DATABRICKS_CONFIG_PROFILE:-DEFAULT}"
    echo -e "  ${YELLOW}ℹ${NC}  No PAT in .env — trying CLI profile '${PROFILE}'..."

    OAUTH_TOKEN=$(databricks auth token --profile "$PROFILE" 2>/dev/null | grep '^{' | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('access_token',''))" 2>/dev/null || true)

    if [ -n "$OAUTH_TOKEN" ]; then
        export DATABRICKS_TOKEN="$OAUTH_TOKEN"
        echo -e "  ${GREEN}✓${NC} OAuth token injected from profile '${PROFILE}'"
    else
        echo ""
        echo -e "${RED}✗  Databricks authentication failed.${NC}"
        echo ""
        echo "  Your CLI session for profile '${PROFILE}' is expired."
        echo "  Fix with ONE of the following:"
        echo ""
        echo -e "  ${GREEN}Option A — Re-authenticate (recommended):${NC}"
        echo "    databricks auth login --profile ${PROFILE}"
        echo "    Then re-run: ./scripts/start.sh"
        echo ""
        echo -e "  ${GREEN}Option B — Use a Personal Access Token:${NC}"
        echo "    1. Go to: ${DATABRICKS_HOST:-https://your-workspace.cloud.databricks.com}"
        echo "       → User Settings → Developer → Access Tokens → Generate"
        echo "    2. Add to .env:  DATABRICKS_TOKEN=dapi..."
        echo "    3. Re-run: ./scripts/start.sh"
        echo ""
        exit 1
    fi
else
    echo -e "  ${GREEN}✓${NC} DATABRICKS_TOKEN set"
fi

echo -e "  ${GREEN}✓${NC} DATABRICKS_HOST=${DATABRICKS_HOST}"
echo ""

# ── Parse CLI args ─────────────────────────────────────────────────────────
PORT=${DATABRICKS_APP_PORT:-8000}
PID_FILE=".ontobricks.pid"
BACKGROUND=false
RESTART=false

for arg in "$@"; do
    case $arg in
        --background|-b) BACKGROUND=true ;;
        --restart|-r)    RESTART=true ;;
        --no-reload)     export ONTOBRICKS_NO_RELOAD=1 ;;
        --help|-h)
            echo "Usage: scripts/start.sh [options]"
            echo ""
            echo "Options:"
            echo "  --background, -b    Run in background"
            echo "  --restart, -r       Restart if already running"
            echo "  --no-reload         Disable auto-reload (use for long live runs"
            echo "                      like 'make scenario-campaign', where a src/"
            echo "                      edit would kill in-flight background tasks)"
            echo "  --help, -h          Show this help message"
            exit 0
            ;;
    esac
done

# ── Handle restart ─────────────────────────────────────────────────────────
if [ "$RESTART" = true ]; then
    echo "Restarting OntoBricks..."
    scripts/stop.sh 2>/dev/null || true
    sleep 1
fi

# ── Check if already running ───────────────────────────────────────────────
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        echo -e "${RED}OntoBricks is already running (PID: $OLD_PID)${NC}"
        echo "Use scripts/stop.sh to stop it first, or scripts/start.sh --restart to restart."
        exit 1
    else
        rm -f "$PID_FILE"
    fi
fi

echo -e "Starting OntoBricks on port ${GREEN}$PORT${NC}..."
echo ""

PYTHON_CMD=".venv/bin/python"

if [ "$BACKGROUND" = true ]; then
    nohup $PYTHON_CMD run.py > .ontobricks.log 2>&1 &
    PID=$!
    echo $PID > "$PID_FILE"
    sleep 2
    if ps -p $PID > /dev/null 2>&1; then
        echo -e "${GREEN}OntoBricks started successfully in background${NC}"
        echo "PID: $PID  |  Log: .ontobricks.log"
        echo ""
        echo -e "Open your browser to: ${GREEN}http://localhost:$PORT${NC}"
        echo ""
        echo "To stop: scripts/stop.sh"
        echo "To view logs: tail -f .ontobricks.log"
    else
        echo -e "${RED}Failed to start OntoBricks${NC}"
        echo "Check .ontobricks.log for errors"
        rm -f "$PID_FILE"
        exit 1
    fi
else
    echo -e "Open your browser to: ${GREEN}http://localhost:$PORT${NC}"
    echo "Press Ctrl+C to stop the server"
    echo ""
    echo $$ > "$PID_FILE"
    trap "rm -f $PID_FILE" EXIT
    $PYTHON_CMD run.py
fi
