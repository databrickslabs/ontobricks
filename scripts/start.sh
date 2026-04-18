#!/bin/bash
# Start script for OntoBricks (Local Development)
# Usage: scripts/start.sh [--background]
#
# NOTE: This script is for LOCAL development only.
# For Databricks Apps deployment, use: scripts/deploy.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo ""
echo -e "${GREEN}====================================${NC}"
echo -e "${GREEN}  OntoBricks - Starting Application ${NC}"
echo -e "${GREEN}====================================${NC}"
echo ""

# Check if running in Databricks Apps context
if [ -n "$DATABRICKS_APP_PORT" ] && [ -n "$DATABRICKS_RUNTIME_VERSION" ]; then
    echo -e "${YELLOW}⚠️  Detected Databricks Apps environment.${NC}"
    echo "This script is for local development."
    echo "In Databricks Apps, the platform runs 'python run.py' automatically."
    echo ""
    echo "Proceeding anyway (DATABRICKS_APP_PORT=$DATABRICKS_APP_PORT)..."
    echo ""
fi

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo -e "${YELLOW}Virtual environment not found. Running setup...${NC}"
    scripts/setup.sh
fi

# Check if Python exists in venv
if [ ! -f ".venv/bin/python" ]; then
    echo -e "${RED}Error: Python not found in virtual environment.${NC}"
    echo "Please run scripts/setup.sh to set up the environment."
    exit 1
fi

echo "Using virtual environment Python..."

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}Warning: .env file not found.${NC}"
    echo "Create one from .env.example or configure via the web UI."
    echo ""
fi

# Get port from environment or use default
PORT=${DATABRICKS_APP_PORT:-8000}

PID_FILE=".ontobricks.pid"

# Parse arguments first so --restart can run before the "already running" check
BACKGROUND=false
RESTART=false
for arg in "$@"; do
    case $arg in
        --background|-b)
            BACKGROUND=true
            ;;
        --restart|-r)
            RESTART=true
            ;;
        --help|-h)
            echo "Usage: scripts/start.sh [options]"
            echo ""
            echo "Options:"
            echo "  --background, -b    Run in background"
            echo "  --restart, -r       Restart if already running"
            echo "  --help, -h          Show this help message"
            exit 0
            ;;
    esac
done

# Handle restart (must run before blocking on PID file)
if [ "$RESTART" = true ]; then
    echo "Restarting OntoBricks..."
    scripts/stop.sh 2>/dev/null || true
    sleep 1
fi

# Check if already running
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        echo -e "${RED}OntoBricks is already running (PID: $OLD_PID)${NC}"
        echo "Use scripts/stop.sh to stop it first, or scripts/start.sh --restart to restart."
        exit 1
    else
        # Remove stale PID file
        rm -f "$PID_FILE"
    fi
fi

echo -e "Starting OntoBricks on port ${GREEN}$PORT${NC}..."
echo ""

# Use Python from the virtual environment
PYTHON_CMD=".venv/bin/python"

if [ "$BACKGROUND" = true ]; then
    # Run in background
    nohup $PYTHON_CMD run.py > .ontobricks.log 2>&1 &
    PID=$!
    echo $PID > "$PID_FILE"
    
    # Wait a moment for the server to start
    sleep 2
    
    if ps -p $PID > /dev/null 2>&1; then
        echo -e "${GREEN}OntoBricks started successfully in background${NC}"
        echo "PID: $PID"
        echo "Log file: .ontobricks.log"
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
    # Run in foreground
    echo -e "Open your browser to: ${GREEN}http://localhost:$PORT${NC}"
    echo ""
    echo "Press Ctrl+C to stop the server"
    echo ""
    
    # Save PID for reference
    echo $$ > "$PID_FILE"
    
    # Trap to clean up PID file on exit
    trap "rm -f $PID_FILE" EXIT
    
    $PYTHON_CMD run.py
fi

