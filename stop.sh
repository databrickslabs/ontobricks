#!/bin/bash
# Stop script for OntoBricks (Local Development)
# Usage: ./stop.sh
#
# NOTE: This script is for LOCAL development only.
# For Databricks Apps, use the Databricks Apps console to stop the app.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo ""
echo -e "${GREEN}====================================${NC}"
echo -e "${GREEN}  OntoBricks - Stopping Application ${NC}"
echo -e "${GREEN}====================================${NC}"
echo ""

PID_FILE=".ontobricks.pid"
PORT=${DATABRICKS_APP_PORT:-8000}

stopped=false

# Method 1: Check PID file
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "Stopping OntoBricks (PID: $PID)..."
        kill "$PID" 2>/dev/null
        
        # Wait for process to stop
        for i in {1..10}; do
            if ! ps -p "$PID" > /dev/null 2>&1; then
                break
            fi
            sleep 0.5
        done
        
        # Force kill if still running
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "Force stopping..."
            kill -9 "$PID" 2>/dev/null
        fi
        
        stopped=true
    fi
    rm -f "$PID_FILE"
fi

# Method 2: Find by port (backup)
if [ "$stopped" = false ]; then
    # Find process using the port
    PIDS=$(lsof -ti:$PORT 2>/dev/null || true)
    
    if [ -n "$PIDS" ]; then
        echo "Stopping process(es) on port $PORT..."
        for PID in $PIDS; do
            # Verify it's our Python process
            CMDLINE=$(ps -p $PID -o command= 2>/dev/null || true)
            if [[ "$CMDLINE" == *"python"*"run.py"* ]] || [[ "$CMDLINE" == *"uvicorn"* ]]; then
                echo "  Stopping PID: $PID"
                kill "$PID" 2>/dev/null
                stopped=true
            fi
        done
        
        # Wait and verify
        sleep 1
        
        for PID in $PIDS; do
            if ps -p "$PID" > /dev/null 2>&1; then
                kill -9 "$PID" 2>/dev/null
            fi
        done
    fi
fi

# Clean up
rm -f "$PID_FILE"

if [ "$stopped" = true ]; then
    echo ""
    echo -e "${GREEN}OntoBricks stopped successfully${NC}"
else
    echo -e "${YELLOW}No running OntoBricks instance found${NC}"
fi

echo ""

