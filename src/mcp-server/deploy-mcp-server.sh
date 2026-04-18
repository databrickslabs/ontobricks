#!/usr/bin/env bash
set -euo pipefail

# ── MCP Server Deployment (via DAB) ────────────────────────────────
# Wrapper that deploys the MCP companion server using the project-root
# Databricks Asset Bundle.
#
# Usage:
#   ./deploy-mcp-server.sh              # deploy + run (dev)
#   ./deploy-mcp-server.sh -t prod      # deploy + run (prod)
#   ./deploy-mcp-server.sh --no-run     # deploy without starting

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TARGET="dev"
NO_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--target) TARGET="$2"; shift 2 ;;
        --no-run)    NO_RUN=true; shift ;;
        -h|--help)   sed -n '3,8p' "$0" | sed 's/^# //'; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "=== Deploying mcp-ontobricks via DAB ==="

cd "$PROJECT_ROOT"
exec scripts/deploy.sh --mcp-only -t "$TARGET" ${NO_RUN:+--no-run}
