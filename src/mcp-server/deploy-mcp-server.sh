#!/usr/bin/env bash
set -euo pipefail

# ── MCP Server Deployment (via DAB) ────────────────────────────────
# Compatibility wrapper that deploys the shared project-root Databricks Asset
# Bundle (main + MCP apps), then starts both unless --no-run is supplied.
#
# Usage:
#   ./deploy-mcp-server.sh              # configured target from deploy.config.sh
#   ./deploy-mcp-server.sh -t dev       # explicit Volume-only target
#   ./deploy-mcp-server.sh --no-run     # deploy without starting

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TARGET=""
NO_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--target) TARGET="$2"; shift 2 ;;
        --no-run)    NO_RUN=true; shift ;;
        -h|--help)   sed -n '3,8p' "$0" | sed 's/^# //'; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

args=()
if [[ -n "$TARGET" ]]; then
    args+=(-t "$TARGET")
fi
if [[ "$NO_RUN" == true ]]; then
    args+=(--no-run)
fi

echo "=== Deploying OntoBricks + MCP via the shared DAB bundle ==="

cd "$PROJECT_ROOT"
exec scripts/deploy.sh "${args[@]}"
