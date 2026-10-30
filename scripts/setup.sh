#!/usr/bin/env bash
# Setup script for OntoBricks

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "====================================="
echo "  OntoBricks Setup"
echo "====================================="
echo ""

# Install uv before the local preflight so Apple python3 3.9 is not a blocker:
# uv provisions a CPython matching requires-python (>=3.11).
if ! command -v uv &> /dev/null; then
    echo "Installing uv package manager..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
    echo ""
fi

chmod +x scripts/_internal/check-deploy-prerequisites.sh
scripts/_internal/check-deploy-prerequisites.sh --local

# Create virtual environment
echo "Creating virtual environment..."
uv venv
echo ""

# Activate virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate
echo ""

# Install dependencies (including lakebase and pitfalls extras for local dev;
# neo4j is a core dependency in pyproject.toml)
echo "Installing dependencies..."
uv sync --frozen --extra lakebase --extra pitfalls
echo ""

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo ""
    echo "⚠️  IMPORTANT: Please edit .env file with your Databricks credentials"
    echo ""
fi

echo "====================================="
echo "  Setup Complete!"
echo "====================================="
echo ""
echo "Next steps:"
echo "  1. Edit .env file with your Databricks credentials"
echo "  2. Activate virtual environment: source .venv/bin/activate"
echo "  3. Run the application: python run.py"
echo "  4. Open browser to: http://localhost:8000"
echo ""
echo "To run tests: pytest"
echo "To format code: black src/ tests/"
echo ""

