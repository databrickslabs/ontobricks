#!/bin/bash
# Setup script for OntoBricks

set -e

echo "====================================="
echo "  OntoBricks Setup"
echo "====================================="
echo ""

# Check Python version
echo "Checking Python version..."
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "Python version: $python_version"
echo ""

# Install uv if not present
if ! command -v uv &> /dev/null; then
    echo "Installing uv package manager..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.cargo/bin:$PATH"
    echo ""
fi

# Create virtual environment
echo "Creating virtual environment..."
uv venv
echo ""

# Activate virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate
echo ""

# Install dependencies
echo "Installing dependencies..."
uv pip install -e .
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
echo "To format code: black app/ tests/"
echo ""

