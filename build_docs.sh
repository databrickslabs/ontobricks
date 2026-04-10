#!/usr/bin/env bash
#
# build_docs.sh -- Build the OntoBricks Sphinx documentation.
#
# Usage:
#   ./build_docs.sh          # build HTML docs
#   ./build_docs.sh clean    # remove previous build artifacts
#   ./build_docs.sh open     # build and open in the default browser
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SPHINX_DIR="$SCRIPT_DIR/docs/sphinx"
BUILD_DIR="$SPHINX_DIR/_build"

# Prefer the project venv if it exists
if [[ -f "$SCRIPT_DIR/.venv/bin/python" ]]; then
    PYTHON="$SCRIPT_DIR/.venv/bin/python"
else
    PYTHON="python"
fi

# ---------- helpers ----------------------------------------------------------

find_sphinx_build() {
    # 1. venv / pip-installed
    if "$PYTHON" -c "import sphinx.application" 2>/dev/null; then
        SPHINX_CMD=("$PYTHON" -m sphinx)
        return
    fi
    # 2. Homebrew (macOS)
    local brew_sphinx
    brew_sphinx="$(brew --prefix sphinx-doc 2>/dev/null)/bin/sphinx-build"
    if [[ -x "${brew_sphinx:-}" ]]; then
        SPHINX_CMD=("$brew_sphinx")
        return
    fi
    # 3. System PATH
    if command -v sphinx-build &>/dev/null; then
        SPHINX_CMD=(sphinx-build)
        return
    fi
    echo "ERROR: Sphinx is not installed."
    echo "Install it with one of:"
    echo "  brew install sphinx-doc"
    echo "  pip install sphinx sphinx-autodoc-typehints"
    echo "  uv pip install sphinx sphinx-autodoc-typehints"
    exit 1
}

# ---------- main -------------------------------------------------------------

if [[ "${1:-}" == "clean" ]]; then
    echo "Cleaning build directory..."
    rm -rf "$BUILD_DIR"
    echo "Done."
    exit 0
fi

find_sphinx_build

# Narrative docs use MyST to include ../../docs/*.md — install if missing:
if ! "$PYTHON" -c "import myst_parser" 2>/dev/null; then
    echo "Installing Sphinx dependencies (sphinx, myst-parser)..."
    "$PYTHON" -m pip install -q "sphinx>=7" "myst-parser>=3" || {
        echo "ERROR: myst-parser is required for Topic guides. Run: uv sync   or   pip install myst-parser"
        exit 1
    }
fi

echo "Building OntoBricks documentation..."
"${SPHINX_CMD[@]}" -b html "$SPHINX_DIR" "$BUILD_DIR/html" --keep-going -q

echo ""
echo "Documentation built successfully."
echo "Open: file://$BUILD_DIR/html/index.html"

if [[ "${1:-}" == "open" ]]; then
    open "$BUILD_DIR/html/index.html" 2>/dev/null \
        || xdg-open "$BUILD_DIR/html/index.html" 2>/dev/null \
        || echo "Open the above URL in your browser."
fi
