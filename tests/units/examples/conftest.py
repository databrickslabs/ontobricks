"""Load the standalone ``examples/`` MCP scripts as modules for unit tests."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
EXAMPLES_DIR = REPO_ROOT / "examples"
MCP_SRC = REPO_ROOT / "src" / "mcp-server"

for _path in (EXAMPLES_DIR, MCP_SRC):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))


@pytest.fixture(scope="session")
def mcp_client():
    return importlib.import_module("mcp_client")


@pytest.fixture(scope="session")
def mcp_bench():
    return importlib.import_module("mcp_bench")
