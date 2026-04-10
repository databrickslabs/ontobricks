#!/usr/bin/env python3
"""
OntoBricks MCP Server — standalone entry point (stdio transport).

This script is designed to be launched by LLM clients such as Cursor or
Claude Desktop.  It communicates over stdio and calls back into the
OntoBricks REST API over HTTP.

Configure the OntoBricks base URL via the ONTOBRICKS_URL environment
variable (defaults to http://localhost:8000).

Usage (from the project root):
    python mcp_server.py              # stdio
    python mcp_server.py --http       # streamable-http on port 9100
"""
import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_ROOT, "src"))
sys.path.insert(0, os.path.join(_ROOT, "src", "mcp-server"))

from server.app import create_mcp_server  # noqa: E402

mcp = create_mcp_server(mode="standalone")

if __name__ == "__main__":
    if "--http" in sys.argv:
        port = int(os.getenv("MCP_PORT", "9100"))
        mcp.run(transport="streamable-http", host="0.0.0.0", port=port)
    else:
        mcp.run(transport="stdio")
