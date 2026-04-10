"""
Entry point for the mcp-ontobricks server.

Started via: uv run mcp-ontobricks
"""
import argparse

import uvicorn


def main():
    parser = argparse.ArgumentParser(description="Start the OntoBricks MCP server")
    parser.add_argument(
        "--port", type=int, default=8000, help="Port to run the server on (default: 8000)"
    )
    args = parser.parse_args()

    uvicorn.run(
        "server.app:combined_app",
        host="0.0.0.0",
        port=args.port,
    )
