"""
Entry point for the mcp-ontobricks server.

Started via: uv run mcp-ontobricks
"""

import argparse
import logging
import os

import uvicorn


def _configure_logging() -> None:
    """Make the MCP server's own loggers visible in Apps log output.

    Without an explicit configuration, ``logging.getLogger(__name__)``
    in ``server.app`` falls back to the root logger at WARNING — every
    INFO line we emit (HTTP requests, registry resolution, OAuth
    fetches) is silently dropped, which makes deployed-app debugging
    impossible. We configure a single stream handler at the level
    requested by ``MCP_LOG_LEVEL`` (default ``INFO``) so those lines
    reach Databricks Apps stdout.
    """
    level_name = os.getenv("MCP_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        force=True,
    )
    logging.getLogger("server").setLevel(level)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def main():
    parser = argparse.ArgumentParser(description="Start the OntoBricks MCP server")
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to run the server on (default: 8000)",
    )
    args = parser.parse_args()

    _configure_logging()

    uvicorn.run(
        "server.app:combined_app",
        host="0.0.0.0",
        port=args.port,
    )
