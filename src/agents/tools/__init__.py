"""
Shared MCP-style tools for OntoBricks agents.

Each tool is a plain function that receives a ToolContext and returns a
JSON-encoded string.  Tool definitions follow the OpenAI function-calling
schema sent to LLM serving endpoints.

This module provides a convenience import for ToolContext; import it as
``from agents.tools import ToolContext``.
"""

from agents.tools.context import ToolContext  # noqa: F401
