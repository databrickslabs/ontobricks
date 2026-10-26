"""Outcome of submitting one document to the parse service."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from back.core.databricks.document.ParseStatus import ParseStatus


@dataclass(frozen=True)
class ParseSubmission:
    """Result of preparing an upload or retry for parsing."""

    filename: str
    uploaded: bool
    no_op: bool
    should_parse: bool
    parse_status: ParseStatus
    error: Optional[str] = None
