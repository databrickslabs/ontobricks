"""Shared safety checks for user-supplied SPARQL."""

from __future__ import annotations

from rdflib.plugins.sparql.parser import parseQuery, parseUpdate

from back.core.errors import ValidationError


def require_read_only_sparql(query: str) -> str:
    """Return stripped query text after structural SPARQL validation."""
    text = query.strip() if isinstance(query, str) else ""
    if not text:
        raise ValidationError("No SPARQL query provided")

    try:
        parseQuery(text)
        return text
    except Exception:
        pass

    try:
        parseUpdate(text)
    except Exception as exc:
        raise ValidationError("Invalid SPARQL query.") from exc
    else:
        raise ValidationError(
            "SPARQL execution is read-only; mutating operations are not allowed."
        )
