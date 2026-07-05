"""Unity Catalog SQL identifier validation for Databricks metadata queries."""

from __future__ import annotations

import re

from back.core.errors import ValidationError

# Unity Catalog segment names: letters, digits, underscore, hyphen; leading letter/underscore.
UC_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")


def validate_uc_identifier(name: str, *, role: str = "identifier") -> str:
    """Return *name* when it is a safe Unity Catalog identifier segment."""
    segment = (name or "").strip()
    if not UC_IDENTIFIER_RE.match(segment) or "--" in segment:
        raise ValidationError(f"Invalid UC {role}: {name!r}")
    return segment


def quote_uc_identifier(name: str, *, role: str = "identifier") -> str:
    """Backtick-quote a validated Unity Catalog identifier."""
    validated = validate_uc_identifier(name, role=role)
    return f"`{validated}`"


def quote_uc_fqn(
    catalog: str,
    schema: str,
    table: str | None = None,
) -> str:
    """Return ``catalog.schema`` or ``catalog.schema.table`` with quoted segments."""
    parts = [
        quote_uc_identifier(catalog, role="catalog"),
        quote_uc_identifier(schema, role="schema"),
    ]
    if table is not None:
        parts.append(quote_uc_identifier(table, role="table"))
    return ".".join(parts)
