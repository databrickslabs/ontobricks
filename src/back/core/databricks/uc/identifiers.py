"""Unity Catalog SQL identifier validation for Databricks metadata queries."""

from __future__ import annotations

import re

from back.core.errors import ValidationError

# Unity Catalog segment names: letters, digits, underscore, hyphen.
# Leading digits are allowed because identifiers are always backtick-quoted in SQL.
UC_IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_-]*$")


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


# Spark treats `` `catalog.schema.table` `` as one identifier. Match that
# illegal form after FROM / JOIN so column names like `` `customer.id` ``
# are left alone.
_COLLAPSED_UC_FQN_RE = re.compile(
    r"(?i)(\b(?:FROM|JOIN)\s+)`("
    r"[A-Za-z0-9_][A-Za-z0-9_-]*"
    r"\.[A-Za-z0-9_][A-Za-z0-9_-]*"
    r"\.[A-Za-z0-9_][A-Za-z0-9_-]*"
    r")`"
)


def find_collapsed_uc_fqn(sql: str) -> list[str]:
    """Return illegal single-backtick three-level names in *sql*."""
    if not sql:
        return []
    return [f"`{m.group(2)}`" for m in _COLLAPSED_UC_FQN_RE.finditer(sql)]


def rewrite_collapsed_uc_fqn(sql: str) -> str:
    """Rewrite ``FROM `a.b.c` `` to ``FROM `a`.`b`.`c` ``.

    Leaves bare ``catalog.schema.table`` and already-split
    `` `catalog`.`schema`.`table` `` unchanged.
    """
    if not sql:
        return sql

    def _replace(match: re.Match[str]) -> str:
        prefix, fqn = match.group(1), match.group(2)
        catalog, schema, table = fqn.split(".")
        return f"{prefix}{quote_uc_fqn(catalog, schema, table)}"

    return _COLLAPSED_UC_FQN_RE.sub(_replace, sql)
