"""Rewrite the registry store's Postgres SQL so SQLite can run it unchanged.

The Lakebase registry mixins issue plain Postgres SQL through psycopg. The
SQLite store reuses those mixins as they are, so every statement passes
through :meth:`SQLiteDialect.translate` first. Only the constructs the
registry actually uses are handled; anything else reaches SQLite verbatim
and fails loudly, which is what the end-to-end store tests rely on.

Placeholders are positional (``%s``) everywhere in the registry, so the
rewrite keeps their order and reports which positions need a parameter
conversion (a Python list bound to ``= ANY(%s)``, or a value cast with
``::timestamptz``).
"""

from __future__ import annotations

import re
from typing import Dict, FrozenSet, Tuple

from back.core.errors import InfrastructureError

# Postgres auto-names some constraints; SQLite only accepts a column list.
_CONFLICT_CONSTRAINTS: Dict[str, str] = {
    "schedules_type_domain_target_key": "(registry_id, task_type, domain_name, target_key)",
}

_IDENT = r'[\w".]+'
_PLACEHOLDER = re.compile(r"%s")
_ANY_BEFORE = re.compile(r"=\s*ANY\(\s*$", re.IGNORECASE)
_TIMESTAMP_AFTER = re.compile(r"\s*::\s*timestamptz", re.IGNORECASE)

_REWRITES = (
    # DELETE FROM t a USING u b WHERE ...  (SQLite has no DELETE ... USING)
    (
        re.compile(
            rf"DELETE\s+FROM\s+(?P<t>{_IDENT})\s+(?:AS\s+)?(?P<a>\w+)\s+USING\s+"
            rf"(?P<u>{_IDENT})\s+(?:AS\s+)?(?P<b>\w+)\s+WHERE\s+(?P<w>.*)",
            re.IGNORECASE | re.DOTALL,
        ),
        r"DELETE FROM \g<t> WHERE rowid IN (SELECT \g<a>.rowid FROM \g<t> AS \g<a>, "
        r"\g<u> AS \g<b> WHERE \g<w>)",
    ),
    # SQLite needs AS before a table alias in DELETE / UPDATE.
    (
        re.compile(rf"DELETE\s+FROM\s+({_IDENT})\s+(?!WHERE\b|AS\b)(\w+)\s+WHERE\b", re.IGNORECASE),
        r"DELETE FROM \1 AS \2 WHERE",
    ),
    (
        re.compile(rf"UPDATE\s+({_IDENT})\s+(?!SET\b|AS\b)(\w+)\s+SET\b", re.IGNORECASE),
        r"UPDATE \1 AS \2 SET",
    ),
    # ORDER BY string_to_array(v.version, '.')::int[]  ->  numeric-aware sort key
    (
        re.compile(r"string_to_array\(\s*([\w.]+)\s*,\s*'\.'\s*\)\s*::\s*int\[\]", re.IGNORECASE),
        r"version_key(\1)",
    ),
    # jsonb_set(col, '{key}', to_jsonb(%s::text))  ->  json_set(col, '$.key', %s)
    (
        re.compile(
            r"jsonb_set\(\s*([\w.]+)\s*,\s*'\{(\w+)\}'\s*,\s*to_jsonb\(\s*(%s)(?:\s*::\s*\w+)?\s*\)\s*\)",
            re.IGNORECASE,
        ),
        r"json_set(\1, '$.\2', \3)",
    ),
    (re.compile(r"\bjsonb_build_object\(", re.IGNORECASE), "json_object("),
    # now() - col > make_interval(secs => %s)  ->  elapsed seconds > %s
    (
        re.compile(
            rf"now\(\)\s*-\s*({_IDENT})\s*>\s*make_interval\(\s*secs\s*=>\s*(%s)\s*\)",
            re.IGNORECASE,
        ),
        r"(julianday(now()) - julianday(\1)) * 86400.0 > \2",
    ),
    # (array_agg(x ORDER BY a DESC, b DESC))[1]  ->  first_by(x, a, b)
    (
        re.compile(
            r"\(\s*array_agg\(\s*([\w.]+)\s+ORDER\s+BY\s+([\w.]+)\s+DESC\s*,\s*([\w.]+)\s+DESC\s*\)\s*\)\s*\[1\]",
            re.IGNORECASE,
        ),
        r"first_by(\1, \2, \3)",
    ),
    # col = ANY(%s)  ->  col IN (SELECT value FROM json_each(%s))
    (re.compile(r"=\s*ANY\(\s*(%s)\s*\)", re.IGNORECASE), r"IN (SELECT value FROM json_each(\1))"),
)

_ON_CONSTRAINT = re.compile(r"ON\s+CONFLICT\s+ON\s+CONSTRAINT\s+(\w+)", re.IGNORECASE)
_CAST = re.compile(r"\s*::\s*\w+(?:\[\])?")


class SQLiteDialect:
    """Translates the registry's Postgres statements into SQLite statements."""

    @classmethod
    def translate(cls, sql: str) -> Tuple[str, FrozenSet[int], FrozenSet[int]]:
        """Return ``(sqlite_sql, list_positions, timestamp_positions)``.

        Positions are 0-based indexes into the statement's parameters.
        """
        list_positions, timestamp_positions = cls._classify_placeholders(sql)
        out = sql
        for pattern, replacement in _REWRITES:
            out = pattern.sub(replacement, out)
        out = _ON_CONSTRAINT.sub(cls._conflict_columns, out)
        out = _CAST.sub("", out)
        out = _PLACEHOLDER.sub("?", out)
        return out, list_positions, timestamp_positions

    @staticmethod
    def _classify_placeholders(sql: str) -> Tuple[FrozenSet[int], FrozenSet[int]]:
        lists, stamps = set(), set()
        for index, match in enumerate(_PLACEHOLDER.finditer(sql)):
            if _ANY_BEFORE.search(sql[: match.start()]):
                lists.add(index)
            if _TIMESTAMP_AFTER.match(sql, match.end()):
                stamps.add(index)
        return frozenset(lists), frozenset(stamps)

    @staticmethod
    def _conflict_columns(match: "re.Match[str]") -> str:
        name = match.group(1)
        columns = _CONFLICT_CONSTRAINTS.get(name)
        if columns is None:
            raise InfrastructureError(
                "SQLite registry: no column list known for constraint %r" % name
            )
        return "ON CONFLICT " + columns
