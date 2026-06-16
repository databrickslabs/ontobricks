"""Mock SQL Warehouse with row-injection + query recording."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


@dataclass
class MockSQLWarehouse:
    """In-memory SQL Warehouse stand-in.

    Behaviour:
    - `inject(table, rows)` — preload rows under a fully-qualified `catalog.schema.table` key.
    - `execute(sql)` — returns rows for `SELECT * FROM catalog.schema.table`-shape queries.
      Returns `[]` for any non-matching query (call `assert_queried` to inspect).
    - `executed_queries` — list of every SQL string that was run.
    - `raise_on(sql_substring)` — configure to raise on a query substring (for error-path tests).

    Not a full SQL engine — intentionally. If you need joins/filters, the test
    is probably better expressed against an in-memory DuckDB.
    """

    rows: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    executed_queries: list[str] = field(default_factory=list)
    _error_substrings: list[tuple[str, Exception]] = field(default_factory=list)

    def inject(self, table: str, rows: list[dict[str, Any]]) -> None:
        self.rows[table] = list(rows)

    def raise_on(self, substring: str, exc: Exception | None = None) -> None:
        self._error_substrings.append((substring, exc or RuntimeError(f"forced error on: {substring}")))

    def execute(self, sql: str) -> list[dict[str, Any]]:
        self.executed_queries.append(sql)
        for needle, exc in self._error_substrings:
            if needle in sql:
                raise exc
        match = re.search(
            r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)",
            sql,
            re.IGNORECASE,
        )
        if not match:
            return []
        table = match.group(1)
        return list(self.rows.get(table, []))

    def assert_queried(self, substring: str) -> None:
        for q in self.executed_queries:
            if substring in q:
                return
        raise AssertionError(
            f"Expected a query containing {substring!r}; saw {len(self.executed_queries)} queries: "
            + "; ".join(q[:60] for q in self.executed_queries)
        )
