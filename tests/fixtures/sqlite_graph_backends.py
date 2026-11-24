"""In-memory SQL doubles that *execute* the overrides-layer SQL for real.

The Delta (Spark SQL) and Lakebase (Postgres) stores emit SQL the unit tests
cannot send to a warehouse. These doubles translate the few dialect-specific
constructs to SQLite so the stores' **real** statements run against real rows:
an edit goes through the production code path, lands in the ``_overrides``
table, and the effective graph is read back through the production views.

What is translated (and nothing else, so a new construct fails loudly):

* Spark: ``cat.sch.name`` -> ``cat__sch__name``, ``CREATE OR REPLACE VIEW``,
  ``USING DELTA CLUSTER BY``, ``LEFT ANTI JOIN``, ``MERGE ... WHEN MATCHED THEN
  DELETE`` and ``(VALUES ...) AS v(...)``, ``current_timestamp()``,
  ``DESCRIBE DETAIL``, ``CREATE OR REPLACE TABLE ... USING DELTA ... AS`` (the
  search-cache companions) and ``DROP VIEW IF EXISTS`` on a table.
* Postgres: ``%s``, ``::TEXT``, ``now()``, ``CREATE OR REPLACE VIEW``,
  ``CREATE TEMP TABLE ... ON COMMIT DROP``, ``COPY ... FROM STDIN``,
  ``DELETE ... USING``, ``ON CONFLICT DO NOTHING``, ``to_regclass``,
  ``TRUNCATE``, ``ANALYZE``, ``text_pattern_ops``, the ``pg_trgm`` probe and the
  ``information_schema`` existence probe (the search-cache companions).

Limits: SQLite is not Spark or Postgres. Type coercion, ``digest()`` generated
columns and concurrency are not exercised; the point is the *set semantics* of
add / delete / modify and of the views that serve them.
"""

from __future__ import annotations

import re
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Iterator

_SPARK_LITERAL = re.compile(r"'(?:[^'\\]|\\.|'')*'")
_PG_LITERAL = re.compile(r"'(?:[^']|'')*'")
_FQN = re.compile(r"\b([A-Za-z_]\w*)\.([A-Za-z_]\w*)\.([A-Za-z_]\w*)\b")


def _mask(sql: str, pattern: re.Pattern[str]) -> tuple[str, list[str]]:
    """Replace every string literal by a ``'@Ln@'`` placeholder."""
    literals: list[str] = []

    def hide(match: re.Match[str]) -> str:
        literals.append(match.group(0))
        return f"'@L{len(literals) - 1}@'"

    return pattern.sub(hide, sql), literals


def _unmask(sql: str, literals: list[str], *, spark: bool) -> str:
    """Put the literals back, as SQLite string literals."""

    def show(match: re.Match[str]) -> str:
        text = literals[int(match.group(1))]
        # Spark doubles backslashes; SQLite (like Postgres) treats them as plain.
        return text.replace("\\\\", "\\") if spark else text

    return re.sub(r"'@L(\d+)@'", show, sql)


def _rows(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    if cursor.description is None:
        return []
    names = [col[0] for col in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


# ---------------------------------------------------------------------------
# Spark SQL (Delta / Unity Catalog)
# ---------------------------------------------------------------------------


def spark_to_sqlite(sql: str) -> list[str]:
    """Translate one Spark SQL statement into SQLite statement(s)."""
    skeleton, literals = _mask(sql.strip().rstrip(";"), _SPARK_LITERAL)
    stmt = _FQN.sub(lambda m: "__".join(m.groups()), skeleton)
    stmt = stmt.replace("current_timestamp()", "CURRENT_TIMESTAMP")

    view = re.match(r"CREATE OR REPLACE VIEW (\S+) AS (.*)$", stmt, re.S)
    ctas = re.match(
        r"CREATE OR REPLACE TABLE (\S+) USING DELTA(?:\s+CLUSTER BY \([^)]*\))?\s+AS (.*)$",
        stmt,
        re.S,
    )
    if view:
        name, body = view.groups()
        statements = [f"DROP VIEW IF EXISTS {name}", f"CREATE VIEW {name} AS {body}"]
    elif ctas:
        name, body = ctas.groups()
        statements = [f"DROP TABLE IF EXISTS {name}", f"CREATE TABLE {name} AS {body}"]
    else:
        statements = [stmt]

    out: list[str] = []
    for statement in statements:
        if statement.startswith("CREATE TABLE") and " AS " not in statement.split("(")[0]:
            statement = re.sub(r"\s+USING DELTA(\s+CLUSTER BY \([^)]*\))?", "", statement)
            statement = re.sub(r"\bSTRING\b", "TEXT", statement)
            statement = re.sub(r"\bTIMESTAMP\b", "TEXT", statement)
        statement = re.sub(
            r"LEFT ANTI JOIN \((?P<sub>.*)\) d ON (?P<on>.*)$",
            r"WHERE NOT EXISTS (SELECT 1 FROM (\g<sub>) d WHERE \g<on>)",
            statement,
            flags=re.S,
        )
        # ``LEFT ANTI JOIN <cte> ON a.x = b.y`` (expansion visited-set filter).
        statement = re.sub(
            r"LEFT ANTI JOIN (\w+) ON (\w+\.\w+ = \w+\.\w+)",
            r"WHERE NOT EXISTS (SELECT 1 FROM \1 WHERE \2)",
            statement,
        )
        merge = re.match(
            r"MERGE INTO (\S+) t USING \(VALUES (.*)\) AS v\(subject, predicate, object\) "
            r"ON (.*) WHEN MATCHED THEN DELETE$",
            statement,
            re.S,
        )
        if merge:
            table, values, on = merge.groups()
            statement = (
                f"WITH v(subject, predicate, object) AS (VALUES {values}) "
                f"DELETE FROM {table} AS t WHERE EXISTS (SELECT 1 FROM v WHERE {on})"
            )
        else:
            inline = re.search(
                r"\(VALUES (.*)\) AS v\(subject, predicate, object\)", statement, re.S
            )
            if inline and statement.startswith("INSERT"):
                statement = (
                    f"WITH v(subject, predicate, object) AS (VALUES {inline.group(1)}) "
                    + statement.replace(inline.group(0), "v")
                )
        out.append(_unmask(statement, literals, spark=True))
    return out


class SqliteDeltaWarehouse:
    """Warehouse client double (``execute_statement`` / ``execute_query``)."""

    # ``MaterializedStore._uses_inline_sql`` reads this off the client's type:
    # inserts then go through ``execute_statement`` instead of a Thrift session.
    uses_inline_statements = True

    def __init__(self) -> None:
        # The search-cache rebuild runs one CTAS per worker thread.
        self.conn = sqlite3.connect(":memory:", isolation_level=None, check_same_thread=False)
        self._lock = threading.RLock()
        self.statements: list[str] = []

    def execute_statement(self, sql: str) -> None:
        self._run(sql)

    def execute_query(self, sql: str) -> list[dict[str, Any]]:
        return self._run(sql)

    def _run(self, sql: str) -> list[dict[str, Any]]:
        with self._lock:
            return self._run_locked(sql)

    def _run_locked(self, sql: str) -> list[dict[str, Any]]:
        self.statements.append(sql)
        text = sql.strip()
        drop_view = re.match(r"DROP VIEW IF EXISTS (\S+)$", text)
        if drop_view:
            # Spark: a no-op when the name is a table; SQLite would raise.
            name = "__".join(drop_view.group(1).split("."))
            found = self.conn.execute(
                "SELECT type FROM sqlite_master WHERE name = ?", (name,)
            ).fetchone()
            if found is not None and found[0] == "table":
                return []
        detail = re.match(r"DESCRIBE DETAIL (\S+)$", text)
        if detail:
            return self._describe(detail.group(1))
        if re.match(r"(OPTIMIZE|ALTER TABLE .* SET TBLPROPERTIES)", text, re.S):
            return []
        truncate = re.match(r"TRUNCATE TABLE (\S+)$", text)
        if truncate:
            text = f"DELETE FROM {truncate.group(1)}"
        rows: list[dict[str, Any]] = []
        for statement in spark_to_sqlite(text):
            rows = _rows(self.conn.execute(statement))
        return rows

    def _describe(self, fqn: str) -> list[dict[str, Any]]:
        name = "__".join(fqn.split("."))
        row = self.conn.execute(
            "SELECT type FROM sqlite_master WHERE name = ?", (name,)
        ).fetchone()
        if row is None:
            raise RuntimeError(f"[TABLE_OR_VIEW_NOT_FOUND] The table or view {fqn} cannot be found")
        if row[0] == "view":
            raise RuntimeError(
                f"[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'DESCRIBE DETAIL' expects a table but {fqn} is a view"
            )
        return [{"format": "delta"}]

    def rows(self, fqn: str, columns: str = "*") -> list[dict[str, Any]]:
        """Test helper: ``SELECT <columns> FROM <fqn>``."""
        return self._run(f"SELECT {columns} FROM {fqn}")


# ---------------------------------------------------------------------------
# Postgres (Lakebase)
# ---------------------------------------------------------------------------


def postgres_to_sqlite(sql: str) -> tuple[list[str], list[str]]:
    """Translate one Postgres statement. Returns ``(statements, on_commit_drop)``."""
    skeleton, literals = _mask(sql.strip().rstrip(";"), _PG_LITERAL)
    stmt = skeleton.replace("%s", "?").replace("::TEXT", "").replace("now()", "CURRENT_TIMESTAMP")
    dropped: list[str] = []

    view = re.match(r"CREATE OR REPLACE VIEW (\S+) AS (.*)$", stmt, re.S)
    if view:
        name, body = view.groups()
        statements = [f"DROP VIEW IF EXISTS {name}", f"CREATE VIEW {name} AS {body}"]
    else:
        statements = [stmt]

    out: list[str] = []
    for statement in statements:
        truncate = re.match(r"TRUNCATE (\w+)$", statement)
        if truncate:
            statement = f"DELETE FROM {truncate.group(1)}"
        if statement.startswith("ANALYZE "):
            continue
        statement = statement.replace(" text_pattern_ops", "")
        temp = re.match(r"CREATE TEMP TABLE (\w+) ", statement)
        if temp and statement.rstrip().endswith("ON COMMIT DROP"):
            dropped.append(temp.group(1))
            statement = statement.rstrip()[: -len("ON COMMIT DROP")].rstrip()
        statement = re.sub(
            r"DELETE FROM (\w+) (\w+) USING (\w+) (\w+) WHERE (.*)$",
            r"DELETE FROM \1 AS \2 WHERE EXISTS (SELECT 1 FROM \3 AS \4 WHERE \5)",
            statement,
            flags=re.S,
        )
        # SQLite cannot tell ``ON CONFLICT`` from a join constraint after a bare FROM.
        statement = re.sub(r"(FROM \w+) ON CONFLICT", r"\1 WHERE true ON CONFLICT", statement)
        out.append(_unmask(statement, literals, spark=False))
    return out, dropped


class _CopyWriter:
    def __init__(self, cursor: "SqlitePgCursor", table: str, columns: list[str]) -> None:
        self._cursor = cursor
        self._insert = (
            f"INSERT INTO {table} ({', '.join(columns)}) "
            f"VALUES ({', '.join('?' for _ in columns)})"
        )

    def write_row(self, row: tuple[Any, ...]) -> None:
        self._cursor.conn.execute(self._insert, row)

    def __enter__(self) -> "_CopyWriter":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False


class SqlitePgCursor:
    """The slice of a psycopg ``dict_row`` cursor the Lakebase store uses."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.rowcount = -1
        self.on_commit_drop: list[str] = []
        self._cursor: sqlite3.Cursor | None = None

    @property
    def description(self) -> Any:
        return self._cursor.description if self._cursor is not None else None

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> "SqlitePgCursor":
        text = sql.strip()
        if text.startswith(("SET ", "RESET ")):
            return self
        if text.startswith("DO "):
            return self  # the pg_trgm CREATE EXTENSION block
        if "pg_extension" in text:
            self._cursor = self.conn.execute("SELECT 1 AS found WHERE 0")
            return self
        if "information_schema.tables" in text and params:
            return self._regclass(params[0])  # ``FlatStore.table_exists``
        if "to_regclass" in text:
            return self._regclass((params or ("",))[0])
        statements, dropped = postgres_to_sqlite(text)
        self.on_commit_drop.extend(dropped)
        for statement in statements:
            self._cursor = self.conn.execute(statement, params or ())
        self.rowcount = self._cursor.rowcount if self._cursor is not None else -1
        return self

    def _regclass(self, relation: str) -> "SqlitePgCursor":
        name = relation.split(".")[-1].strip('"')
        found = self.conn.execute(
            "SELECT 1 AS found FROM sqlite_master WHERE name = ?", (name,)
        )
        self._cursor = found
        return self

    def copy(self, sql: str) -> _CopyWriter:
        match = re.match(r"COPY (\w+) \((.*?)\) FROM STDIN", sql.strip())
        if match is None:
            raise AssertionError(f"unsupported COPY statement: {sql}")
        columns = [c.strip() for c in match.group(2).split(",")]
        return _CopyWriter(self, match.group(1), columns)

    def _as_dicts(self, rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
        names = [col[0] for col in (self.description or [])]
        return [dict(zip(names, row)) for row in rows]

    def fetchone(self) -> dict[str, Any] | None:
        if self._cursor is None:
            return None
        row = self._cursor.fetchone()
        return self._as_dicts([row])[0] if row is not None else None

    def fetchall(self) -> list[dict[str, Any]]:
        return self._as_dicts(self._cursor.fetchall()) if self._cursor is not None else []


class SqlitePostgres:
    """Connection double: autocommit cursors and explicit transactions."""

    def __init__(self) -> None:
        self.conn = sqlite3.connect(":memory:", isolation_level=None)

    def run(self, sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
        cur = SqlitePgCursor(self.conn).execute(sql, params)
        return cur.fetchall()

    def query(self, sql: str) -> list[dict[str, Any]]:
        return self.run(sql)

    @contextmanager
    def cursor(self) -> Iterator[SqlitePgCursor]:
        yield SqlitePgCursor(self.conn)

    @contextmanager
    def transaction(self) -> Iterator[tuple[None, SqlitePgCursor]]:
        cur = SqlitePgCursor(self.conn)
        self.conn.execute("BEGIN")
        try:
            yield None, cur
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")
        finally:
            for table in cur.on_commit_drop:
                self.conn.execute(f"DROP TABLE IF EXISTS {table}")
