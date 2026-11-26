"""SQL shape tests for the SQLGraphReasoningOps mixin (inverse expansion)."""

from back.core.graphdb.SQLGraphReasoningOps import SQLGraphReasoningOps


class _FakeSQLStore(SQLGraphReasoningOps):
    """Minimal host providing the three methods the mixin relies on."""

    def __init__(self, rows=None, error=None):
        self._rows = rows or []
        self._error = error
        self.last_sql = None

    def _sql_escape(self, value: str) -> str:
        return value.replace("'", "''")

    def _sql_relation(self, table_name: str) -> str:
        return table_name

    def execute_query(self, query: str):
        self.last_sql = query
        if self._error is not None:
            raise self._error
        return self._rows


def test_inverse_expand_emits_inverse_predicate_and_returns_rows():
    rows = [
        {
            "subject": "http://ex.org/b",
            "predicate": "http://ex.org/hasWinery",
            "object": "http://ex.org/a",
        }
    ]
    store = _FakeSQLStore(rows=rows)

    out = store.inverse_expand(
        "g._graph", "http://ex.org/locatedInRegion", "http://ex.org/hasWinery"
    )

    assert out == rows
    sql = store.last_sql
    # Driven by the source predicate, emits the inverse as the written predicate.
    assert "t.predicate = 'http://ex.org/locatedInRegion'" in sql
    assert "'http://ex.org/hasWinery' AS predicate" in sql
    # Anti-join guards against a pre-existing inverse triple.
    assert "NOT EXISTS" in sql
    assert "inv.predicate = 'http://ex.org/hasWinery'" in sql


def test_inverse_expand_swallows_backend_error():
    store = _FakeSQLStore(error=RuntimeError("SQL boom"))

    out = store.inverse_expand("g._graph", "http://ex.org/p", "http://ex.org/q")

    assert out == []
