"""``GraphRelation``: ``…_graph`` as a union VIEW or a materialized view, and its refresh hooks."""

from __future__ import annotations

import pytest

from back.core.graphdb.delta import GraphRelation, MaterializedViewBuilder, materialize

pytestmark = pytest.mark.unit

DATA = "main.ob.triplestore_d_V1_data"
GRAPH = "main.ob.triplestore_d_V1_graph"
INFERRED = "main.ob.triplestore_d_V1_inferred"
_SHA = MaterializedViewBuilder.graph_mv_sha(DATA, INFERRED)


class _Client:
    """Records statements; *shas* is the ``ontobricks.mv_sql_sha256`` per relation."""

    def __init__(self, shas=None, fail_on=None):
        self.shas, self.fail_on = dict(shas or {}), fail_on
        self.statements: list[str] = []
        self.queries: list[str] = []

    def execute_query(self, sql):
        self.queries.append(sql)
        relation = sql.split()[2]
        return [{"key": "ontobricks.mv_sql_sha256", "value": self.shas.get(relation, "")}]

    def execute_statement(self, sql):
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError(f"refused: {self.fail_on}")
        self.statements.append(sql)
        return True


class TestEnsureMaterialized:
    def test_a_view_is_dropped_and_the_view_made_a_materialized_view(self):
        client = _Client()

        assert GraphRelation.ensure_materialized(
            client, graph_fqn=GRAPH, data_fqn=DATA, inferred_fqn=INFERRED
        ) is True

        assert client.statements == [
            f"DROP VIEW IF EXISTS {GRAPH}",
            materialize.build_ensure_inferred_sql(INFERRED),
            materialize.build_enable_row_tracking_sql(INFERRED),
            MaterializedViewBuilder.graph_mv_sql(GRAPH, DATA, INFERRED),
        ]

    def test_a_current_definition_is_kept(self):
        client = _Client({GRAPH: _SHA})

        assert GraphRelation.ensure_materialized(
            client, graph_fqn=GRAPH, data_fqn=DATA, inferred_fqn=INFERRED
        ) is False
        assert client.statements == []

    def test_another_refresh_clause_replaces_it(self):
        client = _Client({GRAPH: _SHA})

        assert GraphRelation.ensure_materialized(
            client,
            graph_fqn=GRAPH,
            data_fqn=DATA,
            inferred_fqn=INFERRED,
            refresh_clause="TRIGGER ON UPDATE",
        ) is True
        assert "  TRIGGER ON UPDATE\n" in client.statements[-1]


class TestTheViewAndTheRefresh:
    def test_the_union_view_is_kept_for_a_view_and_never_replaces_a_materialized_view(self):
        view, mv = _Client(), _Client({GRAPH: _SHA})

        GraphRelation.ensure_view(view, GRAPH, DATA, INFERRED)
        GraphRelation.ensure_view(mv, GRAPH, DATA, INFERRED)

        assert view.statements == [materialize.build_union_view_sql(GRAPH, DATA, INFERRED)]
        assert mv.statements == []

    def test_a_full_path_replaces_a_materialized_view_by_the_union_view(self):
        client = _Client({GRAPH: _SHA})

        GraphRelation.replace_with_view(client, GRAPH, DATA, INFERRED)

        assert client.statements == [
            f"DROP MATERIALIZED VIEW IF EXISTS {GRAPH}",
            materialize.build_union_view_sql(GRAPH, DATA, INFERRED),
        ]

    def test_only_a_materialized_view_is_refreshed(self):
        view, mv = _Client(), _Client({GRAPH: _SHA})

        assert GraphRelation.refresh(view, GRAPH) is False
        assert GraphRelation.refresh(mv, GRAPH) is True
        assert view.statements == [] and mv.statements == [f"REFRESH MATERIALIZED VIEW {GRAPH}"]


class TestRefreshAfterWrite:
    def test_a_write_refreshes_a_materialized_view_asynchronously(self):
        client = _Client({GRAPH: _SHA})

        assert GraphRelation.refresh_after_write(client, INFERRED) is True
        assert client.statements == [f"REFRESH MATERIALIZED VIEW {GRAPH} ASYNC"]

    def test_a_view_needs_no_refresh(self):
        client = _Client()

        assert GraphRelation.refresh_after_write(client, INFERRED) is False
        assert client.statements == []

    def test_a_failing_refresh_never_fails_the_write(self, caplog):
        client = _Client({GRAPH: _SHA}, fail_on="REFRESH")

        with caplog.at_level("WARNING"):
            assert GraphRelation.refresh_after_write(client, INFERRED) is False
        assert "not refreshed after a write" in caplog.text

    @pytest.mark.parametrize("written", ["", DATA, "main.ob.some_table"])
    def test_a_table_that_is_no_inferred_companion_runs_nothing(self, written):
        client = _Client({GRAPH: _SHA})

        assert GraphRelation.refresh_after_write(client, written) is False
        assert client.statements == [] and client.queries == []
