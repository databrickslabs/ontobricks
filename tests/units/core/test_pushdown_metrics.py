"""Tests for the SQL-pushdown graph analytics path.

The generated SQL is executed for real against SQLite (with ``least`` /
``greatest`` registered, the only two functions it lacks), so these tests
validate the queries themselves rather than asserting on SQL substrings. The
key test is :class:`TestPushdownParity`, which runs the SQL path and the
NetworkX path over the same fixture and asserts they agree on every metric
both of them compute.

Dialect caveat: SQLite is not Spark SQL or Postgres. These tests catch logic
and shape errors, not engine-specific incompatibilities.
"""

import sqlite3
from typing import Any, Dict, List, Optional

import pytest

from back.core.graph_analysis.GraphBuilder import _DEFAULT_EXCLUDED_PREDICATES
from back.core.graph_analysis.GraphMetrics import GraphMetrics
from back.core.graph_analysis.PushdownMetrics import (
    UNAVAILABLE_METRICS,
    PushdownMetrics,
    supports_pushdown,
)
from back.core.graph_analysis.models import MODE_PUSHDOWN, MetricsRequest
from back.core.graphdb.GraphDBBackend import GraphDBBackend
from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL

pytestmark = pytest.mark.unit

NS = "http://ex.org/"
CUSTOMER = NS + "Customer"
ORDER = NS + "Order"
BUYS = NS + "buys"
NAME = NS + "name"
CREATED_AT = NS + "createdAt"


class SqliteStore(GraphDBBackend):
    """A real SQL backend over SQLite, used to execute the generated queries."""

    def __init__(self, triples: List[Dict[str, str]]) -> None:
        self._conn = sqlite3.connect(":memory:")
        self._conn.row_factory = sqlite3.Row
        # SQLite has no LEAST/GREATEST; every other construct the analytics
        # queries use is standard and supported as-is.
        self._conn.create_function("least", 2, lambda a, b: min(a, b))
        self._conn.create_function("greatest", 2, lambda a, b: max(a, b))
        self._conn.execute(
            "CREATE TABLE triples (subject TEXT, predicate TEXT, object TEXT)"
        )
        self._conn.executemany(
            "INSERT INTO triples VALUES (?, ?, ?)",
            [(t["subject"], t["predicate"], t["object"]) for t in triples],
        )
        self._conn.commit()

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        return [dict(r) for r in self._conn.execute(query).fetchall()]

    def query_triples(self, table_name: str) -> List[Dict[str, str]]:
        return self.execute_query(
            f"SELECT subject, predicate, object FROM {table_name}"
        )

    # -- unused abstract surface -------------------------------------------
    def create_table(self, table_name: str) -> None: ...
    def drop_table(self, table_name: str) -> None: ...
    def insert_triples(self, table_name, triples, batch_size=500, on_progress=None) -> int:
        return 0
    def count_triples(self, table_name: str) -> int:
        return len(self.query_triples(table_name))
    def table_exists(self, table_name: str) -> bool:
        return True
    def get_status(self, table_name: str) -> Dict[str, Any]:
        return {}
    def get_connection(self) -> Any:
        return self._conn
    def close(self) -> None:
        self._conn.close()


def _hub_triples() -> List[Dict[str, str]]:
    """A hub graph with types, labels and one literal attribute.

    ``C0`` buys five orders; ``C1`` buys one. Deliberately free of
    self-loops, which the SQL path drops and NetworkX would count twice.
    """
    triples: List[Dict[str, str]] = []
    customers = [f"{NS}C0", f"{NS}C1"]
    for c in customers:
        triples.append({"subject": c, "predicate": RDF_TYPE, "object": CUSTOMER})
        triples.append({"subject": c, "predicate": RDFS_LABEL, "object": f"label-{c[-2:]}"})
        triples.append({"subject": c, "predicate": NAME, "object": "a literal"})
    for i in range(6):
        o = f"{NS}O{i}"
        triples.append({"subject": o, "predicate": RDF_TYPE, "object": ORDER})
        triples.append({"subject": o, "predicate": CREATED_AT, "object": "2026-01-01"})
    for i in range(5):
        triples.append({"subject": f"{NS}C0", "predicate": BUYS, "object": f"{NS}O{i}"})
    triples.append({"subject": f"{NS}C1", "predicate": BUYS, "object": f"{NS}O5"})
    return triples


def _excluded() -> List[str]:
    return sorted(_DEFAULT_EXCLUDED_PREDICATES)


class TestSupportsPushdown:
    def test_sql_backend_supported(self):
        assert supports_pushdown(SqliteStore([])) is True

    def test_none_not_supported(self):
        assert supports_pushdown(None) is False

    def test_cypher_backend_not_supported(self):
        # A Cypher engine inherits the SQL method names but cannot execute
        # them, so it must be excluded on dialect alone.
        class CypherStore(SqliteStore):
            @property
            def query_dialect(self) -> str:
                return "cypher"

        assert supports_pushdown(CypherStore([])) is False

    def test_store_missing_methods_not_supported(self):
        class Bare:
            query_dialect = "sql"

        assert supports_pushdown(Bare()) is False


class TestStructureStats:
    def test_counts_entity_entity_edges_only(self):
        store = SqliteStore(_hub_triples())
        stats = store.get_graph_structure_stats(
            "triples", excluded_predicates=_excluded()
        )
        # 6 buys edges; literals, rdf:type and rdfs:label are not edges.
        assert stats["edge_count"] == 6
        # C0, C1 and O0..O5 == 8 nodes.
        assert stats["graph_node_count"] == 8
        assert stats["node_count"] == 8

    def test_self_loops_excluded(self):
        triples = _hub_triples() + [
            {"subject": f"{NS}C0", "predicate": BUYS, "object": f"{NS}C0"}
        ]
        store = SqliteStore(triples)
        stats = store.get_graph_structure_stats(
            "triples", excluded_predicates=_excluded()
        )
        assert stats["edge_count"] == 6

    def test_duplicate_edges_collapse(self):
        triples = _hub_triples() + [
            {"subject": f"{NS}O0", "predicate": BUYS, "object": f"{NS}C0"}
        ]
        store = SqliteStore(triples)
        stats = store.get_graph_structure_stats(
            "triples", excluded_predicates=_excluded()
        )
        # The reverse direction of an existing edge is the same undirected edge.
        assert stats["edge_count"] == 6

    def test_class_filter_scopes_node_count_to_instances(self):
        store = SqliteStore(_hub_triples())
        stats = store.get_graph_structure_stats(
            "triples", excluded_predicates=_excluded(), class_filter=[CUSTOMER]
        )
        assert stats["node_count"] == 2           # C0, C1
        assert stats["graph_node_count"] == 8     # plus their order neighbours
        assert stats["edge_count"] == 6

    def test_empty_graph(self):
        store = SqliteStore([])
        stats = store.get_graph_structure_stats(
            "triples", excluded_predicates=_excluded()
        )
        assert stats == {"edge_count": 0, "graph_node_count": 0, "node_count": 0}


class TestTopNodesByDegree:
    def test_hub_ranks_first_with_label_and_type(self):
        store = SqliteStore(_hub_triples())
        rows = store.get_top_nodes_by_degree(
            "triples", excluded_predicates=_excluded(), top_n=3
        )
        assert rows[0]["node_uri"] == f"{NS}C0"
        assert rows[0]["degree"] == 5
        assert rows[0]["label"] == "label-C0"
        assert rows[0]["type_uri"] == CUSTOMER

    def test_respects_top_n(self):
        store = SqliteStore(_hub_triples())
        rows = store.get_top_nodes_by_degree(
            "triples", excluded_predicates=_excluded(), top_n=2
        )
        assert len(rows) == 2

    def test_class_filter_restricts_returned_nodes(self):
        store = SqliteStore(_hub_triples())
        rows = store.get_top_nodes_by_degree(
            "triples",
            excluded_predicates=_excluded(),
            class_filter=[CUSTOMER],
            top_n=50,
        )
        assert {r["node_uri"] for r in rows} == {f"{NS}C0", f"{NS}C1"}


class TestTypeAggregations:
    def test_edge_stats_per_type(self):
        store = SqliteStore(_hub_triples())
        rows = {
            r["type_uri"]: r
            for r in store.get_type_edge_stats(
                "triples", excluded_predicates=_excluded()
            )
        }
        assert rows[CUSTOMER]["connected_count"] == 2
        assert rows[CUSTOMER]["degree_sum"] == 6      # 5 + 1
        assert rows[ORDER]["connected_count"] == 6
        assert rows[ORDER]["degree_sum"] == 6         # one edge each

    def test_predicate_pairs_include_both_directions(self):
        store = SqliteStore(_hub_triples())
        pairs = store.get_type_predicate_pairs(
            "triples", excluded_predicates=_excluded()
        )
        by_type: Dict[str, set] = {}
        for r in pairs:
            by_type.setdefault(r["type_uri"], set()).add(r["predicate"])
        # Customer is the subject of buys and name.
        assert by_type[CUSTOMER] == {BUYS, NAME}
        # Order is the *object* of buys, and the subject of createdAt.
        assert by_type[ORDER] == {BUYS, CREATED_AT}

    def test_excluded_predicates_absent(self):
        store = SqliteStore(_hub_triples())
        pairs = store.get_type_predicate_pairs(
            "triples", excluded_predicates=_excluded()
        )
        assert all(r["predicate"] not in _DEFAULT_EXCLUDED_PREDICATES for r in pairs)


class TestPushdownMetricsResult:
    def _compute(self, request=None, top_n=100):
        store = SqliteStore(_hub_triples())
        return PushdownMetrics(store, "triples", top_n=top_n).compute(
            request or MetricsRequest()
        )

    def test_mode_and_unavailable_metrics(self):
        result = self._compute()
        assert result.mode == MODE_PUSHDOWN
        assert set(result.unavailable_metrics) == set(UNAVAILABLE_METRICS)

    def test_component_count_reported_as_unknown(self):
        # Counting components needs an iterative pass; report None rather
        # than a wrong number the UI would render as a real value.
        assert self._compute().stats.connected_components is None

    def test_nodes_bounded_by_top_n(self):
        result = self._compute(top_n=3)
        assert len(result.nodes) == 3
        # ...while the reported total stays the true node count.
        assert result.stats.node_count == 8

    def test_degree_is_normalised(self):
        result = self._compute()
        # C0 has 5 of 7 possible neighbours.
        assert result.nodes[f"{NS}C0"].degree == pytest.approx(5 / 7, abs=1e-6)

    def test_unavailable_metrics_are_zero(self):
        nm = self._compute().nodes[f"{NS}C0"]
        for key in UNAVAILABLE_METRICS:
            assert getattr(nm, key) == 0.0

    def test_labels_and_types_populated(self):
        result = self._compute()
        assert result.node_labels[f"{NS}C0"] == "label-C0"
        assert result.node_types[f"{NS}C0"] == CUSTOMER

    def test_empty_graph_returns_empty_result(self):
        store = SqliteStore([])
        result = PushdownMetrics(store, "triples").compute(MetricsRequest())
        assert result.nodes == {}
        assert result.stats.node_count == 0
        assert result.mode == MODE_PUSHDOWN

    def test_temporal_predicate_detected_on_order(self):
        profiles = self._compute().entity_type_profiles
        assert profiles[ORDER].has_temporal_predicates is True

    def test_serializable(self):
        payload = self._compute().to_dict()
        assert payload["mode"] == MODE_PUSHDOWN
        assert payload["stats"]["connected_components"] is None
        assert "unavailable_metrics" in payload


class TestPushdownParity:
    """The SQL path must agree with NetworkX on every shared metric."""

    def _both(self, request=None, top_n=100):
        request = request or MetricsRequest()
        triples = _hub_triples()
        nx_result = GraphMetrics(SqliteStore(triples), "triples").compute(request)
        sql_result = PushdownMetrics(
            SqliteStore(triples), "triples", top_n=top_n
        ).compute(request)
        return nx_result, sql_result

    def test_structure_stats_match(self):
        nx_result, sql_result = self._both()
        assert sql_result.stats.graph_node_count == nx_result.stats.graph_node_count
        assert sql_result.stats.edge_count == nx_result.stats.edge_count
        assert sql_result.stats.node_count == nx_result.stats.node_count
        assert sql_result.stats.avg_degree == pytest.approx(
            nx_result.stats.avg_degree, abs=1e-4
        )
        assert sql_result.stats.density == pytest.approx(
            nx_result.stats.density, abs=1e-6
        )

    def test_degree_centrality_matches_for_returned_nodes(self):
        nx_result, sql_result = self._both()
        assert sql_result.nodes
        for uri, metrics in sql_result.nodes.items():
            assert metrics.degree == pytest.approx(
                nx_result.nodes[uri].degree, abs=1e-6
            )

    def test_type_profiles_match(self):
        nx_result, sql_result = self._both()
        assert set(sql_result.entity_type_profiles) == set(
            nx_result.entity_type_profiles
        )
        for uri, sql_profile in sql_result.entity_type_profiles.items():
            nx_profile = nx_result.entity_type_profiles[uri]
            assert sql_profile.count == nx_profile.count
            assert sql_profile.distinct_predicates == nx_profile.distinct_predicates
            assert sql_profile.has_temporal_predicates == (
                nx_profile.has_temporal_predicates
            )
            assert sql_profile.is_flat == nx_profile.is_flat
            assert sql_profile.flat_reasons == nx_profile.flat_reasons
            assert sql_profile.avg_degree == pytest.approx(
                nx_profile.avg_degree, abs=1e-6
            )

    def test_class_filtered_stats_match(self):
        request = MetricsRequest(class_filter=[CUSTOMER])
        nx_result, sql_result = self._both(request)
        assert sql_result.stats.node_count == nx_result.stats.node_count
        assert sql_result.stats.graph_node_count == nx_result.stats.graph_node_count
        assert sql_result.stats.edge_count == nx_result.stats.edge_count

    def test_class_filtered_profiles_cover_same_types(self):
        request = MetricsRequest(class_filter=[CUSTOMER])
        nx_result, sql_result = self._both(request)
        assert set(sql_result.entity_type_profiles) == set(
            nx_result.entity_type_profiles
        )


class TestAnalysisTriplePushdown:
    """The pushed-down triple selection must match its Python oracle."""

    def _sql(self, **kwargs) -> List[Dict[str, str]]:
        store = SqliteStore(_hub_triples())
        return store.query_triples_for_analysis("triples", **kwargs)

    def _python(self, **kwargs) -> List[Dict[str, str]]:
        return GraphDBBackend._filter_analysis_triples_in_python(
            _hub_triples(), **kwargs
        )

    @staticmethod
    def _key(rows):
        return sorted((r["subject"], r["predicate"], r["object"]) for r in rows)

    def test_no_filter_returns_everything(self):
        assert len(self._sql()) == len(_hub_triples())

    def test_class_filter_matches_python_oracle(self):
        kwargs = {"class_filter": [CUSTOMER]}
        assert self._key(self._sql(**kwargs)) == self._key(self._python(**kwargs))

    def test_predicate_filter_matches_python_oracle(self):
        kwargs = {"predicate_filter": [BUYS]}
        assert self._key(self._sql(**kwargs)) == self._key(self._python(**kwargs))

    def test_combined_filters_match_python_oracle(self):
        kwargs = {"class_filter": [CUSTOMER], "predicate_filter": [NAME]}
        assert self._key(self._sql(**kwargs)) == self._key(self._python(**kwargs))

    def test_class_filter_reduces_volume(self):
        # The point of the pushdown: a filtered analysis reads less. Needs a
        # graph with entities outside the selected subgraph — in the plain hub
        # fixture every node is either a Customer or a Customer's neighbour.
        island = [
            {"subject": f"{NS}X{i}", "predicate": RDF_TYPE, "object": NS + "Widget"}
            for i in range(20)
        ]
        store = SqliteStore(_hub_triples() + island)
        scoped = store.query_triples_for_analysis(
            "triples", class_filter=[CUSTOMER]
        )
        assert len(scoped) < len(_hub_triples()) + len(island)
        assert not any(r["subject"].startswith(f"{NS}X") for r in scoped)

    def test_type_and_label_survive_predicate_filter(self):
        # They never become edges but the analysis needs them for node types
        # and display labels, so excluding them must not drop them.
        rows = self._sql(predicate_filter=[RDF_TYPE, RDFS_LABEL])
        assert any(r["predicate"] == RDF_TYPE for r in rows)
        assert any(r["predicate"] == RDFS_LABEL for r in rows)
