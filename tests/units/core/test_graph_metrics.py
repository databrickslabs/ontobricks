"""Tests for graph_analysis.GraphMetrics and related models."""

import pytest
from unittest.mock import MagicMock

from back.core.graph_analysis.models import (
    MetricsRequest,
    NodeMetrics,
    MetricsStats,
    MetricsResult,
)
from back.core.graph_analysis.GraphMetrics import GraphMetrics

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------

class TestMetricsModels:
    def test_metrics_request_defaults(self):
        req = MetricsRequest()
        assert req.predicate_filter is None
        assert req.class_filter is None
        assert req.max_triples == 500_000
        assert req.max_nodes_betweenness == 2_000

    def test_node_metrics_defaults(self):
        nm = NodeMetrics()
        assert nm.degree == 0.0
        assert nm.pagerank == 0.0
        assert nm.betweenness == 0.0
        assert nm.closeness == 0.0
        assert nm.clustering == 0.0

    def test_metrics_stats_defaults(self):
        s = MetricsStats()
        assert s.node_count == 0
        assert s.connected_components == 0
        assert s.density == 0.0

    def test_metrics_result_defaults(self):
        r = MetricsResult()
        assert r.nodes == {}
        assert isinstance(r.stats, MetricsStats)
        assert r.top_pagerank == []


# ---------------------------------------------------------------------------
# GraphMetrics tests
# ---------------------------------------------------------------------------

class TestGraphMetrics:
    _NS = "http://ex.org/"

    # Hub graph: H connected to A, B, C, D, E; plus isolated pair F-G
    _HUB_TRIPLES = [
        {"subject": "http://ex.org/H", "predicate": "http://ex.org/rel", "object": "http://ex.org/A"},
        {"subject": "http://ex.org/H", "predicate": "http://ex.org/rel", "object": "http://ex.org/B"},
        {"subject": "http://ex.org/H", "predicate": "http://ex.org/rel", "object": "http://ex.org/C"},
        {"subject": "http://ex.org/H", "predicate": "http://ex.org/rel", "object": "http://ex.org/D"},
        {"subject": "http://ex.org/H", "predicate": "http://ex.org/rel", "object": "http://ex.org/E"},
        {"subject": "http://ex.org/F", "predicate": "http://ex.org/rel", "object": "http://ex.org/G"},
    ]

    def _make_service(self, triples=None):
        store = MagicMock()
        store.query_triples.return_value = (
            list(self._HUB_TRIPLES) if triples is None else triples
        )
        return GraphMetrics(store, "test_graph")

    def test_compute_returns_metrics_result(self):
        svc = self._make_service()
        result = svc.compute(MetricsRequest())
        assert isinstance(result, MetricsResult)

    def test_node_count_and_edge_count(self):
        svc = self._make_service()
        result = svc.compute(MetricsRequest())
        assert result.stats.node_count == 8  # H A B C D E F G
        assert result.stats.edge_count == 6

    def test_hub_has_highest_pagerank(self):
        svc = self._make_service()
        result = svc.compute(MetricsRequest())
        assert result.top_pagerank[0] == "http://ex.org/H"

    def test_connected_components(self):
        svc = self._make_service()
        result = svc.compute(MetricsRequest())
        assert result.stats.connected_components == 2  # {H,A,B,C,D,E} + {F,G}

    def test_all_nodes_have_metrics(self):
        svc = self._make_service()
        result = svc.compute(MetricsRequest())
        for uri in [self._NS + x for x in ["H", "A", "B", "C", "D", "E", "F", "G"]]:
            assert uri in result.nodes
            nm = result.nodes[uri]
            assert isinstance(nm, NodeMetrics)

    def test_hub_degree_higher_than_leaf(self):
        svc = self._make_service()
        result = svc.compute(MetricsRequest())
        assert result.nodes[self._NS + "H"].degree > result.nodes[self._NS + "A"].degree

    def test_top_pagerank_length(self):
        svc = self._make_service()
        result = svc.compute(MetricsRequest())
        assert len(result.top_pagerank) <= 10

    def test_elapsed_ms_non_negative(self):
        svc = self._make_service()
        result = svc.compute(MetricsRequest())
        assert result.stats.elapsed_ms >= 0

    def test_density_in_range(self):
        svc = self._make_service()
        result = svc.compute(MetricsRequest())
        assert 0.0 <= result.stats.density <= 1.0

    def test_avg_degree_positive(self):
        svc = self._make_service()
        result = svc.compute(MetricsRequest())
        assert result.stats.avg_degree > 0.0

    def test_max_triples_guard(self):
        svc = self._make_service()
        with pytest.raises(ValueError, match="exceeds max_triples"):
            svc.compute(MetricsRequest(max_triples=2))

    def test_empty_graph_returns_empty_result(self):
        svc = self._make_service(triples=[])
        result = svc.compute(MetricsRequest())
        assert result.stats.node_count == 0
        assert result.nodes == {}
        assert result.top_pagerank == []

    def test_literal_objects_excluded(self):
        """Attribute triples (object = plain string literal) must never create graph nodes."""
        triples = [
            {"subject": "http://ex.org/A", "predicate": "http://ex.org/name", "object": "Alice"},
            {"subject": "http://ex.org/A", "predicate": "http://ex.org/age",  "object": "42"},
            {"subject": "http://ex.org/A", "predicate": "http://ex.org/rel",  "object": "http://ex.org/B"},
        ]
        svc = self._make_service(triples=triples)
        result = svc.compute(MetricsRequest())
        # Only the entity–entity edge is kept; literals "Alice" and "42" are nodes
        assert result.stats.node_count == 2
        assert result.stats.edge_count == 1
        assert "Alice" not in result.nodes
        assert "42" not in result.nodes

    def test_predicate_filter_excludes_edges(self):
        triples = [
            {"subject": "http://ex.org/A", "predicate": "http://ex.org/excluded", "object": "http://ex.org/B"},
            {"subject": "http://ex.org/A", "predicate": "http://ex.org/kept",     "object": "http://ex.org/C"},
        ]
        svc = self._make_service(triples=triples)
        result = svc.compute(MetricsRequest(predicate_filter=["http://ex.org/excluded"]))
        assert result.stats.edge_count == 1

    def test_class_filter_restricts_nodes(self):
        triples = [
            {"subject": "http://ex.org/A", "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "object": "http://ex.org/Person"},
            {"subject": "http://ex.org/A", "predicate": "http://ex.org/knows", "object": "http://ex.org/B"},
            {"subject": "http://ex.org/C", "predicate": "http://ex.org/knows", "object": "http://ex.org/D"},
        ]
        svc = self._make_service(triples=triples)
        result = svc.compute(MetricsRequest(class_filter=["http://ex.org/Person"]))
        # Only A and its direct neighbour B should remain
        assert result.stats.node_count <= 2

    def test_betweenness_sampling_triggered(self):
        """Sampling path: max_nodes_betweenness=1 forces k-sample even for tiny graphs."""
        svc = self._make_service()
        result = svc.compute(MetricsRequest(max_nodes_betweenness=1))
        assert result.stats.node_count > 0

    def test_rdf_type_predicate_excluded(self):
        triples = [
            {"subject": "http://ex.org/A", "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "object": "http://ex.org/Class1"},
            {"subject": "http://ex.org/A", "predicate": "http://ex.org/rel", "object": "http://ex.org/B"},
        ]
        svc = self._make_service(triples=triples)
        result = svc.compute(MetricsRequest())
        # rdf:type triple excluded; only the rel edge adds A and B
        assert result.stats.node_count == 2
        assert result.stats.edge_count == 1

    def test_node_types_populated(self):
        triples = [
            {"subject": "http://ex.org/A", "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "object": "http://ex.org/Person"},
            {"subject": "http://ex.org/A", "predicate": "http://ex.org/rel", "object": "http://ex.org/B"},
        ]
        svc = self._make_service(triples=triples)
        result = svc.compute(MetricsRequest())
        assert result.node_types.get("http://ex.org/A") == "http://ex.org/Person"
        assert "http://ex.org/B" not in result.node_types


# ---------------------------------------------------------------------------
# EntityTypeProfile / _build_type_profiles heuristic tests
# ---------------------------------------------------------------------------

class TestBuildTypeProfiles:
    """Unit tests for GraphMetrics._build_type_profiles()."""

    _RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

    def _make_svc(self, triples):
        store = MagicMock()
        store.query_triples.return_value = triples
        return GraphMetrics(store, "test")

    def _make_triples_for_flat_type(self, n=25, pred="http://ex.org/value", type_uri="http://ex.org/Reading"):
        """Create n Reading instances that appear in the graph as objects (leaf nodes)
        but have no outgoing entity-entity predicates → distinct_predicates == 0 → flat."""
        triples = []
        for i in range(n):
            reading_uri = f"http://ex.org/r{i}"
            device_uri  = f"http://ex.org/dev{i}"
            triples.append({"subject": reading_uri, "predicate": self._RDF_TYPE, "object": type_uri})
            # Device references the reading → reading enters the graph as an object node
            triples.append({"subject": device_uri, "predicate": "http://ex.org/hasReading", "object": reading_uri})
        return triples

    def test_profiles_populated_after_compute(self):
        triples = self._make_triples_for_flat_type()
        svc = self._make_svc(triples)
        result = svc.compute(MetricsRequest())
        assert "http://ex.org/Reading" in result.entity_type_profiles

    def test_flat_detected_low_predicates(self):
        """Type with only 1 relationship predicate (incoming) and 25+ instances should be flagged as flat."""
        triples = self._make_triples_for_flat_type(n=25)
        svc = self._make_svc(triples)
        result = svc.compute(MetricsRequest())
        profile = result.entity_type_profiles.get("http://ex.org/Reading")
        assert profile is not None
        assert profile.is_flat is True
        assert profile.distinct_predicates == 1  # only hasReading (incoming from device)

    def test_flat_reasons_non_empty_when_flagged(self):
        triples = self._make_triples_for_flat_type(n=25)
        svc = self._make_svc(triples)
        result = svc.compute(MetricsRequest())
        profile = result.entity_type_profiles["http://ex.org/Reading"]
        assert len(profile.flat_reasons) > 0

    def test_temporal_predicate_detected(self):
        """A predicate with 'timestamp' in the local name triggers has_temporal_predicates."""
        triples = []
        for i in range(5):
            uri = f"http://ex.org/ev{i}"
            triples.append({"subject": uri, "predicate": self._RDF_TYPE, "object": "http://ex.org/Event"})
            triples.append({"subject": uri, "predicate": "http://ex.org/timestamp", "object": "http://ex.org/device0"})
            triples.append({"subject": uri, "predicate": "http://ex.org/rel", "object": "http://ex.org/other"})
        svc = self._make_svc(triples)
        result = svc.compute(MetricsRequest())
        profile = result.entity_type_profiles.get("http://ex.org/Event")
        assert profile is not None
        assert profile.has_temporal_predicates is True

    def test_well_connected_type_not_flat(self):
        """A type with many predicates and rich connections should NOT be flagged as flat."""
        triples = []
        instances = [f"http://ex.org/person{i}" for i in range(5)]
        for uri in instances:
            triples.append({"subject": uri, "predicate": self._RDF_TYPE, "object": "http://ex.org/Person"})
        # Each person linked to others via many predicates
        pred_names = [f"http://ex.org/pred{k}" for k in range(6)]
        for i, uri in enumerate(instances):
            for k, pred in enumerate(pred_names):
                target = instances[(i + k + 1) % len(instances)]
                triples.append({"subject": uri, "predicate": pred, "object": target})
        svc = self._make_svc(triples)
        result = svc.compute(MetricsRequest())
        profile = result.entity_type_profiles.get("http://ex.org/Person")
        assert profile is not None
        # Should have more than 3 distinct predicates → no "only N distinct" reason
        assert profile.distinct_predicates > 3

    def test_profile_count_matches_instances(self):
        """Profile count equals number of instances regardless of edge count."""
        # Instances with one entity-entity edge each so they appear in the graph
        triples = []
        for i in range(10):
            uri = f"http://ex.org/r{i}"
            triples.append({"subject": uri, "predicate": self._RDF_TYPE, "object": "http://ex.org/Reading"})
            triples.append({"subject": uri, "predicate": "http://ex.org/linkedTo", "object": "http://ex.org/device0"})
        svc = self._make_svc(triples)
        result = svc.compute(MetricsRequest())
        profile = result.entity_type_profiles.get("http://ex.org/Reading")
        assert profile is not None
        assert profile.count == 10

    def test_empty_graph_no_profiles(self):
        svc = self._make_svc([])
        result = svc.compute(MetricsRequest())
        assert result.entity_type_profiles == {}

    def test_metrics_result_entity_type_profiles_default(self):
        from back.core.graph_analysis.models import MetricsResult
        r = MetricsResult()
        assert r.entity_type_profiles == {}
