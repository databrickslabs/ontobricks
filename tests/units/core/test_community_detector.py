"""Tests for graph_analysis.CommunityDetector and models."""

import pytest
from unittest.mock import MagicMock

from back.core.graph_analysis.models import (
    ClusterRequest,
    ClusterResult,
    DetectionStats,
    DetectionResult,
)
from back.core.graph_analysis.CommunityDetector import CommunityDetector


class TestGraphAnalysisModels:
    def test_cluster_request_defaults(self):
        req = ClusterRequest()
        assert req.algorithm == "louvain"
        assert req.resolution == 1.0
        assert req.predicate_filter is None
        assert req.class_filter is None
        assert req.max_triples == 500_000

    def test_cluster_result(self):
        cr = ClusterResult(id=0, members=["a", "b"], size=2)
        assert cr.id == 0
        assert len(cr.members) == 2

    def test_detection_stats_defaults(self):
        stats = DetectionStats()
        assert stats.node_count == 0
        assert stats.modularity == 0.0

    def test_detection_result_defaults(self):
        result = DetectionResult()
        assert result.clusters == []
        assert isinstance(result.stats, DetectionStats)


class TestCommunityDetector:
    @staticmethod
    def _make_triples():
        return [
            {"subject": "A", "predicate": "knows", "object": "B"},
            {"subject": "B", "predicate": "knows", "object": "C"},
            {"subject": "C", "predicate": "knows", "object": "A"},
            {"subject": "D", "predicate": "knows", "object": "E"},
            {"subject": "E", "predicate": "knows", "object": "F"},
            {"subject": "F", "predicate": "knows", "object": "D"},
        ]

    def _make_detector(self, triples=None):
        store = MagicMock()
        store.query_triples.return_value = self._make_triples() if triples is None else triples
        return CommunityDetector(store, "test_graph")

    def test_unsupported_algorithm(self):
        detector = self._make_detector()
        with pytest.raises(ValueError, match="Unsupported algorithm"):
            detector.detect(ClusterRequest(algorithm="unknown"))

    def test_louvain(self):
        detector = self._make_detector()
        result = detector.detect(ClusterRequest(algorithm="louvain"))
        assert isinstance(result, DetectionResult)
        assert result.stats.algorithm == "louvain"
        assert result.stats.node_count > 0
        assert result.stats.edge_count > 0
        assert len(result.clusters) >= 1

    def test_label_propagation(self):
        detector = self._make_detector()
        result = detector.detect(ClusterRequest(algorithm="label_propagation"))
        assert result.stats.algorithm == "label_propagation"
        assert len(result.clusters) >= 1

    def test_greedy_modularity(self):
        detector = self._make_detector()
        result = detector.detect(ClusterRequest(algorithm="greedy_modularity"))
        assert result.stats.algorithm == "greedy_modularity"

    def test_empty_graph(self):
        detector = self._make_detector(triples=[])
        result = detector.detect(ClusterRequest())
        assert result.stats.node_count == 0
        assert result.clusters == []

    def test_max_triples_exceeded(self):
        detector = self._make_detector()
        with pytest.raises(ValueError, match="exceeds max_triples"):
            detector.detect(ClusterRequest(max_triples=2))

    def test_excludes_rdf_type_predicate(self):
        triples = [
            {"subject": "A", "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "object": "Class1"},
            {"subject": "A", "predicate": "knows", "object": "B"},
        ]
        detector = self._make_detector(triples)
        result = detector.detect(ClusterRequest())
        assert result.stats.node_count == 2

    def test_class_filter(self):
        triples = [
            {"subject": "A", "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "object": "Person"},
            {"subject": "A", "predicate": "knows", "object": "B"},
            {"subject": "C", "predicate": "knows", "object": "D"},
        ]
        detector = self._make_detector(triples)
        result = detector.detect(ClusterRequest(class_filter=["Person"]))
        node_count = result.stats.node_count
        assert node_count <= 2

    def test_predicate_filter(self):
        triples = [
            {"subject": "A", "predicate": "likes", "object": "B"},
            {"subject": "A", "predicate": "knows", "object": "C"},
        ]
        detector = self._make_detector(triples)
        result = detector.detect(ClusterRequest(predicate_filter=["likes"]))
        assert result.stats.edge_count <= 1

    def test_clusters_sorted_by_size(self):
        detector = self._make_detector()
        result = detector.detect(ClusterRequest())
        if len(result.clusters) > 1:
            for i in range(len(result.clusters) - 1):
                assert result.clusters[i].size >= result.clusters[i + 1].size

    def test_modularity_in_range(self):
        detector = self._make_detector()
        result = detector.detect(ClusterRequest())
        assert -1.0 <= result.stats.modularity <= 1.0

    def test_elapsed_ms_positive(self):
        detector = self._make_detector()
        result = detector.detect(ClusterRequest())
        assert result.stats.elapsed_ms >= 0
