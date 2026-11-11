"""URI helpers for community materialisation."""

import pytest

from back.core.graph_analysis.CommunityVocabulary import CommunityVocabulary

pytestmark = pytest.mark.unit

BASE = "http://example.org/onto"


def test_cluster_uris_are_stable():
    assert CommunityVocabulary.cluster_class(BASE) == f"{BASE}/Cluster"
    assert CommunityVocabulary.in_cluster(BASE) == f"{BASE}/inCluster"
    assert CommunityVocabulary.cluster_prefix(BASE) == f"{BASE}/cluster/"
    assert CommunityVocabulary.cluster(BASE, 3) == f"{BASE}/cluster/c-3"
    assert CommunityVocabulary.parse_cluster_id(f"{BASE}/cluster/c-3") == 3
    assert CommunityVocabulary.parse_cluster_id("not-a-cluster") == -1


def test_join_respects_hash_separator():
    hashed = "http://example.org/onto#"
    assert CommunityVocabulary.cluster_class(hashed) == "http://example.org/onto#Cluster"
