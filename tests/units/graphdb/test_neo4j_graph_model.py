"""Unit tests for the pure RDF→property-graph transform (Neo4jGraphModel).

No Neo4j driver needed — these exercise the classification/dedup/label logic
that is the heart of the typed-node model. Uses the verified InsurBricks demo
shapes as fixtures.
"""

import pytest

from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL
from back.core.graphdb.neo4j.Neo4jGraphModel import (
    is_uri,
    label_from_class_uri,
    reltype_from_predicate,
    plan_writes,
)

pytestmark = pytest.mark.unit

NS = "https://databricks-ontology.com/InsurBricks/"


# ---------------------------------------------------------------------------
#  Literal vs URI classification
# ---------------------------------------------------------------------------

class TestIsUri:
    def test_http_uri_is_entity_reference(self):
        assert is_uri(f"{NS}Policy/POL-20008")

    def test_https_uri_is_entity_reference(self):
        assert is_uri("https://example.com/x")

    def test_plain_literal_is_not_uri(self):
        assert not is_uri("Marie Lefebvre")

    def test_date_literal_is_not_uri(self):
        assert not is_uri("2016-07-07")

    def test_empty_is_not_uri(self):
        assert not is_uri("")


# ---------------------------------------------------------------------------
#  Label / reltype derivation
# ---------------------------------------------------------------------------

class TestNameDerivation:
    def test_label_from_class_uri_uses_local_name(self):
        assert label_from_class_uri(f"{NS.rstrip('/')}#Customer") == "Customer"

    def test_label_from_path_style_class_uri(self):
        assert label_from_class_uri(f"{NS}MotorPolicy") == "MotorPolicy"

    def test_reltype_from_predicate(self):
        assert reltype_from_predicate(f"{NS}filedAgainst") == "filedAgainst"

    def test_hyphens_sanitised(self):
        assert reltype_from_predicate(f"{NS}has-part") == "has_part"

    def test_leading_digit_prefixed(self):
        assert label_from_class_uri(f"{NS}3DModel") == "_3DModel"


# ---------------------------------------------------------------------------
#  plan_writes — node/edge/label/property planning
# ---------------------------------------------------------------------------

class TestPlanWrites:
    def _marie_triples(self):
        c = f"{NS}Customer/CUST-1007"
        return [
            {"subject": c, "predicate": RDF_TYPE, "object": f"{NS.rstrip('/')}#Customer"},
            {"subject": c, "predicate": RDFS_LABEL, "object": "Marie Lefebvre"},
            {"subject": c, "predicate": f"{NS}city", "object": "Paris"},
            {"subject": c, "predicate": f"{NS}holds", "object": f"{NS}Policy/POL-20008"},
        ]

    def test_literal_predicate_becomes_property(self):
        p = plan_writes(self._marie_triples())
        marie = next(n for n in p.nodes if n.uri.endswith("CUST-1007"))
        assert marie.properties["city"] == "Paris"

    def test_rdfs_label_becomes_name(self):
        p = plan_writes(self._marie_triples())
        marie = next(n for n in p.nodes if n.uri.endswith("CUST-1007"))
        assert marie.properties["name"] == "Marie Lefebvre"

    def test_rdf_type_becomes_label(self):
        p = plan_writes(self._marie_triples())
        marie = next(n for n in p.nodes if n.uri.endswith("CUST-1007"))
        assert "Customer" in marie.labels

    def test_uri_object_becomes_edge_not_property(self):
        p = plan_writes(self._marie_triples())
        assert any(
            e.reltype == "holds" and e.object.endswith("POL-20008") for e in p.edges
        )
        marie = next(n for n in p.nodes if n.uri.endswith("CUST-1007"))
        assert "holds" not in marie.properties

    def test_edge_target_node_is_created(self):
        # The Policy node must exist even though it has no own triples in the batch.
        p = plan_writes(self._marie_triples())
        assert any(n.uri.endswith("POL-20008") for n in p.nodes)

    def test_node_dedup_shared_uri_is_one_node(self):
        c = f"{NS}Customer/CUST-1007"
        triples = [
            {"subject": c, "predicate": f"{NS}city", "object": "Paris"},
            {"subject": c, "predicate": f"{NS}country", "object": "FR"},
            {"subject": c, "predicate": RDF_TYPE, "object": f"{NS}Customer"},
        ]
        p = plan_writes(triples)
        matches = [n for n in p.nodes if n.uri == c]
        assert len(matches) == 1
        assert matches[0].properties == {"city": "Paris", "country": "FR"}

    def test_multi_label_when_entity_has_two_types(self):
        c = f"{NS}Customer/CUST-1007"
        triples = [
            {"subject": c, "predicate": RDF_TYPE, "object": f"{NS}Customer"},
            {"subject": c, "predicate": RDF_TYPE, "object": f"{NS}InsurancePerson"},
        ]
        p = plan_writes(triples)
        marie = next(n for n in p.nodes if n.uri == c)
        assert set(marie.labels) == {"Customer", "InsurancePerson"}

    def test_reverse_maps_capture_uris(self):
        p = plan_writes(self._marie_triples())
        assert p.label_map["Customer"].endswith("#Customer")
        assert p.reltype_map["holds"] == f"{NS}holds"
        assert p.prop_map["city"] == f"{NS}city"

    def test_empty_input(self):
        p = plan_writes([])
        assert p.nodes == [] and p.edges == []

    def test_missing_subject_or_predicate_skipped(self):
        p = plan_writes([{"subject": "", "predicate": f"{NS}x", "object": "y"}])
        assert p.nodes == [] and p.edges == []
