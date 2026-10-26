"""`Ontology` facade delegators introduced by the 0.9.0 capability extract.

Each test drives a real behaviour through the facade (not a mock) so a broken
delegation (wrong argument order, dropped keyword) is caught.
"""

import pytest

from back.objects.ontology.Ontology import Ontology

pytestmark = pytest.mark.unit

EX = "http://example.org/onto#"

OWL_TTL = """@prefix ex: <http://example.org/onto#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
<http://example.org/onto> a owl:Ontology ; rdfs:label "Facade"@en .
ex:Customer a owl:Class ; rdfs:label "Client"@fr .
ex:Order a owl:Class .
"""

RDFS_TTL = """@prefix ex: <http://example.org/v#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
ex:Animal a rdfs:Class .
"""

SHACL_TTL = """@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.org/onto#> .
ex:S a sh:NodeShape ; sh:targetClass ex:Customer ;
    sh:property [ sh:path ex:email ; sh:minCount 1 ] .
"""


@pytest.fixture
def session(domain_session):
    domain_session.info["name"] = "Dom"
    domain_session.ontology["base_uri"] = EX
    domain_session.ontology["classes"] = [
        {"name": "Customer", "uri": "http://mine.org#Customer", "label": "Mine"},
    ]
    domain_session.ontology["properties"] = []
    return domain_session


class TestStaticDelegators:
    def test_sync_class_data_properties_merges_domain_attributes(self):
        config = {
            "classes": [{"name": "Customer", "dataProperties": []}],
            "properties": [{"name": "email", "type": "DatatypeProperty", "domain": "Customer"}],
        }
        Ontology.sync_class_data_properties(config)
        assert [dp["name"] for dp in config["classes"][0]["dataProperties"]] == ["email"]

    def test_diff_by_uri_reports_added_updated_removed(self):
        old = [{"uri": "u:a", "name": "A"}, {"uri": "u:b", "name": "B"}]
        new = [{"uri": "u:b", "name": "B", "label": "changed"}, {"uri": "u:c", "name": "C"}]
        assert Ontology._diff_by_uri(old, new) == (
            [("u:c", "C")], [("u:b", "B")], [("u:a", "A")],
        )

    def test_parse_rdfs_returns_vocabulary(self):
        _info, classes, props = Ontology.parse_rdfs(RDFS_TTL)
        assert [c["name"] for c in classes] == ["Animal"]
        assert props == []


class TestImportDelegators:
    def test_try_import_as_shacl(self, session):
        assert Ontology(session)._try_import_as_shacl(SHACL_TTL)["shacl"] is True
        assert Ontology(session)._try_import_as_shacl(OWL_TTL) is None

    def test_analyze_import_flags_name_conflict_without_mutation(self, session):
        report = Ontology(session).analyze_import(OWL_TTL)["report"]
        assert report["summary"]["conflicts"] == 1
        assert len(session.ontology["classes"]) == 1

    def test_merge_with_rename_keeps_existing_entity(self, session):
        Ontology(session).merge_parsed_owl_to_domain(
            OWL_TTL, {f"{EX}Customer": "rename:Client"}
        )
        names = sorted(c["name"] for c in session.ontology["classes"])
        assert names == ["Client", "Customer", "Order"]

    def test_merge_rdfs_delegator(self, session):
        payload = Ontology(session)._merge_rdfs(RDFS_TTL, {})
        assert payload["stats"]["new"] == 1
        assert {c["name"] for c in session.ontology["classes"]} == {"Customer", "Animal"}

    def test_apply_parsed_owl_and_payload_builders(self, session):
        ont = Ontology(session)
        info = {"label": "Parsed", "namespace": EX, "uri": "http://example.org/onto"}
        classes = [{"name": "X", "uri": f"{EX}X"}]
        name = ont.apply_parsed_owl_to_domain(info, classes, [], [], [], [], groups=[])
        assert name == "Parsed"
        assert session.ontology["classes"][0]["name"] == "X"

        imported = ont.build_import_owl_success_payload(classes, [], [])
        assert imported["stats"] == {"classes": 1, "properties": 0, "constraints": 0}

        parsed = ont.build_parse_owl_success_payload(info, classes, [], [], [], [], None, name)
        assert parsed["ontology"]["info"] == {
            "label": "Parsed", "namespace": EX, "uri": "http://example.org/onto",
        }
        assert parsed["stats"]["expressions"] == 0

        loaded = ont.build_load_owl_file_success_payload(classes, [], [], [], [])
        assert loaded["ontology"] is session.ontology
        assert loaded["stats"]["classes"] == 1


class TestDesignLayoutSync:
    def test_view_entities_for_deleted_classes_are_pruned(self, session):
        session._data["design_layout"] = {
            "views": {
                "Main": {
                    "entities": [
                        {"id": "e1", "name": "Customer", "properties": []},
                        {"id": "e2", "name": "Ghost", "properties": []},
                    ],
                    "relationships": [],
                    "inheritances": [],
                }
            }
        }
        Ontology(session)._sync_design_layout_with_ontology()
        entities = session.design_layout["views"]["Main"]["entities"]
        assert [e["name"] for e in entities] == ["Customer"]

    def test_no_views_is_a_noop(self, session):
        session._data["design_layout"] = {}
        Ontology(session)._sync_design_layout_with_ontology()
        assert session.design_layout == {}
