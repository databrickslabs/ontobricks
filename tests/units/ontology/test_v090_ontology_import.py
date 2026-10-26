"""OWL / RDFS / SHACL ingest and merge into a domain session (release 0.9.0).

Covers OWL import fidelity (labels vs local names, language tags, external
IRIs, leftover RDF triples), the RDFS/SKOS fallback, SHACL routing to data
quality, conflict-aware merge and industry imports.
"""

import sys

import pytest
from rdflib import RDFS, Graph, Literal, URIRef
from rdflib.namespace import SKOS

from back.core.errors import InfrastructureError, NotFoundError, ValidationError
from back.core.w3c.owl import ConflictItem, ConflictReport
from back.objects.ontology.Import import Import
from back.objects.ontology.Owl import Owl

pytestmark = pytest.mark.unit

import_module = sys.modules[Import.__module__]

EX = "http://example.org/onto#"

OWL_TTL = """@prefix ex: <http://example.org/onto#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .

<http://example.org/onto> a owl:Ontology ;
    rdfs:label "Sales Ontology"@fr ;
    rdfs:comment "Un commentaire"@fr .

ex:Customer a owl:Class ;
    rdfs:label "Client"@fr ;
    skos:editorialNote "Review naming"@en ;
    rdfs:subClassOf <http://other.org/x#Party> .

ex:Order a owl:Class ;
    rdfs:label "Commande"@fr .

ex:places a owl:ObjectProperty ;
    rdfs:domain ex:Customer ;
    rdfs:range ex:Order ;
    rdfs:label "passe"@fr .

ex:fullName a owl:DatatypeProperty ;
    rdfs:domain ex:Customer ;
    rdfs:range xsd:string .
"""

RDFS_TTL = """@prefix ex: <http://example.org/v#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

ex:Animal a rdfs:Class ; rdfs:label "Animal" .
ex:Dog a rdfs:Class ; rdfs:subClassOf ex:Animal .
ex:owns a rdf:Property ; rdfs:domain ex:Animal ; rdfs:range ex:Dog .
"""

SHACL_TTL = """@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.org/onto#> .

ex:CustomerShape a sh:NodeShape ;
    sh:targetClass ex:Customer ;
    sh:property [ sh:path ex:email ; sh:minCount 1 ] .
"""

EMPTY_TTL = """@prefix ex: <http://example.org/onto#> .
ex:a ex:b ex:c .
"""


@pytest.fixture
def session(domain_session):
    domain_session.info["name"] = "SalesDomain"
    return domain_session


def _without_ontology_header(ttl):
    return "\n".join(
        line for line in ttl.splitlines()
        if not line.startswith("<http://example.org/onto>")
        and "Sales Ontology" not in line
        and "Un commentaire" not in line
    )


def _cls(session, name):
    return next(c for c in session.ontology["classes"] if c["name"] == name)


def _prop(session, name):
    return next(p for p in session.ontology["properties"] if p["name"] == name)


# ---------------------------------------------------------------------------
# ingest_owl — fidelity
# ---------------------------------------------------------------------------


class TestIngestOwlFidelity:
    def test_ontology_header_label_and_language_win_over_domain_name(self, session):
        Import(session).ingest_owl(OWL_TTL)
        ont = session.ontology
        assert ont["name"] == "Sales Ontology"
        assert ont["label_lang"] == "fr"
        assert ont["comment_lang"] == "fr"
        assert ont["base_uri"] == EX

    def test_class_label_kept_separate_from_local_name(self, session):
        Import(session).ingest_owl(OWL_TTL)
        customer = _cls(session, "Customer")
        assert customer["label"] == "Client"
        assert customer["label_lang"] == "fr"
        assert customer["uri"] == f"{EX}Customer"

    def test_property_label_language_and_endpoint_iris_preserved(self, session):
        Import(session).ingest_owl(OWL_TTL)
        places = _prop(session, "places")
        assert places["label"] == "passe"
        assert places["label_lang"] == "fr"
        assert places["domain_uri"] == f"{EX}Customer"
        assert places["range_uri"] == f"{EX}Order"

    def test_external_parent_iri_preserved(self, session):
        Import(session).ingest_owl(OWL_TTL)
        customer = _cls(session, "Customer")
        assert customer["parent"] == "Party"
        assert customer["parent_uri"] == "http://other.org/x#Party"

    def test_leftover_annotation_retained_in_rdf_extras(self, session):
        Import(session).ingest_owl(OWL_TTL)
        triples = session.ontology["rdf_extras"]["triples"]
        note = next(t for t in triples if t["p"] == str(SKOS.editorialNote))
        assert note["s"] == f"{EX}Customer"
        assert note["o"] == "Review naming"
        assert note["lang"] == "en"

    def test_reexport_keeps_iris_language_tags_and_extras(self, session):
        Import(session).ingest_owl(OWL_TTL)
        ont = session.ontology
        turtle = Owl.generate_owl(ont)
        g = Graph()
        g.parse(data=turtle, format="turtle")
        customer = URIRef(f"{EX}Customer")
        assert (customer, RDFS.subClassOf, URIRef("http://other.org/x#Party")) in g
        assert (customer, RDFS.label, Literal("Client", lang="fr")) in g
        assert (customer, SKOS.editorialNote, Literal("Review naming", lang="en")) in g

    def test_header_less_owl_falls_back_to_domain_name(self, session):
        Import(session).ingest_owl(_without_ontology_header(OWL_TTL))
        assert session.ontology["name"] == "SalesDomain"

    def test_header_less_owl_uses_default_base_uri(self, session):
        from shared.config.constants import DEFAULT_BASE_URI

        Import(session).ingest_owl(_without_ontology_header(OWL_TTL))
        assert session.ontology["base_uri"] == DEFAULT_BASE_URI
        assert {c["name"] for c in session.ontology["classes"]} == {"Customer", "Order"}

    def test_apply_parsed_owl_uses_domain_name_when_info_has_no_label(self, session):
        name = Import(session).apply_parsed_owl_to_domain({}, [], [], [], [], [])
        assert name == "SalesDomain"
        assert session.ontology["name"] == "SalesDomain"
        assert session.ontology["rdf_extras"] == {}
        assert session.ontology["groups"] == []

    def test_apply_parsed_owl_without_fallback_keeps_empty_name(self, session):
        name = Import(session).apply_parsed_owl_to_domain(
            {"uri": "http://u.org/o"}, [], [], [], [], [], name_fallback_to_domain=False
        )
        assert name == ""
        assert session.ontology["base_uri"] == "http://u.org/o"


class TestIngestOwlOutcomes:
    def test_import_outcome_reports_stats(self, session):
        payload = Import(session).ingest_owl(OWL_TTL)
        assert payload["success"] is True
        assert payload["stats"]["classes"] == 2
        assert payload["stats"]["properties"] == 2
        assert payload["config"] is session.ontology

    def test_parse_outcome_exposes_resolved_label_and_namespace(self, session):
        payload = Import(session).ingest_owl(OWL_TTL, outcome="parse")
        info = payload["ontology"]["info"]
        assert info["label"] == "Sales Ontology"
        assert info["namespace"] == EX
        assert info["uri"] == "http://example.org/onto"
        assert payload["stats"]["swrl_rules"] == 0

    def test_load_file_outcome_returns_session_ontology(self, session):
        payload = Import(session).ingest_owl(OWL_TTL, outcome="load_file")
        assert payload["ontology"] is session.ontology
        assert payload["stats"]["axioms"] == 0


class TestIngestOwlFallbacks:
    def test_rdfs_only_content_falls_back_to_rdfs_parser(self, session):
        payload = Import(session).ingest_owl(RDFS_TTL)
        names = {c["name"] for c in session.ontology["classes"]}
        assert names == {"Animal", "Dog"}
        assert _cls(session, "Dog")["parent"] == "Animal"
        assert payload["stats"]["properties"] == 1
        assert session.ontology["constraints"] == []

    def test_rdfs_fallback_base_uri_is_vocabulary_namespace(self, session):
        Import(session).ingest_owl(RDFS_TTL)
        assert session.ontology["base_uri"].startswith("http://example.org/v")

    def test_rdfs_parser_failure_is_swallowed(self, session, monkeypatch):
        def _boom(_content):
            raise RuntimeError("bad rdfs")

        monkeypatch.setattr(Owl, "parse_rdfs", staticmethod(_boom))
        payload = Import(session).ingest_owl(EMPTY_TTL)
        assert payload["success"] is True
        assert payload["stats"]["classes"] == 0

    def test_shacl_content_routed_to_data_quality(self, session):
        payload = Import(session).ingest_owl(SHACL_TTL)
        assert payload["shacl"] is True
        assert payload["imported_count"] == 1
        assert payload["stats"] == {"classes": 0, "properties": 0, "shacl_shapes": 1}
        assert session.shacl_shapes[0]["target_class"] == "Customer"
        assert not session.ontology.get("classes")


# ---------------------------------------------------------------------------
# SHACL detection
# ---------------------------------------------------------------------------


class TestTryImportAsShacl:
    def test_non_shacl_turtle_returns_none(self, session):
        assert Import(session)._try_import_as_shacl(OWL_TTL) is None

    def test_unparseable_content_returns_none(self, session):
        assert Import(session)._try_import_as_shacl("not turtle {{{") is None

    def test_node_shape_without_importable_shapes_returns_none(self, session, monkeypatch):
        from back.core.w3c import SHACLService

        monkeypatch.setattr(SHACLService, "import_shapes", lambda self, c: [])
        assert Import(session)._try_import_as_shacl(SHACL_TTL) is None

    def test_same_id_shapes_replaced_and_new_ones_appended(self, session, monkeypatch):
        from back.core.w3c import SHACLService

        session.shacl_shapes = [
            {"id": "s1", "label": "old"},
            {"id": "keep", "label": "untouched"},
        ]
        monkeypatch.setattr(
            SHACLService,
            "import_shapes",
            lambda self, c: [{"id": "s1", "label": "new"}, {"id": "s2", "label": "added"}],
        )
        payload = Import(session)._try_import_as_shacl(SHACL_TTL)
        assert payload["imported_count"] == 2
        assert session.shacl_shapes == [
            {"id": "s1", "label": "new"},
            {"id": "keep", "label": "untouched"},
            {"id": "s2", "label": "added"},
        ]

    def test_reimporting_same_shacl_file_does_not_duplicate_shapes(self, session):
        imp = Import(session)
        imp._try_import_as_shacl(SHACL_TTL)
        imp._try_import_as_shacl(SHACL_TTL)
        assert len(session.shacl_shapes) == 1

    def test_rdfs_import_redirects_shacl_to_data_quality(self, session):
        payload = Import(session).apply_parsed_rdfs_to_domain(SHACL_TTL)
        assert payload["shacl"] is True
        assert len(session.shacl_shapes) == 1


class TestApplyParsedRdfs:
    def test_rdfs_import_resets_owl_fidelity_metadata(self, session):
        session.ontology["rdf_extras"] = {"triples": [{"s": "x"}]}
        session.ontology["label_lang"] = "fr"
        payload = Import(session).apply_parsed_rdfs_to_domain(RDFS_TTL)
        assert payload["stats"] == {"classes": 2, "properties": 1}
        assert session.ontology["rdf_extras"] == {}
        assert session.ontology["label_lang"] is None
        assert session.ontology["comment_lang"] is None


# ---------------------------------------------------------------------------
# analyze_import
# ---------------------------------------------------------------------------


class TestAnalyzeImport:
    def test_owl_analysis_does_not_mutate_session(self, session):
        result = Import(session).analyze_import(OWL_TTL)
        assert result["success"] is True
        assert result["report"]["summary"]["new"] >= 4
        assert not session.ontology.get("classes")

    def test_owl_analysis_flags_name_conflict(self, session):
        session.ontology["classes"] = [{"name": "Customer", "uri": "http://mine.org#Customer"}]
        report = Import(session).analyze_import(OWL_TTL)["report"]
        assert report["summary"]["conflicts"] == 1

    def test_owl_analysis_falls_back_to_rdfs(self, session):
        report = Import(session).analyze_import(RDFS_TTL)["report"]
        assert report["summary"]["new"] == 3

    def test_owl_analysis_tolerates_rdfs_parser_failure(self, session, monkeypatch):
        def _boom(_content):
            raise RuntimeError("bad rdfs")

        monkeypatch.setattr(Owl, "parse_rdfs", staticmethod(_boom))
        report = Import(session).analyze_import(EMPTY_TTL)["report"]
        assert report["summary"]["new"] == 0

    def test_rdfs_analysis_reports_new_items(self, session):
        report = Import(session).analyze_import(RDFS_TTL, format="rdfs")["report"]
        assert report["summary"]["new"] == 3

    def test_rdfs_analysis_rejects_shacl(self, session):
        with pytest.raises(ValidationError, match="SHACL"):
            Import(session).analyze_import(SHACL_TTL, format="rdfs")

    def test_rdfs_analysis_rejects_empty_vocabulary(self, session):
        with pytest.raises(ValidationError, match="No classes or properties"):
            Import(session).analyze_import(EMPTY_TTL, format="rdfs")


# ---------------------------------------------------------------------------
# merge_parsed_owl_to_domain
# ---------------------------------------------------------------------------


@pytest.fixture
def existing(session):
    session.ontology["base_uri"] = EX
    session.ontology["classes"] = [
        {"name": "Customer", "uri": "http://mine.org#Customer", "label": "My customer",
         "dataProperties": [{"name": "vip", "localName": "vip"}]},
        {"name": "Region", "uri": "http://mine.org#Region", "label": "Region"},
    ]
    session.ontology["properties"] = []
    return session


class TestMergeOwl:
    def test_new_items_appended_and_conflicts_skipped_by_default(self, existing):
        payload = Import(existing).merge_parsed_owl_to_domain(OWL_TTL, {})
        names = [c["name"] for c in existing.ontology["classes"]]
        assert names.count("Customer") == 1
        assert "Order" in names and "Region" in names
        customer = _cls(existing, "Customer")
        assert customer["uri"] == "http://mine.org#Customer"
        assert customer["label"] == "My customer"
        assert payload["stats"]["conflicts_resolved"] == 1
        assert payload["stats"]["new"] == 3
        assert payload["message"] == "Merged: 3 new item(s) added"

    def test_overwrite_resolution_replaces_by_name(self, existing):
        Import(existing).merge_parsed_owl_to_domain(
            OWL_TTL, {f"{EX}Customer": "overwrite"}
        )
        customers = [c for c in existing.ontology["classes"] if c["name"] == "Customer"]
        assert len(customers) == 1
        assert customers[0]["uri"] == f"{EX}Customer"
        assert customers[0]["label"] == "Client"

    def test_rename_resolution_adds_incoming_under_new_name(self, existing):
        Import(existing).merge_parsed_owl_to_domain(
            OWL_TTL, {f"{EX}Customer": "rename:ImportedCustomer"}
        )
        names = [c["name"] for c in existing.ontology["classes"]]
        assert "Customer" in names and "ImportedCustomer" in names
        assert _cls(existing, "Customer")["uri"] == "http://mine.org#Customer"
        assert _cls(existing, "ImportedCustomer")["label"] == "Client"

    def test_identical_reimport_adds_nothing(self, session):
        Import(session).ingest_owl(OWL_TTL)
        before = len(session.ontology["classes"])
        payload = Import(session).merge_parsed_owl_to_domain(OWL_TTL, {})
        assert payload["stats"]["new"] == 0
        assert len(session.ontology["classes"]) == before

    def test_owl_merge_falls_back_to_rdfs(self, existing):
        payload = Import(existing).merge_parsed_owl_to_domain(RDFS_TTL, {})
        names = {c["name"] for c in existing.ontology["classes"]}
        assert {"Animal", "Dog", "Customer", "Region"} <= names
        assert payload["stats"]["new"] == 3

    def test_owl_merge_tolerates_rdfs_parser_failure(self, existing, monkeypatch):
        def _boom(_content):
            raise RuntimeError("bad rdfs")

        monkeypatch.setattr(Owl, "parse_rdfs", staticmethod(_boom))
        payload = Import(existing).merge_parsed_owl_to_domain(EMPTY_TTL, {})
        assert payload["stats"]["new"] == 0
        assert len(existing.ontology["classes"]) == 2


class TestMergeRdfs:
    def test_rdfs_merge_appends_new_vocabulary(self, existing):
        payload = Import(existing).merge_parsed_owl_to_domain(
            RDFS_TTL, {}, format="rdfs"
        )
        names = {c["name"] for c in existing.ontology["classes"]}
        assert names == {"Customer", "Region", "Animal", "Dog"}
        assert payload["stats"] == {
            "classes": 4,
            "properties": 1,
            "new": 3,
            "duplicates_skipped": 0,
            "conflicts_resolved": 0,
        }

    def test_rdfs_merge_rejects_shacl(self, existing):
        with pytest.raises(ValidationError, match="SHACL"):
            Import(existing).merge_parsed_owl_to_domain(
                SHACL_TTL, {}, format="rdfs"
            )

    def test_rdfs_merge_rejects_empty_vocabulary(self, existing):
        with pytest.raises(ValidationError, match="No classes or properties"):
            Import(existing).merge_parsed_owl_to_domain(
                EMPTY_TTL, {}, format="rdfs"
            )


class TestApplyResolutions:
    @staticmethod
    def _report(*items):
        report = ConflictReport()
        for item in items:
            (report.new_items if item.conflict_type == "new" else report.conflicts).append(item)
        return report

    def test_overwrite_without_index_match_appends(self):
        item = ConflictItem(
            entity_type="group", uri="", name="ghost", conflict_type="uri_conflict",
            incoming={"name": "Ghost"}, existing=None,
        )
        result = Import._apply_resolutions(
            "group", [{"name": "Other"}], self._report(item), {"ghost": "overwrite"}
        )
        assert result == [{"name": "Other"}, {"name": "Ghost"}]

    def test_overwrite_by_uri_replaces_in_place(self):
        item = ConflictItem(
            entity_type="class", uri="u:1", name="a", conflict_type="uri_conflict",
            incoming={"uri": "u:1", "name": "A", "label": "new"}, existing=None,
        )
        existing = [{"uri": "u:1", "name": "A", "label": "old"}, {"uri": "u:2", "name": "B"}]
        result = Import._apply_resolutions(
            "class", existing, self._report(item), {"u:1": "overwrite"}
        )
        assert result[0]["label"] == "new"
        assert len(result) == 2

    def test_other_entity_types_ignored(self):
        item = ConflictItem(
            entity_type="property", uri="u:p", name="p", conflict_type="new",
            incoming={"uri": "u:p"}, existing=None,
        )
        assert Import._apply_resolutions("class", [], self._report(item), {}) == []


# ---------------------------------------------------------------------------
# Industry imports
# ---------------------------------------------------------------------------


def _industry_result(info):
    return {
        "ontology_info": info,
        "classes": [{"name": "Thing", "uri": "http://x#Thing"}],
        "properties": [],
        "constraints": [],
        "swrl_rules": [],
        "axioms": [],
        "expressions": [],
        "message": "ok",
        "stats": {"classes": 1},
        "failed": [],
    }


class TestIndustryImport:
    @pytest.mark.parametrize(
        "kind, expected_name, expected_base, desc_prefix",
        [
            ("cdisc", "CDISC", "http://rdf.cdisc.org/", "CDISC Foundational"),
            ("fibo", "FIBO", "https://spec.edmcouncil.org/fibo/ontology/", "Financial Industry"),
            ("iof", "IOF", "https://spec.industrialontologies.org/ontology/", "Industrial Ontologies"),
        ],
    )
    def test_defaults_applied_and_fidelity_metadata_cleared(
        self, session, monkeypatch, kind, expected_name, expected_base, desc_prefix
    ):
        session.ontology["rdf_extras"] = {"triples": [1]}
        session.ontology["label_lang"] = "fr"
        monkeypatch.setitem(
            import_module._INDUSTRY_FETCH, kind, lambda keys: _industry_result({})
        )
        payload = Import(session).import_industry_ontology(kind, ["A", "B"])
        ont = session.ontology
        assert payload == {"success": True, "message": "ok", "stats": {"classes": 1}, "failed": []}
        assert ont["name"] == expected_name
        assert ont["base_uri"] == expected_base
        assert ont["description"].startswith(desc_prefix)
        assert ont["description"].endswith("A, B")
        assert ont["rdf_extras"] == {}
        assert ont["label_lang"] is None

    def test_fhir_uses_requested_version(self, session, monkeypatch):
        captured = {}

        def _fetch(keys, version):
            captured["version"] = version
            return _industry_result({})

        monkeypatch.setitem(import_module._INDUSTRY_FETCH, "fhir", _fetch)
        Import(session).import_industry_ontology("fhir", ["Patient"], version="R5")
        assert captured["version"] == "R5"
        assert session.ontology["name"] == "HL7 FHIR R5"
        assert session.ontology["base_uri"] == "http://hl7.org/fhir/"
        assert session.ontology["description"] == "HL7 FHIR R5 – Patient"

    def test_fhir_defaults_version_when_missing(self, session, monkeypatch):
        from back.core.industry.fhir import FhirImportService

        captured = {}

        def _fetch(keys, version):
            captured["version"] = version
            return _industry_result({"name": "FHIR custom", "base_uri": "http://f/"})

        monkeypatch.setitem(import_module._INDUSTRY_FETCH, "fhir", _fetch)
        Import(session).import_industry_ontology("fhir", ["Patient"])
        assert captured["version"] == FhirImportService.DEFAULT_VERSION
        assert session.ontology["name"] == "FHIR custom"
        assert session.ontology["base_uri"] == "http://f/"

    def test_empty_selection_rejected(self, session):
        with pytest.raises(ValidationError, match="No FIBO domains selected"):
            Import(session).import_industry_ontology("fibo", [])

    def test_domain_errors_propagate_unchanged(self, session, monkeypatch):
        def _fetch(keys):
            raise NotFoundError("missing module")

        monkeypatch.setitem(import_module._INDUSTRY_FETCH, "iof", _fetch)
        with pytest.raises(NotFoundError):
            Import(session).import_industry_ontology("iof", ["x"])

    def test_unexpected_errors_wrapped_as_infrastructure_error(self, session, monkeypatch):
        def _fetch(keys):
            raise RuntimeError("network down")

        monkeypatch.setitem(import_module._INDUSTRY_FETCH, "cdisc", _fetch)
        with pytest.raises(InfrastructureError, match="CDISC import failed"):
            Import(session).import_industry_ontology("cdisc", ["x"])
        assert not session.ontology.get("classes")
