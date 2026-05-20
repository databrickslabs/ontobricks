"""Tests for industry import services – catalogs and module collection (pure/unit)."""

import pytest
from unittest.mock import patch, MagicMock

from back.core.industry.fibo.FiboImportService import FiboImportService
from back.core.industry.cdisc.CdiscImportService import CdiscImportService
from back.core.industry.iof.IofImportService import IofImportService
from back.core.industry.fhir.FhirImportService import FhirImportService


class TestFiboImportService:
    def test_catalog_returns_all_domains(self):
        catalog = FiboImportService.get_fibo_catalog()
        assert isinstance(catalog, list)
        assert len(catalog) == len(FiboImportService.FIBO_DOMAINS)
        keys = {c["key"] for c in catalog}
        assert "FND" in keys
        assert "SEC" in keys

    def test_catalog_entry_fields(self):
        catalog = FiboImportService.get_fibo_catalog()
        for entry in catalog:
            assert "key" in entry
            assert "name" in entry
            assert "description" in entry
            assert "icon" in entry
            assert "module_count" in entry
            assert entry["module_count"] > 0

    def test_collect_module_paths_single_domain(self):
        paths = FiboImportService._collect_module_paths(["FND"])
        assert len(paths) == len(FiboImportService.FIBO_DOMAINS["FND"]["modules"])

    def test_collect_module_paths_auto_includes_fnd(self):
        paths = FiboImportService._collect_module_paths(["BE"])
        fnd_modules = FiboImportService.FIBO_DOMAINS["FND"]["modules"]
        for mod in fnd_modules:
            assert mod in paths

    def test_collect_module_paths_deduplication(self):
        paths = FiboImportService._collect_module_paths(["FND", "FND"])
        assert len(paths) == len(set(paths))

    def test_collect_module_paths_unknown_domain(self):
        """Unknown domain is skipped, but FND is auto-included as a dependency."""
        paths = FiboImportService._collect_module_paths(["UNKNOWN"])
        fnd_count = len(FiboImportService.FIBO_DOMAINS["FND"]["modules"])
        assert len(paths) == fnd_count

    def test_collect_module_paths_fnd_only_no_duplication(self):
        paths = FiboImportService._collect_module_paths(["FND"])
        fnd_count = len(FiboImportService.FIBO_DOMAINS["FND"]["modules"])
        assert len(paths) == fnd_count

    @patch("requests.get")
    def test_fetch_single_module_success(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = "@prefix owl: <http://www.w3.org/2002/07/owl#> ."
        mock_get.return_value = mock_resp

        path, content, error = FiboImportService._fetch_single_module("FND/Parties/Parties")
        assert content is not None
        assert error is None

    @patch("requests.get")
    def test_fetch_single_module_html_rejected(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = "<!DOCTYPE html><html>Not found</html>"
        mock_get.return_value = mock_resp

        path, content, error = FiboImportService._fetch_single_module("FND/Parties/Parties")
        assert content is None
        assert error is not None

    @patch("requests.get")
    def test_fetch_single_module_http_error(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_get.return_value = mock_resp

        path, content, error = FiboImportService._fetch_single_module("FND/Bad/Path")
        assert content is None
        assert "404" in error


class TestCdiscImportService:
    def test_catalog_returns_all_domains(self):
        catalog = CdiscImportService.get_cdisc_catalog()
        assert isinstance(catalog, list)
        assert len(catalog) == len(CdiscImportService.CDISC_DOMAINS)

    def test_catalog_entry_fields(self):
        catalog = CdiscImportService.get_cdisc_catalog()
        for entry in catalog:
            assert "key" in entry
            assert "name" in entry
            assert "required" in entry
            assert "module_count" in entry

    def test_schemas_marked_required(self):
        catalog = CdiscImportService.get_cdisc_catalog()
        schemas = next(c for c in catalog if c["key"] == "SCHEMAS")
        assert schemas["required"] is True

    def test_collect_modules_auto_includes_schemas(self):
        modules = CdiscImportService._collect_modules(["SDTM"])
        urls = [m["url"] for m in modules]
        schema_urls = [m["url"] for m in CdiscImportService.CDISC_DOMAINS["SCHEMAS"]["modules"]]
        for url in schema_urls:
            assert url in urls

    def test_collect_modules_deduplication(self):
        modules = CdiscImportService._collect_modules(["SCHEMAS", "SCHEMAS"])
        urls = [m["url"] for m in modules]
        assert len(urls) == len(set(urls))

    def test_collect_modules_unknown_domain(self):
        """Unknown domain is skipped, but SCHEMAS is auto-included."""
        modules = CdiscImportService._collect_modules(["UNKNOWN"])
        schema_count = len(CdiscImportService.CDISC_DOMAINS["SCHEMAS"]["modules"])
        assert len(modules) == schema_count

    def test_xsd_to_simple_string(self):
        assert CdiscImportService._xsd_to_simple("xsd:string") == "string"
        assert CdiscImportService._xsd_to_simple("xsd:integer") == "integer"
        assert CdiscImportService._xsd_to_simple("xsd:boolean") == "boolean"
        assert CdiscImportService._xsd_to_simple("xsd:decimal") == "decimal"
        assert CdiscImportService._xsd_to_simple("xsd:date") == "date"
        assert CdiscImportService._xsd_to_simple("xsd:dateTime") == "dateTime"
        assert CdiscImportService._xsd_to_simple("unknown") == "string"

    @patch("requests.get")
    def test_fetch_single_module_success(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = "@prefix owl: <http://www.w3.org/2002/07/owl#> ."
        mock_get.return_value = mock_resp

        label, content, error = CdiscImportService._fetch_single_module(
            {"url": "https://example.com/test.ttl", "format": "turtle", "label": "Test"}
        )
        assert content is not None
        assert error is None
        assert label == "Test"


class TestIofImportService:
    def test_catalog_returns_all_domains(self):
        catalog = IofImportService.get_iof_catalog()
        assert isinstance(catalog, list)
        assert len(catalog) == len(IofImportService.IOF_DOMAINS)

    def test_catalog_entry_fields(self):
        catalog = IofImportService.get_iof_catalog()
        for entry in catalog:
            assert "key" in entry
            assert "name" in entry
            assert "required" in entry
            assert "module_count" in entry

    def test_core_marked_required(self):
        catalog = IofImportService.get_iof_catalog()
        core = next(c for c in catalog if c["key"] == "CORE")
        assert core["required"] is True

    def test_collect_modules_auto_includes_core(self):
        modules = IofImportService._collect_modules(["MAINTENANCE"])
        paths = [m["path"] for m in modules]
        core_paths = [m["path"] for m in IofImportService.IOF_DOMAINS["CORE"]["modules"]]
        for p in core_paths:
            assert p in paths

    def test_collect_modules_deduplication(self):
        modules = IofImportService._collect_modules(["CORE", "CORE"])
        paths = [m["path"] for m in modules]
        assert len(paths) == len(set(paths))

    def test_collect_modules_unknown_domain(self):
        """Unknown domain is skipped, but CORE is auto-included."""
        modules = IofImportService._collect_modules(["UNKNOWN"])
        core_count = len(IofImportService.IOF_DOMAINS["CORE"]["modules"])
        assert len(modules) == core_count

    def test_resolve_property_label_from_bfo_dict(self):
        from rdflib import Graph
        IofImportService._property_label_cache.clear()
        label = IofImportService._resolve_property_label(
            Graph(), "http://purl.obolibrary.org/obo/BFO_0000050"
        )
        assert label == "partOf"

    def test_resolve_property_label_local_name_fallback(self):
        from rdflib import Graph
        IofImportService._property_label_cache.clear()
        label = IofImportService._resolve_property_label(
            Graph(), "http://example.org/ont#myRelation"
        )
        assert label == "myRelation"

    @patch("requests.get")
    def test_fetch_single_module_success(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = "<rdf:RDF xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#'/>"
        mock_get.return_value = mock_resp

        label, content, error = IofImportService._fetch_single_module(
            {"path": "core/Core.rdf", "label": "IOF Core"}
        )
        assert content is not None
        assert error is None


class TestFhirImportService:
    def test_catalog_returns_all_domains(self):
        catalog = FhirImportService.get_fhir_catalog()
        assert isinstance(catalog, list)
        assert len(catalog) == len(FhirImportService.FHIR_DOMAINS)

    def test_catalog_entry_fields(self):
        catalog = FhirImportService.get_fhir_catalog()
        for entry in catalog:
            assert "key" in entry
            assert "name" in entry
            assert "description" in entry
            assert "icon" in entry
            assert "required" in entry
            assert "module_count" in entry

    def test_foundation_marked_required(self):
        catalog = FhirImportService.get_fhir_catalog()
        foundation = next(c for c in catalog if c["key"] == "FOUNDATION")
        assert foundation["required"] is True

    def test_other_domains_not_required(self):
        catalog = FhirImportService.get_fhir_catalog()
        for entry in catalog:
            if entry["key"] != "FOUNDATION":
                assert entry["required"] is False

    def test_build_allowed_resources_auto_includes_foundation(self):
        allowed = FhirImportService._build_allowed_resources(["CLINICAL"])
        # FOUNDATION resources must be present
        assert "Resource" in allowed
        assert "DomainResource" in allowed
        # CLINICAL resources must be present
        assert "Patient" in allowed
        assert "Encounter" in allowed

    def test_build_allowed_resources_foundation_only(self):
        allowed = FhirImportService._build_allowed_resources(["FOUNDATION"])
        assert "Resource" in allowed
        assert "Bundle" in allowed
        # CLINICAL resources should NOT be included
        assert "Patient" not in allowed

    def test_transform_resource_not_self_referencing(self):
        """fhir:Resource has rdfs:subClassOf fhir:Base — must not set parent='Resource' on itself."""
        from rdflib import Graph, RDF, OWL, RDFS, URIRef, Literal

        graph = Graph()
        fhir_ns = "http://hl7.org/fhir/"

        resource_uri = URIRef(f"{fhir_ns}Resource")
        base_uri = URIRef(f"{fhir_ns}Base")
        graph.add((resource_uri, RDF.type, OWL.Class))
        graph.add((resource_uri, RDFS.label, Literal("Resource")))
        graph.add((resource_uri, RDFS.subClassOf, base_uri))  # Resource → Base

        result = FhirImportService._transform_fhir_to_ontobricks(graph, ["FOUNDATION"])
        resource_cls = next((c for c in result["classes"] if c["name"] == "Resource"), None)
        assert resource_cls is not None, "Resource class not found"
        assert resource_cls["parent"] != "Resource", "Resource must not be its own parent"

    def test_transform_base_is_tree_root(self):
        """Base (the true FHIR root) must have an empty parent so it appears as a root node."""
        from rdflib import Graph

        result = FhirImportService._transform_fhir_to_ontobricks(Graph(), ["FOUNDATION"])
        base_cls = next((c for c in result["classes"] if c["name"] == "Base"), None)
        assert base_cls is not None, "Base class not found"
        assert base_cls["parent"] == "", f"Base parent must be empty, got '{base_cls['parent']}'"

    def test_build_allowed_resources_always_includes_complex_types(self):
        # Complex types must always be present so property ranges resolve in the frontend
        allowed = FhirImportService._build_allowed_resources(["FOUNDATION"])
        assert "Address" in allowed
        assert "CodeableConcept" in allowed
        assert "Reference" in allowed
        assert "HumanName" in allowed
        assert "Identifier" in allowed
        assert "CodeableReference" in allowed

    def test_build_allowed_resources_deduplication(self):
        # Calling with FOUNDATION twice should not duplicate
        allowed1 = FhirImportService._build_allowed_resources(["FOUNDATION"])
        allowed2 = FhirImportService._build_allowed_resources(["FOUNDATION", "FOUNDATION"])
        assert allowed1 == allowed2

    def test_transform_returns_classes_and_properties(self):
        """Minimal smoke test: empty graph always returns base types."""
        from rdflib import Graph
        result = FhirImportService._transform_fhir_to_ontobricks(Graph(), ["FOUNDATION"])
        assert "classes" in result
        assert "properties" in result
        assert "ontology_info" in result
        assert "stats" in result
        # Base types always injected even when not in the parsed graph
        root_names = [c["name"] for c in result["classes"]]
        assert "Resource" in root_names
        assert "DomainResource" in root_names

    def test_transform_uses_domain_filter(self):
        """Classes outside the selected domain are not returned."""
        from rdflib import Graph, RDF, OWL, RDFS, URIRef, Literal

        graph = Graph()
        fhir_ns = "http://hl7.org/fhir/"

        # Add a Patient class (CLINICAL) and a Claim class (FINANCIAL)
        patient_uri = URIRef(f"{fhir_ns}Patient")
        claim_uri = URIRef(f"{fhir_ns}Claim")

        for uri in (patient_uri, claim_uri):
            graph.add((uri, RDF.type, OWL.Class))
            graph.add((uri, RDFS.label, Literal(uri.split("/")[-1])))

        # Select only CLINICAL — Claim (FINANCIAL) must be absent
        result = FhirImportService._transform_fhir_to_ontobricks(graph, ["CLINICAL"])
        class_names = {c["name"] for c in result["classes"]}
        assert "Patient" in class_names
        assert "Resource" in class_names   # injected as base type
        assert "Claim" not in class_names

    def test_transform_extracts_data_properties_from_restrictions(self):
        """Data properties from owl:Restriction blank nodes appear on the class."""
        from rdflib import Graph, RDF, OWL, RDFS, URIRef, Literal, BNode

        graph = Graph()
        fhir_ns = "http://hl7.org/fhir/"

        patient_uri = URIRef(f"{fhir_ns}Patient")
        birthdate_prop = URIRef(f"{fhir_ns}birthDate")
        date_type = URIRef(f"{fhir_ns}date")

        graph.add((patient_uri, RDF.type, OWL.Class))
        graph.add((patient_uri, RDFS.label, Literal("Patient")))

        graph.add((birthdate_prop, RDF.type, OWL.ObjectProperty))
        graph.add((birthdate_prop, RDFS.label, Literal("birthDate")))

        # rdfs:subClassOf [ a owl:Restriction ; owl:onProperty fhir:birthDate ;
        #                    owl:allValuesFrom fhir:date ]
        restriction = BNode()
        graph.add((restriction, RDF.type, OWL.Restriction))
        graph.add((restriction, OWL.onProperty, birthdate_prop))
        graph.add((restriction, OWL.allValuesFrom, date_type))
        graph.add((patient_uri, RDFS.subClassOf, restriction))

        result = FhirImportService._transform_fhir_to_ontobricks(graph, ["CLINICAL"])
        patient_cls = next(c for c in result["classes"] if c["name"] == "Patient")
        dp_names = [dp["name"] for dp in patient_cls["dataProperties"]]
        assert "birthDate" in dp_names

    @patch("requests.get")
    def test_fetch_fhir_ttl_success(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b"@prefix fhir: <http://hl7.org/fhir/> ."
        mock_resp.text = "@prefix fhir: <http://hl7.org/fhir/> ."
        mock_get.return_value = mock_resp

        content = FhirImportService._fetch_fhir_ttl()
        assert "@prefix fhir:" in content

    @patch("requests.get")
    def test_fetch_fhir_ttl_html_rejected(self, mock_get):
        from back.core.errors import InfrastructureError

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b"<!DOCTYPE html>"
        mock_resp.text = "<!DOCTYPE html><html>Error</html>"
        mock_get.return_value = mock_resp

        with pytest.raises(InfrastructureError):
            FhirImportService._fetch_fhir_ttl()

    @patch("requests.get")
    def test_fetch_fhir_ttl_http_error(self, mock_get):
        from back.core.errors import InfrastructureError

        mock_resp = MagicMock()
        mock_resp.status_code = 503
        mock_get.return_value = mock_resp

        with pytest.raises(InfrastructureError):
            FhirImportService._fetch_fhir_ttl()

    def test_fetch_and_parse_empty_domains_raises(self):
        from back.core.errors import ValidationError

        with pytest.raises(ValidationError):
            FhirImportService.fetch_and_parse_fhir([])

    # ------------------------------------------------------------------
    # Version support
    # ------------------------------------------------------------------

    def test_get_fhir_versions_returns_list(self):
        versions = FhirImportService.get_fhir_versions()
        assert isinstance(versions, list)
        assert len(versions) == len(FhirImportService.FHIR_VERSIONS)

    def test_get_fhir_versions_entry_fields(self):
        for v in FhirImportService.get_fhir_versions():
            assert "key" in v
            assert "label" in v
            assert "default" in v

    def test_get_fhir_versions_has_r4_r4b_r5(self):
        keys = {v["key"] for v in FhirImportService.get_fhir_versions()}
        assert {"R4", "R4B", "R5"} <= keys

    def test_get_fhir_versions_exactly_one_default(self):
        defaults = [v for v in FhirImportService.get_fhir_versions() if v["default"]]
        assert len(defaults) == 1
        assert defaults[0]["key"] == FhirImportService.DEFAULT_VERSION

    @patch("requests.get")
    def test_fetch_fhir_ttl_uses_correct_url_for_r4(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b"@prefix fhir: <http://hl7.org/fhir/> ."
        mock_resp.text = "@prefix fhir: <http://hl7.org/fhir/> ."
        mock_get.return_value = mock_resp

        FhirImportService._fetch_fhir_ttl("R4")

        called_url = mock_get.call_args[0][0]
        assert "R4" in called_url
        assert "fhir.ttl" in called_url
        assert "R5" not in called_url

    @patch("requests.get")
    def test_fetch_fhir_ttl_uses_correct_url_for_r4b(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b"@prefix fhir: <http://hl7.org/fhir/> ."
        mock_resp.text = "@prefix fhir: <http://hl7.org/fhir/> ."
        mock_get.return_value = mock_resp

        FhirImportService._fetch_fhir_ttl("R4B")

        called_url = mock_get.call_args[0][0]
        assert "R4B" in called_url
        assert "fhir.ttl" in called_url

    @patch("requests.get")
    def test_fetch_fhir_ttl_uses_correct_url_for_r5(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b"@prefix fhir: <http://hl7.org/fhir/> ."
        mock_resp.text = "@prefix fhir: <http://hl7.org/fhir/> ."
        mock_get.return_value = mock_resp

        FhirImportService._fetch_fhir_ttl("R5")

        called_url = mock_get.call_args[0][0]
        assert "R5" in called_url
        assert "fhir.ttl" in called_url

    def test_fetch_fhir_ttl_unknown_version_raises(self):
        from back.core.errors import ValidationError

        with pytest.raises(ValidationError, match="Unknown FHIR version"):
            FhirImportService._fetch_fhir_ttl("R99")

    def test_fetch_and_parse_unknown_version_raises(self):
        from back.core.errors import ValidationError

        with pytest.raises(ValidationError):
            FhirImportService.fetch_and_parse_fhir(["FOUNDATION"], version="R99")

    def test_transform_ontology_info_contains_version(self):
        from rdflib import Graph

        for ver in ("R4", "R4B", "R5"):
            result = FhirImportService._transform_fhir_to_ontobricks(
                Graph(), ["FOUNDATION"], version=ver
            )
            info = result["ontology_info"]
            assert info["version"] == ver
            assert ver in info["name"]
            assert ver in info["description"]

    def test_transform_ontology_info_url_matches_version(self):
        """ontology_info.description must reference the correct HL7 base URL."""
        from rdflib import Graph

        for ver in ("R4", "R4B", "R5"):
            result = FhirImportService._transform_fhir_to_ontobricks(
                Graph(), ["FOUNDATION"], version=ver
            )
            desc = result["ontology_info"]["description"]
            expected_base = FhirImportService.FHIR_VERSIONS[ver]["base_url"]
            assert expected_base in desc

    # ------------------------------------------------------------------
    # Property extraction — object properties
    # ------------------------------------------------------------------

    def test_transform_extracts_object_properties_from_restrictions(self):
        """Object properties (range = non-primitive class) appear in top-level properties list."""
        from rdflib import Graph, RDF, OWL, RDFS, URIRef, Literal, BNode

        graph = Graph()
        fhir_ns = "http://hl7.org/fhir/"

        patient_uri = URIRef(f"{fhir_ns}Patient")
        name_prop = URIRef(f"{fhir_ns}name")
        human_name_type = URIRef(f"{fhir_ns}HumanName")

        graph.add((patient_uri, RDF.type, OWL.Class))
        graph.add((patient_uri, RDFS.label, Literal("Patient")))

        graph.add((name_prop, RDF.type, OWL.ObjectProperty))
        graph.add((name_prop, RDFS.label, Literal("name")))

        restriction = BNode()
        graph.add((restriction, RDF.type, OWL.Restriction))
        graph.add((restriction, OWL.onProperty, name_prop))
        graph.add((restriction, OWL.allValuesFrom, human_name_type))
        graph.add((patient_uri, RDFS.subClassOf, restriction))

        result = FhirImportService._transform_fhir_to_ontobricks(graph, ["CLINICAL"])
        prop_names = {p["name"] for p in result["properties"]}
        assert "name" in prop_names

        name_prop_entry = next(p for p in result["properties"] if p["name"] == "name")
        assert name_prop_entry["domain"] == "Patient"
        assert name_prop_entry["range"] == "HumanName"

    # ------------------------------------------------------------------
    # Multi-domain union
    # ------------------------------------------------------------------

    def test_build_allowed_resources_multi_domain_union(self):
        allowed = FhirImportService._build_allowed_resources(["CLINICAL", "FINANCIAL"])
        # CLINICAL resources
        assert "Patient" in allowed
        assert "Encounter" in allowed
        # FINANCIAL resources
        assert "Claim" in allowed
        assert "Coverage" in allowed
        # FOUNDATION always present
        assert "Resource" in allowed

    def test_transform_multi_domain_includes_all_classes(self):
        """Selecting multiple domains returns classes from each."""
        from rdflib import Graph, RDF, OWL, RDFS, URIRef, Literal

        graph = Graph()
        fhir_ns = "http://hl7.org/fhir/"

        for local_name in ("Patient", "Claim", "Observation"):
            uri = URIRef(f"{fhir_ns}{local_name}")
            graph.add((uri, RDF.type, OWL.Class))
            graph.add((uri, RDFS.label, Literal(local_name)))

        result = FhirImportService._transform_fhir_to_ontobricks(
            graph, ["CLINICAL", "FINANCIAL", "DIAGNOSTICS"]
        )
        class_names = {c["name"] for c in result["classes"]}
        assert "Patient" in class_names
        assert "Claim" in class_names
        assert "Observation" in class_names

    # ------------------------------------------------------------------
    # fetch_and_parse_fhir end-to-end (mocked HTTP)
    # ------------------------------------------------------------------

    @patch("requests.get")
    def test_fetch_and_parse_default_version_is_r5(self, mock_get):
        """When version is omitted the R5 URL is used."""
        minimal_ttl = "@prefix fhir: <http://hl7.org/fhir/> ."
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = minimal_ttl.encode()
        mock_resp.text = minimal_ttl
        mock_get.return_value = mock_resp

        result = FhirImportService.fetch_and_parse_fhir(["FOUNDATION"])

        called_url = mock_get.call_args[0][0]
        assert "R5" in called_url
        assert result["success"] is True

    @patch("requests.get")
    def test_fetch_and_parse_r4_uses_r4_url(self, mock_get):
        minimal_ttl = "@prefix fhir: <http://hl7.org/fhir/> ."
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = minimal_ttl.encode()
        mock_resp.text = minimal_ttl
        mock_get.return_value = mock_resp

        result = FhirImportService.fetch_and_parse_fhir(["FOUNDATION"], version="R4")

        called_url = mock_get.call_args[0][0]
        assert "R4" in called_url
        assert "R5" not in called_url
        assert result["success"] is True
        assert result["ontology_info"]["version"] == "R4"

    @patch("requests.get")
    def test_fetch_and_parse_version_case_insensitive(self, mock_get):
        """Lower-case 'r5' should be normalised and work."""
        minimal_ttl = "@prefix fhir: <http://hl7.org/fhir/> ."
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = minimal_ttl.encode()
        mock_resp.text = minimal_ttl
        mock_get.return_value = mock_resp

        result = FhirImportService.fetch_and_parse_fhir(["FOUNDATION"], version="r5")
        assert result["success"] is True
        assert result["ontology_info"]["version"] == "R5"

    @patch("requests.get")
    def test_fetch_and_parse_auto_includes_foundation(self, mock_get):
        """FOUNDATION is injected even when not explicitly listed."""
        minimal_ttl = "@prefix fhir: <http://hl7.org/fhir/> ."
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = minimal_ttl.encode()
        mock_resp.text = minimal_ttl
        mock_get.return_value = mock_resp

        result = FhirImportService.fetch_and_parse_fhir(["CLINICAL"])
        class_names = {c["name"] for c in result["classes"]}
        # Base types always injected
        assert "Resource" in class_names
        assert "DomainResource" in class_names
