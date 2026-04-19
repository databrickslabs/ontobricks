"""Tests for industry import services – catalogs and module collection (pure/unit)."""

import pytest
from unittest.mock import patch, MagicMock

from back.core.industry.fibo.FiboImportService import FiboImportService
from back.core.industry.cdisc.CdiscImportService import CdiscImportService
from back.core.industry.iof.IofImportService import IofImportService


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
