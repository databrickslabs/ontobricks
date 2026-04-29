"""Tests for DomainSession."""

import pytest
from unittest.mock import MagicMock, patch

from back.objects.session.DomainSession import DomainSession, get_empty_domain


class TestGetEmptyDomain:
    def test_has_required_keys(self):
        data = get_empty_domain()
        assert "domain" in data
        assert "ontology" in data
        assert "assignment" in data
        assert "design_layout" in data
        assert "settings" in data
        assert "databricks" in data["settings"]
        assert "registry" in data["settings"]

    def test_default_name(self):
        data = get_empty_domain()
        assert data["domain"]["info"]["name"] == "NewDomain"

    def test_default_registry(self):
        data = get_empty_domain()
        reg = data["settings"]["registry"]
        assert reg["catalog"] == ""
        assert reg["volume"] == "OntoBricksRegistry"

    def test_no_persisted_reasoning_metadata_under_domain(self):
        data = get_empty_domain()
        assert "reasoning" not in data
        assert "reasoning" not in data["ontology"]
        assert data["domain"]["metadata"] == {}

    def test_migrates_root_metadata_and_reasoning(self, mock_session_mgr):
        empty = get_empty_domain()
        mock_session_mgr.set(
            "project_data",
            {
                "domain": {**empty["domain"]},
                "ontology": {**empty["ontology"], "classes": []},
                "assignment": {"entities": [], "relationships": []},
                "design_layout": empty["design_layout"],
                "settings": empty["settings"],
                "metadata": {"catalog": "c", "schema": "s", "tables": [{"name": "t"}]},
                "reasoning": {
                    "last_run": "x",
                    "inferred_count": 3,
                    "violations_count": 0,
                    "inferred_triples": [],
                    "violations": [],
                },
            },
        )
        ds = DomainSession(mock_session_mgr)
        assert ds.catalog_metadata.get("catalog") == "c"
        assert "reasoning" not in ds._data
        assert "reasoning" not in ds.ontology
        assert "metadata" not in ds._data


class TestUcDomainPath:
    def test_uc_domain_path_prefers_registry_volume_path(self, mock_session_mgr):
        data = get_empty_domain()
        data["settings"]["registry"] = {
            "catalog": "stale_cat",
            "schema": "stale_sch",
            "volume": "stale_vol",
        }
        data["domain"]["domain_folder"] = "export_me"
        mock_session_mgr.set("domain_data", data)
        ds = DomainSession(mock_session_mgr)

        fake_settings = MagicMock()
        fake_settings.registry_volume_path = (
            "/Volumes/benoit_cayla/ontobricks_deployed/registry"
        )
        fake_settings.registry_catalog = ""
        fake_settings.registry_schema = ""
        fake_settings.registry_volume = ""

        with patch("shared.config.settings.get_settings", return_value=fake_settings):
            assert (
                ds.uc_domain_path
                == "/Volumes/benoit_cayla/ontobricks_deployed/registry/domains/export_me"
            )

    def test_uc_version_path_includes_version(self, mock_session_mgr):
        data = get_empty_domain()
        data["settings"]["registry"] = {
            "catalog": "cat",
            "schema": "sch",
            "volume": "vol",
        }
        data["domain"]["domain_folder"] = "my_domain"
        data["domain"]["current_version"] = "3"
        mock_session_mgr.set("domain_data", data)
        ds = DomainSession(mock_session_mgr)

        fake_settings = MagicMock()
        fake_settings.registry_volume_path = ""
        fake_settings.registry_catalog = "cat"
        fake_settings.registry_schema = "sch"
        fake_settings.registry_volume = "vol"

        with patch("shared.config.settings.get_settings", return_value=fake_settings):
            assert ds.uc_version_path == "/Volumes/cat/sch/vol/domains/my_domain/V3"


class TestDomainSessionDelta:
    """``domain.delta`` must follow the same RegistryCfg precedence as
    ``uc_domain_path`` / ``uc_version_path``. Otherwise the Build-page
    Triple-Store badge resolves ``effective_view_table`` against
    whatever was persisted in ``settings["registry"]`` even when the
    active backend pointed at a *different* triplet (e.g. Lakebase row
    overrides the Volume binding).
    """

    def test_delta_uses_registry_volume_path_over_stale_settings(self, mock_session_mgr):
        # Session has stale catalog/schema saved (carry-over from a
        # prior backend), but the Apps runtime now binds a different
        # Volume. ``delta`` must follow the binding, not the stale
        # session — same precedence as ``uc_domain_path``.
        data = get_empty_domain()
        data["settings"]["registry"] = {
            "catalog": "stale_cat",
            "schema": "stale_sch",
            "volume": "stale_vol",
        }
        data["domain"]["info"]["name"] = "Cust360Auto"
        data["domain"]["current_version"] = "4"
        mock_session_mgr.set("domain_data", data)
        ds = DomainSession(mock_session_mgr)

        fake_settings = MagicMock()
        fake_settings.registry_volume_path = (
            "/Volumes/benoit_cayla/ontobricks_deployed/registry"
        )
        fake_settings.registry_backend = "volume"
        fake_settings.registry_catalog = ""
        fake_settings.registry_schema = ""
        fake_settings.registry_volume = ""
        fake_settings.lakebase_schema = "ontobricks_registry"
        fake_settings.lakebase_database = ""

        with patch("shared.config.settings.get_settings", return_value=fake_settings):
            d = ds.delta
        assert d["catalog"] == "benoit_cayla"
        assert d["schema"] == "ontobricks_deployed"
        assert d["table_name"] == "triplestore_cust360auto_V4"

    def test_delta_uses_lakebase_row_when_backend_lakebase(self, mock_session_mgr):
        # The whole point of the fix: when the active backend is
        # Lakebase, ``delta`` must pick up the catalog/schema stored
        # in the Lakebase ``registries`` row, not the Volume binding.
        data = get_empty_domain()
        data["settings"]["registry"] = {
            "catalog": "stale_cat",
            "schema": "stale_sch",
            "volume": "stale_vol",
            "backend": "lakebase",
        }
        data["domain"]["info"]["name"] = "Cust360Auto"
        data["domain"]["current_version"] = "4"
        mock_session_mgr.set("domain_data", data)
        ds = DomainSession(mock_session_mgr)

        fake_settings = MagicMock()
        fake_settings.registry_volume_path = (
            "/Volumes/benoit_cayla/ontobricks_deployed/registry"
        )
        fake_settings.registry_backend = "lakebase"
        fake_settings.registry_catalog = ""
        fake_settings.registry_schema = ""
        fake_settings.registry_volume = ""
        fake_settings.lakebase_schema = "ontobricks_registry"
        fake_settings.lakebase_database = ""

        from back.objects.registry.store.lakebase import store as _lb_store

        with patch("shared.config.settings.get_settings", return_value=fake_settings), \
             patch.object(
                _lb_store,
                "fetch_lakebase_registry_triplet",
                lambda schema, database="": (
                    "benoit_cayla",
                    "ontobricks",
                    "OntoBricksRegistry",
                ),
             ):
            d = ds.delta
        # Catalog/schema come from the Lakebase row, not the Volume
        # binding — the existence check now points at where artefacts
        # actually live.
        assert d["catalog"] == "benoit_cayla"
        assert d["schema"] == "ontobricks"
        assert d["table_name"] == "triplestore_cust360auto_V4"

    def test_delta_falls_back_to_session_when_resolver_raises(self, mock_session_mgr):
        # Fail-soft contract: ``delta`` must never raise. If
        # ``RegistryCfg.from_domain`` blows up (e.g. settings module
        # not importable in some test harness), we degrade to the raw
        # session values rather than 500-ing the whole Build page.
        data = get_empty_domain()
        data["settings"]["registry"] = {
            "catalog": "fallback_cat",
            "schema": "fallback_sch",
            "volume": "fallback_vol",
        }
        data["domain"]["info"]["name"] = "Demo"
        data["domain"]["current_version"] = "1"
        mock_session_mgr.set("domain_data", data)
        ds = DomainSession(mock_session_mgr)

        with patch(
            "shared.config.settings.get_settings",
            side_effect=RuntimeError("settings broken"),
        ):
            d = ds.delta
        assert d["catalog"] == "fallback_cat"
        assert d["schema"] == "fallback_sch"
        assert d["table_name"] == "triplestore_demo_V1"


class TestDomainSession:
    def test_init_empty(self, mock_session_mgr):
        ds = DomainSession(mock_session_mgr)
        assert ds.info["name"] == "NewDomain"

    def test_info_property(self, domain_session):
        assert "name" in domain_session.info

    def test_set_and_get_info(self, domain_session):
        domain_session.info["name"] = "Test"
        assert domain_session.info["name"] == "Test"

    def test_current_version(self, domain_session):
        assert domain_session.current_version == "1"
        domain_session.current_version = "2"
        assert domain_session.current_version == "2"

    def test_ontology(self, domain_session):
        assert isinstance(domain_session.ontology, dict)
        assert "classes" in domain_session.ontology

    def test_get_classes_empty(self, domain_session):
        assert domain_session.get_classes() == []

    def test_get_properties_empty(self, domain_session):
        assert domain_session.get_properties() == []

    def test_add_class(self, domain_session):
        domain_session.ontology["classes"].append(
            {"name": "Foo", "uri": "http://t/Foo"}
        )
        assert len(domain_session.get_classes()) == 1

    def test_assignment(self, domain_session):
        assert isinstance(domain_session.assignment, dict)
        assert domain_session.get_entity_mappings() == []

    def test_generated_lazy_init(self, domain_session):
        gen = domain_session.generated
        assert gen == {"owl": "", "sql": "", "r2rml": ""}

    def test_get_set_r2rml(self, domain_session):
        assert domain_session.get_r2rml() == ""
        domain_session.set_r2rml("some r2rml")
        assert domain_session.get_r2rml() == "some r2rml"

    def test_clear_generated_content(self, domain_session):
        domain_session.set_r2rml("content")
        domain_session.generated["owl"] = "owl content"
        domain_session.clear_generated_content()
        assert domain_session.get_r2rml() == ""
        assert domain_session.generated["owl"] == ""


class TestSaveAndReset:
    def test_save(self, mock_session_mgr, domain_session):
        domain_session.info["name"] = "Saved"
        domain_session.save()
        data = mock_session_mgr.get("domain_data")
        assert data is not None
        assert data["domain"]["info"]["name"] == "Saved"

    def test_save_excludes_generated(self, mock_session_mgr, domain_session):
        domain_session.generated["owl"] = "test owl"
        domain_session.save()
        data = mock_session_mgr.get("domain_data")
        assert "generated" not in data

    def test_reset(self, mock_session_mgr, domain_session):
        domain_session.info["name"] = "Before Reset"
        domain_session.save()
        domain_session.reset()
        assert domain_session.info["name"] == "NewDomain"


class TestExportImport:
    def test_export_for_save(self, domain_session):
        domain_session.info["name"] = "Export Test"
        domain_session.ontology["base_uri"] = "http://test.org#"
        domain_session.ontology["classes"] = [{"name": "A"}]
        export = domain_session.export_for_save()
        assert export["info"]["name"] == "Export Test"
        assert "versions" in export

    def test_import_from_file(self, domain_session):
        domain_data = {
            "info": {"name": "Imported", "description": "Test import"},
            "versions": {
                "1": {
                    "ontology": {
                        "name": "ImpOntology",
                        "base_uri": "http://imp.org#",
                        "classes": [{"name": "Imp"}],
                        "properties": [],
                        "constraints": [],
                        "swrl_rules": [],
                        "axioms": [],
                        "expressions": [],
                    },
                    "assignment": {"entities": [], "relationships": []},
                    "design_layout": {"views": {}, "map": {}},
                }
            },
        }
        domain_session.import_from_file(domain_data)
        assert domain_session.info["name"] == "Imported"
        assert len(domain_session.get_classes()) == 1


class TestLegacyMigration:
    def test_migrates_flat_constraints(self, mock_session_mgr):
        mock_session_mgr.set(
            "project_data",
            {
                "ontology": {
                    "classes": [{"name": "A"}],
                    "properties": [],
                },
                "constraints": [{"type": "functional", "property": "p"}],
                "swrl_rules": [],
                "axioms": [],
            },
        )
        ds = DomainSession(mock_session_mgr)
        assert len(ds.constraints) == 1
        assert ds.constraints[0]["type"] == "functional"

    def test_migrates_mapping_key(self, mock_session_mgr):
        mock_session_mgr.set(
            "project_data",
            {
                "ontology": {"classes": [], "properties": []},
                "mapping": {
                    "data_source_mappings": [{"ontology_class": "A"}],
                    "relationship_mappings": [],
                },
            },
        )
        ds = DomainSession(mock_session_mgr)
        assert len(ds.get_entity_mappings()) == 1
