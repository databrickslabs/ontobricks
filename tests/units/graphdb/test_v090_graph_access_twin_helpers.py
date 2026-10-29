"""0.9.0 extracted twin helpers: TwinGraphAccess, TwinResolve, TwinSparqlTranslate."""

from __future__ import annotations

import builtins
import sys
from types import SimpleNamespace

import pytest

from back.core.errors import InfrastructureError, NotFoundError, ValidationError
from back.objects.digitaltwin.DigitalTwin import DigitalTwin
from back.objects.digitaltwin.TwinGraphAccess import TwinGraphAccess
from back.objects.digitaltwin.TwinResolve import TwinResolve
from back.objects.digitaltwin.query.TwinSparqlTranslate import TwinSparqlTranslate

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# TwinGraphAccess
# ---------------------------------------------------------------------------


class TestTwinGraphAccess:
    def test_graph_query_table_forwards_store_and_inferred_flag(self, monkeypatch):
        seen = {}

        def fake(domain, settings, *, include_inferred, store):
            seen.update(domain=domain, settings=settings, inf=include_inferred, store=store)
            return "cat.sch.g_data"

        monkeypatch.setattr(sys.modules[TwinGraphAccess.__module__], "effective_graph_query_table", fake)
        out = TwinGraphAccess.graph_query_table("d", "s", "st", include_inferred=False)
        assert out == "cat.sch.g_data"
        assert seen == {"domain": "d", "settings": "s", "inf": False, "store": "st"}

    def test_source_from_payload_coerces_flags(self):
        assert TwinGraphAccess.source_from_payload(None) == (True, None)
        assert TwinGraphAccess.source_from_payload({"include_inferred": "false"}) == (
            False,
            None,
        )
        assert TwinGraphAccess.source_from_payload(
            {"include_inferred": False, "simulation_id": " s1 "}
        ) == (False, "s1")
        assert TwinGraphAccess.source_from_payload({"simulation_id": "live"}) == (
            True,
            None,
        )
        assert TwinGraphAccess.source_from_payload(
            {"simulation_id": "live:inferred"}
        ) == (True, None)

    def test_graph_query_table_uses_simulation_union(self, monkeypatch):
        from back.objects.simulation import Simulation

        seen = {}

        def fake_explorer(self, sid, *, include_inferred, store):
            seen.update(sid=sid, inf=include_inferred, store=store)
            return f"g__smu_{sid}"

        monkeypatch.setattr(Simulation, "explorer_relation", fake_explorer)
        out = TwinGraphAccess.graph_query_table(
            "d", "s", "st", include_inferred=False, simulation_id="abc"
        )
        assert out == "g__smu_abc"
        assert seen == {"sid": "abc", "inf": False, "store": "st"}

    def test_dataquality_table_requires_view(self):
        with pytest.raises(ValidationError, match="VIEW is not available"):
            TwinGraphAccess.dataquality_table(None, None, view_table_fn=lambda d, s: "  ")

    def test_dataquality_table_strips(self):
        assert TwinGraphAccess.dataquality_table(None, None, view_table_fn=lambda d, s: " v ") == "v"

    def test_require_graph_store_raises_when_unconfigured(self):
        with pytest.raises(InfrastructureError, match="not configured"):
            TwinGraphAccess.require_graph_store(None, None, get_store=lambda d, s: None)

    def test_require_graph_store_returns_store(self):
        assert TwinGraphAccess.require_graph_store(None, None, get_store=lambda d, s: "S") == "S"

    @pytest.mark.parametrize(
        "folder,version", [("", "1"), ("dom", ""), (None, None)]
    )
    def test_load_metrics_needs_folder_and_version(self, folder, version):
        d = SimpleNamespace(uc_domain_folder=folder, current_version=version)
        assert TwinGraphAccess.load_stored_metrics(d, None) is None

    def test_load_metrics_returns_registry_row(self, monkeypatch):
        from back.objects.registry.RegistryService import RegistryService

        calls = []

        class Svc:
            def load_graph_analytics(self, folder, version):
                calls.append((folder, version))
                return {"nodes": 3}

        monkeypatch.setattr(RegistryService, "from_context", staticmethod(lambda d, s: Svc()))
        d = SimpleNamespace(uc_domain_folder="dom", current_version=2)
        assert TwinGraphAccess.load_stored_metrics(d, None) == {"nodes": 3}
        assert calls == [("dom", "2")]

    def test_load_metrics_never_raises(self, monkeypatch):
        from back.objects.registry.RegistryService import RegistryService

        def boom(d, s):
            raise RuntimeError("registry offline")

        monkeypatch.setattr(RegistryService, "from_context", staticmethod(boom))
        d = SimpleNamespace(uc_domain_folder="dom", current_version="1")
        assert TwinGraphAccess.load_stored_metrics(d, None) is None


# ---------------------------------------------------------------------------
# TwinResolve
# ---------------------------------------------------------------------------


class TestEffectiveBackendLabel:
    def test_explicit_triplestore_backend_wins(self):
        d = SimpleNamespace(triplestore={"backend": "neo4j"}, delta={"catalog": "c"})
        assert TwinResolve(d).effective_backend_label() == "neo4j"

    def test_delta_catalog_means_sql_warehouse(self):
        d = SimpleNamespace(triplestore=None, delta={"catalog": "c"})
        assert TwinResolve(d).effective_backend_label() == "Delta (SQL Warehouse)"

    def test_default_is_lakebase(self):
        assert TwinResolve(SimpleNamespace()).effective_backend_label() == "Lakebase"


class _Domain:
    def __init__(self):
        self.events = []
        self.domain_folder = None

    def clear_generated_content(self):
        self.events.append("clear")

    def import_from_file(self, data, version=None):
        self.events.append(("import", data, version))

    def ensure_generated_content(self):
        self.events.append("generate")

    def save(self):
        self.events.append("save")


class _Svc:
    read_version_result = (True, {"info": {"status": "PUBLISHED"}}, "")
    cached_result = (True, {"k": "cached"}, "3", "")
    live_result = (True, {"k": "live"}, "4", "")

    def __init__(self, cfg, uc):
        self.calls = []

    def read_version(self, name, version):
        return self.read_version_result

    def load_published_domain_data_cached(self, name):
        return self.cached_result

    def load_published_domain_data(self, name):
        return self.live_result

    def load_latest_domain_data(self, name):
        return self.live_result


@pytest.fixture
def resolve_env(monkeypatch):
    import back.objects.registry as registry_pkg

    dt_mod = sys.modules[DigitalTwin.__module__]
    domain = _Domain()
    configured = {"value": True}

    class Cfg:
        @staticmethod
        def from_dict(reg):
            return SimpleNamespace(is_configured=configured["value"])

    svc_cls = type("Svc", (_Svc,), {})
    monkeypatch.setattr(dt_mod, "get_domain", lambda mgr: domain)
    monkeypatch.setattr(DigitalTwin, "resolve_registry", staticmethod(lambda *a: {}))
    monkeypatch.setattr(DigitalTwin, "uc_from_domain", staticmethod(lambda d, s: None))
    monkeypatch.setattr(registry_pkg, "RegistryCfg", Cfg)
    monkeypatch.setattr(registry_pkg, "RegistryService", svc_cls)
    return SimpleNamespace(domain=domain, configured=configured, svc=svc_cls)


class TestResolveDomain:
    def test_no_name_returns_session_domain(self, resolve_env):
        assert TwinResolve.resolve_domain("", None, None) is resolve_env.domain
        assert resolve_env.domain.events == []

    def test_unconfigured_registry_rejected(self, resolve_env):
        resolve_env.configured["value"] = False
        with pytest.raises(ValidationError, match="Registry not configured"):
            TwinResolve.resolve_domain("dom", None, None)

    def test_explicit_published_version_is_imported(self, resolve_env):
        data = {"info": {"status": "PUBLISHED"}}
        resolve_env.svc.read_version_result = (True, data, "")
        d = TwinResolve.resolve_domain("dom", None, None, domain_version="7")
        assert ("import", data, "7") in d.events
        assert d.domain_folder == "dom"
        assert d.events[-2:] == ["generate", "save"]

    def test_explicit_version_not_found(self, resolve_env):
        resolve_env.svc.read_version_result = (False, None, "Version 7 not found")
        with pytest.raises(NotFoundError):
            TwinResolve.resolve_domain("dom", None, None, domain_version="7")

    def test_explicit_version_read_failure_is_infrastructure(self, resolve_env):
        resolve_env.svc.read_version_result = (False, None, "volume unreachable")
        with pytest.raises(InfrastructureError):
            TwinResolve.resolve_domain("dom", None, None, domain_version="7")

    def test_explicit_draft_version_is_imported(self, resolve_env):
        data = {"info": {"status": "DRAFT"}}
        resolve_env.svc.read_version_result = (True, data, "")
        d = TwinResolve.resolve_domain("dom", None, None, domain_version="7")
        assert ("import", data, "7") in d.events

    def test_explicit_in_review_version_is_imported(self, resolve_env):
        data = {"info": {"status": "IN-REVIEW"}}
        resolve_env.svc.read_version_result = (True, data, "")
        d = TwinResolve.resolve_domain("dom", None, None, domain_version="7")
        assert ("import", data, "7") in d.events

    def test_read_only_loads_latest_and_skips_generation_and_save(self, resolve_env):
        d = TwinResolve.resolve_domain("dom", None, None, read_only=True)
        assert d.events == ["clear", ("import", {"k": "live"}, "4")]

    def test_read_only_latest_miss_is_not_found(self, resolve_env):
        resolve_env.svc.live_result = (False, None, None, "no versions")
        with pytest.raises(NotFoundError, match="no versions"):
            TwinResolve.resolve_domain("dom", None, None, read_only=True)

    def test_write_path_uses_live_registry(self, resolve_env):
        d = TwinResolve.resolve_domain("dom", None, None)
        assert ("import", {"k": "live"}, "4") in d.events
        assert d.events[-1] == "save"

    def test_write_path_missing_latest_is_not_found(self, resolve_env):
        resolve_env.svc.live_result = (False, None, None, "missing")
        with pytest.raises(NotFoundError, match="missing"):
            TwinResolve.resolve_domain("dom", None, None)


class TestTwinResolveUtilities:
    def test_make_snapshot_drops_token(self):
        d = SimpleNamespace(info={"a": 1}, databricks={"host": "h", "token": "secret"})
        snap = TwinResolve.make_snapshot(d)
        assert snap.info == {"a": 1}
        assert "token" not in snap.databricks

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("http://x.org/onto#Customer/42", "42"),
            ("http://x.org/onto#Customer", "Customer"),
        ],
    )
    def test_extract_local_id(self, uri, expected):
        assert TwinResolve.extract_local_id(uri) == expected

    def test_owlrl_unavailable_returns_false(self, monkeypatch):
        real_import = builtins.__import__

        def fake_import(name, *a, **kw):
            if name == "owlrl":
                raise ImportError("no owlrl")
            return real_import(name, *a, **kw)

        monkeypatch.delitem(sys.modules, "owlrl", raising=False)
        monkeypatch.setattr(builtins, "__import__", fake_import)
        assert TwinResolve.is_owlrl_available() is False

    @pytest.mark.parametrize("rng,expected", [("xsd:string", True), ("ex:Customer", False)])
    def test_is_datatype_range(self, rng, expected):
        assert TwinResolve.is_datatype_range(rng) is expected


# ---------------------------------------------------------------------------
# TwinSparqlTranslate
# ---------------------------------------------------------------------------


class _TranslateDomain:
    def __init__(self, r2rml):
        self._r2rml = r2rml
        self.generated = False
        self.ontology = {}
        self.assignment = {"entities": []}

    def ensure_generated_content(self):
        self.generated = True

    def get_r2rml(self):
        return self._r2rml


class TestTwinSparqlTranslate:
    def test_missing_r2rml_rejected_after_generation(self):
        d = _TranslateDomain("")
        with pytest.raises(ValidationError, match="No R2RML mapping"):
            TwinSparqlTranslate.translate(d, "SELECT * WHERE {?s ?p ?o}", 10, "http://b#")
        assert d.generated is True

    def test_pipeline_uses_default_base_and_augments_mappings(self, monkeypatch):
        mod = sys.modules[TwinSparqlTranslate.__module__]
        calls = {}

        monkeypatch.setattr(mod.sparql, "extract_r2rml_mappings", lambda c: (["E"], ["R"]))

        def aug_e(em, assignment, base, onto):
            calls["e"] = (em, base)
            return em + ["E2"]

        def aug_r(rm, assignment, base, onto):
            calls["r"] = (rm, base)
            return rm + ["R2"]

        def translate(q, em, limit, rm):
            calls["t"] = (q, em, limit, rm)
            return {"sql": "SELECT 1"}

        monkeypatch.setattr(DigitalTwin, "augment_mappings_from_config", staticmethod(aug_e))
        monkeypatch.setattr(DigitalTwin, "augment_relationships_from_config", staticmethod(aug_r))
        monkeypatch.setattr(mod.sparql, "translate_sparql_to_spark", translate)

        out = TwinSparqlTranslate.translate(_TranslateDomain("@prefix rr: <x> ."), "Q", 25, "http://b#")
        assert out == {"sql": "SELECT 1"}
        assert calls["e"] == (["E"], "http://b#")
        assert calls["r"] == (["R"], "http://b#")
        assert calls["t"] == ("Q", ["E", "E2"], 25, ["R", "R2"])

    def test_ontology_base_uri_overrides_default(self, monkeypatch):
        mod = sys.modules[TwinSparqlTranslate.__module__]
        bases = []
        monkeypatch.setattr(mod.sparql, "extract_r2rml_mappings", lambda c: ([], []))
        monkeypatch.setattr(
            DigitalTwin, "augment_mappings_from_config",
            staticmethod(lambda em, a, base, o: bases.append(base) or em),
        )
        monkeypatch.setattr(
            DigitalTwin, "augment_relationships_from_config",
            staticmethod(lambda rm, a, base, o: rm),
        )
        monkeypatch.setattr(mod.sparql, "translate_sparql_to_spark", lambda *a: {})
        d = _TranslateDomain("r2rml")
        d.ontology = {"base_uri": "http://onto#"}
        TwinSparqlTranslate.translate(d, "Q", None, "http://default#")
        assert bases == ["http://onto#"]
