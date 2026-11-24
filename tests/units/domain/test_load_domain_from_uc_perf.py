"""load_domain_from_uc must stay lean: no eager OWL/sims, one session save."""

from unittest.mock import MagicMock, patch

import pytest

from back.objects.domain.Domain import Domain

pytestmark = pytest.mark.unit


def _session():
    s = MagicMock()
    s.get_entity_mappings.return_value = []
    s.get_relationship_mappings.return_value = []
    s.get_classes.return_value = []
    s.info = {"status": "DRAFT", "name": "Acme"}
    s.settings = {}
    s.triplestore = {}
    s.domain_folder = ""
    s.is_active_version = True
    s.current_version = "1"
    s.ontology = {}
    s.registry = {"catalog": "c", "schema": "s", "volume": "v"}
    s.delta = {}
    return s


def test_load_skips_simulations_and_ensure_generated_and_saves_once():
    session = _session()
    settings = MagicMock()
    domain = Domain(session, settings)

    svc = MagicMock()
    svc.cfg.catalog = "c"
    svc.cfg.schema = "s"
    svc.cfg.volume = "v"
    svc.read_version.return_value = (
        True,
        {"info": {"name": "Acme", "status": "DRAFT"}, "versions": {"1": {}}},
        "",
    )
    svc.list_versions_sorted.return_value = ["1"]

    with patch.object(
        domain,
        "get_domain_info",
        return_value={"success": True, "info": {"name": "Acme"}, "stats": {}},
    ), patch(
        "back.objects.domain.Domain.clear_version_status_cache"
    ), patch(
        "back.objects.domain.Domain.invalidate_registry_cache"
    ):
        result = domain.load_domain_from_uc(svc, "Acme", "1")

    svc.read_version.assert_called_once_with(
        "Acme", "1", include_simulations=False
    )
    session.ensure_generated_content.assert_not_called()
    assert session.save.call_count == 1
    assert result["success"] is True
    assert "domain" in result


def test_catalog_read_version_can_skip_simulations_attach():
    from back.objects.registry.store.lakebase.Catalog import Catalog

    calls = {"attach": 0}

    class Stub(Catalog):
        def __init__(self):
            self._status_column_ready = True
            self._quorum_column_ready = True
            self._mcp_policy_column_ready = True
            self._schema = "ontobricks_registry"

        def _q(self, s):
            return f'"{s}"'

        def _registry(self):
            return 1

        def _connect(self):
            raise AssertionError("should be mocked")

        def attach_simulations_to_doc(self, doc, folder, version):
            calls["attach"] += 1

    stub = Stub()
    row = {
        "info": {},
        "ontology": {},
        "assignment": {},
        "design_layout": {},
        "metadata": {},
        "version": "1",
        "mcp_enabled": False,
        "status": "DRAFT",
        "last_update": None,
        "last_build": None,
        "review_quorum": 1,
        "mcp_policy": None,
        "domain_base_uri": "",
    }

    class Cur:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, *a, **k):
            return None

        def fetchone(self):
            return row

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def cursor(self, **k):
            return Cur()

    stub._connect = lambda: Conn()  # type: ignore
    stub._ensure_domain_versions_status_column = lambda: True  # type: ignore
    stub._ensure_domains_review_quorum_column = lambda: True  # type: ignore
    stub._ensure_domains_mcp_policy_column = lambda: True  # type: ignore

    ok, doc, msg = stub.read_version("Acme", "1", include_simulations=False)
    assert ok and msg == ""
    assert calls["attach"] == 0

    ok, doc, msg = stub.read_version("Acme", "1", include_simulations=True)
    assert ok
    assert calls["attach"] == 1
