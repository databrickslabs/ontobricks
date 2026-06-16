"""RegistryService PUBLISHED-resolution tests.

Exercises the lifecycle-aware resolvers introduced with the domain
status lifecycle: ``find_published_version``, ``load_published_domain_data``
and ``set_version_status``, using the in-memory fake store.
"""

from back.objects.registry import RegistryCfg, RegistryService

from tests.units.registry.test_registry_store import _InMemoryStore


def _svc():
    cfg = RegistryCfg(catalog="c", schema="s", volume="v")
    return RegistryService(cfg, uc=None, store=_InMemoryStore())


def _write(svc, folder, version, status):
    svc._store.write_version(
        folder, version, {"info": {"name": folder, "status": status}}
    )


def test_find_published_returns_latest_published():
    svc = _svc()
    _write(svc, "demo", "1", "PUBLISHED")
    _write(svc, "demo", "2", "DRAFT")
    _write(svc, "demo", "3", "PUBLISHED")

    ver, data = svc.find_published_version("demo")
    assert ver == "3"
    assert data["info"]["status"] == "PUBLISHED"


def test_find_published_none_when_no_published():
    svc = _svc()
    _write(svc, "demo", "1", "DRAFT")
    _write(svc, "demo", "2", "IN-REVIEW")

    ver, data = svc.find_published_version("demo")
    assert ver is None
    assert data == {}


def test_load_published_domain_data_no_fallback():
    svc = _svc()
    _write(svc, "demo", "1", "DRAFT")

    ok, data, ver, err = svc.load_published_domain_data("demo")
    assert ok is False
    assert ver == ""
    assert "PUBLISHED" in err


def test_load_published_domain_data_returns_published():
    svc = _svc()
    _write(svc, "demo", "1", "PUBLISHED")
    _write(svc, "demo", "2", "DRAFT")

    ok, data, ver, err = svc.load_published_domain_data("demo")
    assert ok is True
    assert ver == "1"
    assert err == ""


def test_find_mcp_version_is_published_alias():
    svc = _svc()
    _write(svc, "demo", "1", "PUBLISHED")
    assert svc.find_mcp_version("demo") == svc.find_published_version("demo")


def test_set_version_status_delegates_to_store():
    svc = _svc()
    _write(svc, "demo", "1", "DRAFT")
    ok, msg = svc.set_version_status("demo", "1", "IN-REVIEW")
    assert ok, msg
    _, data, _ = svc.read_version("demo", "1")
    assert data["info"]["status"] == "IN-REVIEW"
