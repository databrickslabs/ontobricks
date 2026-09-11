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


def test_load_published_domain_data_cached_serves_from_cache():
    from back.objects.registry.registry_cache import invalidate_registry_cache

    invalidate_registry_cache()
    svc = _svc()
    _write(svc, "demo", "1", "PUBLISHED")

    ok, _data, ver, err = svc.load_published_domain_data_cached("demo")
    assert ok and ver == "1" and err == ""

    # Directly mutate the store without going through the service (so no
    # cache invalidation) — the cached result must still be served.
    svc._store.write_version(
        "demo", "2", {"info": {"name": "demo", "status": "PUBLISHED"}}
    )
    ok2, _d2, ver2, _e2 = svc.load_published_domain_data_cached("demo")
    assert ver2 == "1"  # cache hit, not the new v2

    # A version write *through the service* invalidates the cache.
    svc.write_version("demo", "3", {"info": {"name": "demo", "status": "PUBLISHED"}})
    ok3, _d3, ver3, _e3 = svc.load_published_domain_data_cached("demo")
    assert ver3 == "3"
    invalidate_registry_cache()


def test_list_mcp_domains_filters_published_via_metadata():
    from back.objects.registry.registry_cache import invalidate_registry_cache

    invalidate_registry_cache()
    svc = _svc()
    _write(svc, "pub", "1", "PUBLISHED")
    _write(svc, "draft_only", "1", "DRAFT")

    ok, items, _ = svc.list_mcp_domains()
    assert ok
    names = {d["name"] for d in items}
    assert names == {"pub"}
    invalidate_registry_cache()


def test_list_mcp_domains_reports_latest_published_graph_backend():
    svc = _svc()
    svc.list_domain_details_cached = lambda: (
        True,
        [
            {
                "name": "ontology_only",
                "description": "Ontology without a graph",
                "versions": [
                    {
                        "version": "2",
                        "status": "PUBLISHED",
                        "graph_backend": "none",
                        "last_build": "",
                    },
                    {
                        "version": "1",
                        "status": "PUBLISHED",
                        "graph_backend": "lakebase",
                        "last_build": "2026-08-01T00:00:00Z",
                    },
                ],
            }
        ],
        "",
    )

    ok, items, _ = svc.list_mcp_domains()

    assert ok
    assert items[0]["graph_backend"] == "none"
    assert items[0]["has_graph"] is False
