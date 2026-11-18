"""Contracts for responsive Registry data loading on the Home tabs."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
REGISTRY_JS = REPO_ROOT / "src/front/static/registry/js/registry.js"
HOME_JS = REPO_ROOT / "src/front/static/home/js/home.js"


def test_home_tab_events_identify_their_source():
    source = HOME_JS.read_text(encoding="utf-8")
    assert source.count("source: 'home'") >= 1
    assert "homeRegistryModal" in source
    assert "sidebarSectionChanged" in source


def test_registry_domain_loads_are_deduplicated_and_cached_briefly():
    source = REGISTRY_JS.read_text(encoding="utf-8")
    assert "let domainsLoadPromise = null;" in source
    assert "const DOMAINS_CACHE_TTL_MS = 15000;" in source
    assert "if (domainsLoadPromise) return domainsLoadPromise;" in source
    assert "loadRegistryDomains(true)" in source


def test_registry_bridges_prefetch_with_request_deduplication():
    source = REGISTRY_JS.read_text(encoding="utf-8")
    assert "let bridgesLoadPromise = null;" in source
    assert "if (bridgesLoadPromise) return bridgesLoadPromise;" in source
    assert "if (section === 'bridges') {" in source
    assert "loadRegistryBridges(true)" in source


def test_domain_mutations_invalidate_prefetched_bridges():
    source = REGISTRY_JS.read_text(encoding="utf-8")
    assert "function invalidateRegistryBridges()" in source
    assert source.count("invalidateRegistryBridges();") >= 3
