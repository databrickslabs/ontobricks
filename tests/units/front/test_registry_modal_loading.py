"""Contracts for responsive Registry modal data loading."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
REGISTRY_JS = REPO_ROOT / "src/front/static/registry/js/registry.js"
REGISTRY_MODAL_JS = REPO_ROOT / "src/front/static/global/js/registry-modal.js"


def test_registry_modal_events_identify_their_source():
    source = REGISTRY_MODAL_JS.read_text(encoding="utf-8")
    assert source.count("source: 'registry-modal'") == 2


def test_registry_domain_loads_are_deduplicated_and_cached_briefly():
    source = REGISTRY_JS.read_text(encoding="utf-8")
    assert "let domainsLoadPromise = null;" in source
    assert "const DOMAINS_CACHE_TTL_MS = 15000;" in source
    assert "if (domainsLoadPromise) return domainsLoadPromise;" in source
    assert "loadRegistryDomains(true)" in source


def test_registry_modal_prefetches_bridges_with_request_deduplication():
    source = REGISTRY_JS.read_text(encoding="utf-8")
    assert "let bridgesLoadPromise = null;" in source
    assert "if (bridgesLoadPromise) return bridgesLoadPromise;" in source
    assert "e.detail?.source === 'registry-modal'" in source
    assert "loadRegistryBridges();" in source
    assert "loadRegistryBridges(true)" in source


def test_domain_mutations_invalidate_prefetched_bridges():
    source = REGISTRY_JS.read_text(encoding="utf-8")
    assert "function invalidateRegistryBridges()" in source
    assert source.count("invalidateRegistryBridges();") >= 3
