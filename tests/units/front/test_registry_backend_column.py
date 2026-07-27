"""Contract: Registry Browse domain table exposes a Backend column after URI."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
REGISTRY_JS = REPO_ROOT / "src/front/static/registry/js/registry.js"


def _source() -> str:
    return REGISTRY_JS.read_text(encoding="utf-8")


def test_registry_browse_has_backend_column_after_uri():
    source = _source()
    start = source.index("async function loadRegistryDomains")
    # Next top-level async function after the domain list renderer.
    end = source.index("async function ", start + len("async function loadRegistryDomains"))
    body = source[start:end]

    uri_idx = body.index(">URI</th>")
    backend_idx = body.index(">Backend</th>")
    desc_idx = body.index(">Description</th>")
    assert uri_idx < backend_idx < desc_idx
    assert "d.graph_backend" in body
    assert "colspan=\"6\"" in body
    assert "Lakebase" in body
    assert "Lakehouse" in body
    assert "Neo4j" in body
