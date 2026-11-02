"""Backend cloisonnement guardrail (design spec §6 / §8).

No module outside ``src/back/core/graphdb/`` may import a backend package —
``back.core.graphdb.{lakebase,delta,neo4j}.*``. Every backend access goes
through the factory-vended capabilities (``GraphBuilder`` / ``GraphHealthProbe``
/ ``GraphProvisioner`` / ``GraphSimulation``) or the backend-agnostic graphdb
modules (``__init__`` re-exports, ``constants``, ``engine_config``,
``search_cache``, the ``GraphDBFactory`` normalizers).

The allow-list is structural: anything *under* ``back/core/graphdb/`` may import
a backend package (that is the abstraction's own internals); anything else may
not. The invariant is fully satisfied — this test asserts zero offenders rather
than tracking a shrinking baseline.
"""

from __future__ import annotations

import ast
from typing import Iterator, List, Tuple

import pytest

from tests.units.architecture.source_index import SOURCE_INDEX

pytestmark = pytest.mark.unit

_BACKEND_PACKAGES = (
    "back.core.graphdb.lakebase",
    "back.core.graphdb.delta",
    "back.core.graphdb.neo4j",
)

# Modules that legitimately import backend packages: the graphdb layer itself.
_ALLOWED_PREFIX = "back/core/graphdb/"


def _targets_backend(module: str | None) -> bool:
    if not module:
        return False
    return any(
        module == pkg or module.startswith(pkg + ".") for pkg in _BACKEND_PACKAGES
    )


def _backend_imports(tree: ast.Module) -> Iterator[int]:
    """Yield line numbers of imports that reach into a backend package."""
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level == 0 and _targets_backend(node.module):
                yield node.lineno
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if _targets_backend(alias.name):
                    yield node.lineno


def _offenders() -> List[Tuple[str, int]]:
    found: List[Tuple[str, int]] = []
    for module in SOURCE_INDEX.modules:
        if module.rel_path.startswith(_ALLOWED_PREFIX):
            continue
        for lineno in _backend_imports(module.tree):
            found.append((module.rel_path, lineno))
    return sorted(found)


def test_no_module_outside_graphdb_imports_a_backend_package():
    offenders = _offenders()
    assert not offenders, (
        "These modules import a graph DB backend package directly — route the "
        "access through a GraphDBFactory capability instead "
        "(GraphBuilder / GraphHealthProbe / GraphProvisioner / GraphSimulation): "
        f"{offenders}"
    )
