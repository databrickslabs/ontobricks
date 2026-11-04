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

# The materialization orchestration is engine-aware by design: it drives the
# engines' *mechanics* — Lakeflow slot pipelines, Delta materialized views and
# change feeds, Lakebase synced tables and companion DDL — so it may import the
# backend packages' infrastructure modules. The concrete stores stay behind the
# interfaces there too (``test_orchestration_imports_no_concrete_store``).
_ORCHESTRATION_PREFIXES = (
    "back/objects/digitaltwin/build/",
    "back/objects/registry/schedule/",
)
_STORE_MODULES = (
    "back.core.graphdb.delta.ReadStore",
    "back.core.graphdb.delta.MaterializedStore",
    "back.core.graphdb.delta.ViewStore",
    "back.core.graphdb.delta.BuildPipeline",
    "back.core.graphdb.lakebase.FlatStore",
    "back.core.graphdb.lakebase.Base",
    "back.core.graphdb.neo4j.Store",
)
_STORE_NAMES = tuple(name.rsplit(".", 1)[1] for name in _STORE_MODULES)


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
        if module.rel_path.startswith((_ALLOWED_PREFIX, *_ORCHESTRATION_PREFIXES)):
            continue
        for lineno in _backend_imports(module.tree):
            found.append((module.rel_path, lineno))
    return sorted(found)


def _store_imports(tree: ast.Module) -> Iterator[int]:
    """Yield line numbers of imports of a concrete store / backend-root class."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or node.level:
            continue
        if node.module in _STORE_MODULES:
            yield node.lineno
        elif _targets_backend(node.module) and any(
            alias.name in _STORE_NAMES for alias in node.names
        ):
            yield node.lineno


def _store_offenders() -> List[Tuple[str, int]]:
    found: List[Tuple[str, int]] = []
    for module in SOURCE_INDEX.modules:
        if not module.rel_path.startswith(_ORCHESTRATION_PREFIXES):
            continue
        for lineno in _store_imports(module.tree):
            found.append((module.rel_path, lineno))
    return sorted(found)


def test_orchestration_imports_no_concrete_store():
    offenders = _store_offenders()
    assert not offenders, (
        "The materialization orchestration may use the engines' mechanics but "
        "not their stores — ask GraphDBFactory (get_graphdb, get_builder, "
        f"classify_store) or the interfaces instead: {offenders}"
    )


def test_no_module_outside_graphdb_imports_a_backend_package():
    offenders = _offenders()
    assert not offenders, (
        "These modules import a graph DB backend package directly — route the "
        "access through a GraphDBFactory capability instead "
        "(GraphBuilder / GraphHealthProbe / GraphProvisioner / GraphSimulation): "
        f"{offenders}"
    )
