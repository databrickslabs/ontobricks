"""Every public class in ``back/objects/<subpackage>/`` is re-exported (``.coding_rules §7``)."""

from __future__ import annotations

import ast
from pathlib import PurePosixPath

import pytest

from tests.units.architecture.source_index import SOURCE_INDEX

pytestmark = pytest.mark.unit


def _exported_names(init: ast.Module) -> set:
    names = set()
    for node in ast.walk(init):
        if isinstance(node, ast.ImportFrom):
            names.update(alias.asname or alias.name for alias in node.names)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            names.add(node.value)
    return names


def test_public_classes_are_reexported_by_their_package():
    inits = {
        str(PurePosixPath(m.rel_path).parent): m.tree
        for m in SOURCE_INDEX.under("back/objects/")
        if m.stem == "__init__"
    }
    missing = []
    for module in SOURCE_INDEX.under("back/objects/"):
        package = str(PurePosixPath(module.rel_path).parent)
        if module.stem == "__init__" or package not in inits or package == "back/objects":
            continue
        exported = _exported_names(inits[package])
        missing += [
            f"{module.rel_path}::{c.name}" for c in module.public_classes if c.name not in exported
        ]
    assert not missing, f"Add these classes to their package __init__.py: {missing}"
