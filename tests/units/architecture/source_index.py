"""AST index of ``src/`` shared by the architecture guardrail tests."""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import Iterator, List, Tuple

SRC_ROOT = Path(__file__).resolve().parents[3] / "src"

_SKIPPED_DIRS = {"__pycache__", ".venv", "node_modules"}


@dataclass(frozen=True)
class ClassInfo:
    name: str
    lineno: int
    size: int


@dataclass(frozen=True)
class FunctionInfo:
    qualname: str
    lineno: int
    size: int


@dataclass
class ModuleInfo:
    rel_path: str
    tree: ast.Module
    classes: List[ClassInfo] = field(default_factory=list)

    @property
    def stem(self) -> str:
        return Path(self.rel_path).stem

    @property
    def public_classes(self) -> List[ClassInfo]:
        return [c for c in self.classes if not c.name.startswith("_")]

    def iter_functions(self) -> Iterator[FunctionInfo]:
        """Yield module-level functions and methods of top-level classes."""
        for node in self.tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                yield FunctionInfo(node.name, node.lineno, _size(node))
            elif isinstance(node, ast.ClassDef):
                for member in node.body:
                    if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        yield FunctionInfo(
                            f"{node.name}.{member.name}", member.lineno, _size(member)
                        )

    def duplicate_definitions(self) -> List[Tuple[str, int]]:
        """Return ``(qualname, lineno)`` of functions silently redefined in the same scope.

        Decorated definitions are ignored: ``@property`` / ``@x.setter`` and
        ``@overload`` legitimately reuse a name.
        """
        duplicates: List[Tuple[str, int]] = []
        scopes = [("", self.tree.body)] + [
            (f"{n.name}.", n.body) for n in self.tree.body if isinstance(n, ast.ClassDef)
        ]
        for prefix, body in scopes:
            seen: set = set()
            for node in body:
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if node.decorator_list:
                    continue
                if node.name in seen:
                    duplicates.append((f"{prefix}{node.name}", node.lineno))
                seen.add(node.name)
        return duplicates


def _size(node: ast.AST) -> int:
    return node.end_lineno - node.lineno + 1


class SourceIndex:
    """Parsed view of every Python module under ``src/``."""

    def __init__(self, root: Path = SRC_ROOT) -> None:
        self._root = root

    @cached_property
    def modules(self) -> List[ModuleInfo]:
        modules: List[ModuleInfo] = []
        for path in sorted(self._root.rglob("*.py")):
            if _SKIPPED_DIRS.intersection(path.parts):
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            info = ModuleInfo(rel_path=path.relative_to(self._root).as_posix(), tree=tree)
            info.classes = [
                ClassInfo(n.name, n.lineno, _size(n))
                for n in tree.body
                if isinstance(n, ast.ClassDef)
            ]
            modules.append(info)
        return modules

    def under(self, *prefixes: str) -> List[ModuleInfo]:
        return [m for m in self.modules if m.rel_path.startswith(prefixes)]


SOURCE_INDEX = SourceIndex()
