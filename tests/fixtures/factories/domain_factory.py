"""DomainFactory — builds domain JSON for registry/domain-state tests.

Mirrors `back.objects.domain.Domain` and `back.objects.registry.RegistryService`
shape.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DomainFactory:
    """Build domain config dicts.

    Args:
        name: Domain slug.
        display_name: Human-readable name.
        versions: Number of versions to include (each gets a placeholder ontology + mapping).
        with_layout: Include a non-empty design_layout block.
        seed: Deterministic seed.
    """

    name: str = "test_domain"
    display_name: str = "Test Domain"
    versions: int = 1
    with_layout: bool = False
    seed: int = 0

    @classmethod
    def build(cls, **overrides: Any) -> dict[str, Any]:
        return cls(**overrides)._build()

    def _build(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "display_name": self.display_name,
            "description": f"{self.display_name} — built by DomainFactory",
            "active": True,
            "current_version": f"v{self.versions}",
            "versions": [self._version(i + 1) for i in range(self.versions)],
            "design_layout": self._layout() if self.with_layout else {},
            "metadata": {"created_by": "test", "tags": ["test", "factory"]},
        }

    def _version(self, n: int) -> dict[str, Any]:
        return {
            "version": f"v{n}",
            "ontology": {
                "name": f"{self.name}_ontology_v{n}",
                "base_uri": f"http://test.org/{self.name}/v{n}#",
                "classes": [],
                "properties": [],
            },
            "mapping": {"entities": [], "relationships": []},
            "assignment": {"icons": {}, "positions": {}},
        }

    def _layout(self) -> dict[str, Any]:
        return {
            "nodes": [
                {"id": "Customer", "x": 100, "y": 100, "icon": "👤"},
                {"id": "Order", "x": 300, "y": 100, "icon": "📦"},
            ],
            "edges": [{"source": "Customer", "target": "Order", "label": "hasOrder"}],
            "view": {"zoom": 1.0, "pan": {"x": 0, "y": 0}},
        }
