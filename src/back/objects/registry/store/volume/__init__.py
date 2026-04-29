"""Volume-backed :class:`RegistryStore` implementation.

Stores every piece of registry-shaped data as a JSON file underneath
``/Volumes/<catalog>/<schema>/<volume>`` on Unity Catalog. This is the
original — and default — OntoBricks layout. See
:mod:`back.objects.registry.store.volume.store` for the full
contract implementation and the on-Volume file layout.
"""

from __future__ import annotations

from .store import VolumeRegistryStore

__all__ = ["VolumeRegistryStore"]
