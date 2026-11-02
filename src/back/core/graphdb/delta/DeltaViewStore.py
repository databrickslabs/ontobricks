"""No-copy Delta (Unity Catalog) view store on Databricks SQL Warehouse.

A :class:`VirtualBackend` that reads a raw, read-only Delta relation by its exact
FQN — the ``engine="view"`` store bound to the **Build** SQL warehouse for health
probes and quality / SHACL SQL (never the Lakehouse//RT query warehouse).  It
never materializes its own copy of the triples and exposes no write/build/sync
surface.

It still *reads* the adjacency / entity-search / props companions a build
pipeline may have materialized alongside the live relation — capability flags are
left ``True`` and the search-cache option (``auto`` / ``force_on`` /
``force_off``) governs per request whether they are used, exactly as for the
materialized store.  The SQL read surface is shared via
:class:`back.core.graphdb.delta.DeltaReadStore.DeltaReadStore`.
"""

from __future__ import annotations

from back.core.graphdb.interfaces.VirtualBackend import VirtualBackend
from back.core.graphdb.delta.DeltaReadStore import DeltaReadStore


class DeltaViewStore(DeltaReadStore, VirtualBackend):
    """Read-only, no-copy Delta view store (live-relation pass-through)."""

    # Read-only, but companion tables materialized next to the live relation can
    # still be read; the search-cache option decides per request. Flags mirror
    # the materialized store so the companion/live-SPO choice is unchanged.
    supports_materialized_inference_purge = False
    supports_adjacency = True
    supports_entity_search = True
    supports_props = True
