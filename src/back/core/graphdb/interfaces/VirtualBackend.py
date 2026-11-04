"""Hat interface for virtual (no-copy) graph DB backends.

A *virtual* backend is a read-only pass-through over a live source — e.g. a
Databricks Delta/Unity Catalog ``view`` bound to the Build SQL warehouse for
health probes and quality SQL.  It never materializes its own copy of the
triples, so it exposes **no write, build, or remote-sync surface**: the abstract
write contract (``create_table`` / ``drop_table`` / ``insert_triples``) lives on
:class:`back.core.graphdb.interfaces.MaterializedGraphBackend.MaterializedGraphBackend`
only, and virtual engines never implement it.

What a virtual backend *may* still do is **read** pre-built companion tables
(adjacency / entity-search / props) that a build pipeline materialized alongside
the live view.  Whether those companions are consulted is governed per request
by the existing search-cache option (``auto`` / ``force_on`` / ``force_off`` —
see :mod:`back.core.graphdb.search_cache`), exactly as for a materialized
engine.  So the companion capability flags are left to the concrete engine (a
virtual engine sets ``supports_*`` as appropriate); this hat does not force them
off.

The read contract (query / reasoning-read / stats) is inherited unchanged from
:class:`back.core.graphdb.interfaces.GraphBackend.GraphBackend`.
"""

from back.core.graphdb.interfaces.GraphBackend import GraphBackend


class VirtualBackend(GraphBackend):
    """Read-only, no-copy graph backend (live-source pass-through).

    The distinguishing characteristic versus
    :class:`~back.core.graphdb.interfaces.MaterializedGraphBackend.MaterializedGraphBackend`
    is the absence of a write/build/sync surface — not the absence of companion
    reads.  Concrete virtual engines set the ``supports_*`` capability flags
    (inherited default ``False``) according to which companions they can read.
    """
