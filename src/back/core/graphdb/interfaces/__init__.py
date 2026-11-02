"""Graph DB backend contract — the interfaces every engine implements.

This package is the single source of truth for *what a backend is*. It holds
only abstract classes; concrete engines live under ``back/core/graphdb/<engine>/``
and subclass these.

* Hats (over the shared read root :class:`GraphBackend`):
  :class:`MaterializedGraphBackend` — copy-owning engines with the full
  write / companion / sync surface; :class:`VirtualBackend` — read-only,
  no-copy pass-through engines.
* Capabilities, vended by :class:`~back.core.graphdb.GraphDBFactory`:
  :class:`GraphBuilder`, :class:`GraphHealthProbe`, :class:`GraphProvisioner`,
  :class:`GraphSimulation`.

See the Engine integration section of ``docs/backend.md`` for the full guide
to adding a backend.
"""

from back.core.graphdb.interfaces.GraphBackend import GraphBackend  # noqa: F401
from back.core.graphdb.interfaces.MaterializedGraphBackend import (  # noqa: F401
    MaterializedGraphBackend,
)
from back.core.graphdb.interfaces.VirtualBackend import VirtualBackend  # noqa: F401
from back.core.graphdb.interfaces.GraphBuilder import GraphBuilder  # noqa: F401
from back.core.graphdb.interfaces.GraphHealthProbe import (  # noqa: F401
    GraphHealthProbe,
)
from back.core.graphdb.interfaces.GraphProvisioner import (  # noqa: F401
    GraphProvisioner,
)
from back.core.graphdb.interfaces.GraphSimulation import (  # noqa: F401
    GraphSimulation,
)

__all__ = [
    "GraphBackend",
    "MaterializedGraphBackend",
    "VirtualBackend",
    "GraphBuilder",
    "GraphHealthProbe",
    "GraphProvisioner",
    "GraphSimulation",
]
