"""Incremental triple materialization: row-local mappings compiled to Lakeflow SDP SQL.

:class:`SlotProjectionCompiler` turns row-local R2RML mappings into slot-event
views and AUTO CDC flows that keep a ``_slots`` streaming table in sync with
the source tables' change feed; :class:`SlotPipelineSpec` is its result.
"""

from back.core.streaming.SlotPipelineSpec import SlotPipelineSpec  # noqa: F401
from back.core.streaming.SlotProjectionCompiler import (  # noqa: F401
    SlotProjectionCompiler,
)

__all__ = ["SlotPipelineSpec", "SlotProjectionCompiler"]
