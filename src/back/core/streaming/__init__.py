"""Incremental triple materialization: row-local mappings compiled to Lakeflow SDP SQL.

:class:`SlotProjectionCompiler` turns row-local R2RML mappings into slot-event
views and AUTO CDC flows that keep a ``_slots`` streaming table in sync with
the source tables' change feed; :class:`SlotPipelineSpec` is its result.
:class:`SlotPipelineRunner` creates and runs that pipeline,
:class:`SourceReadiness` probes whether the sources can feed it, and
:class:`SlotReconciler` checks the slots against the ``_data`` relation.
"""

from back.core.streaming.SlotPipelineRunner import SlotPipelineRunner  # noqa: F401
from back.core.streaming.SlotPipelineSpec import SlotPipelineSpec  # noqa: F401
from back.core.streaming.SlotProjectionCompiler import (  # noqa: F401
    SlotProjectionCompiler,
)
from back.core.streaming.SlotReconciler import SlotReconciler  # noqa: F401
from back.core.streaming.SourceReadiness import SourceReadiness  # noqa: F401

__all__ = [
    "SlotPipelineRunner",
    "SlotPipelineSpec",
    "SlotProjectionCompiler",
    "SlotReconciler",
    "SourceReadiness",
]
