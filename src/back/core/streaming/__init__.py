"""Incremental triple materialization: row-local mappings compiled to Lakeflow SDP SQL.

:class:`SlotProjectionCompiler` turns row-local R2RML mappings into slot-event
views and AUTO CDC flows that keep a ``_slots`` streaming table in sync with
the source tables' change feed; :class:`SlotPipelineSpec` is its result.
:class:`SlotPipelineRunner` creates and runs that pipeline (:class:`SlotUpdateMetrics`
sums the rows an update wrote),
:class:`SourceReadiness` probes whether the sources can feed it, and
:class:`SlotReconciler` checks the slots against the ``_data`` relation.
In ``realtime`` mode the views read the Debezium topic instead
(:class:`TopicSlotSource` over a :class:`TopicSource`), typing its values
with :class:`DebeziumValueText`.
For the Real-Time-Mode stream, :class:`SlotRowProjector` evaluates the same
projection in Python, :class:`SlotDiffProcessor` diffs a source row's slots
against its state and emits :class:`SlotChange` upserts and tombstones.
"""

from back.core.streaming.DebeziumValueText import DebeziumValueText  # noqa: F401
from back.core.streaming.SlotChange import SlotChange  # noqa: F401
from back.core.streaming.SlotDiffProcessor import SlotDiffProcessor  # noqa: F401
from back.core.streaming.SlotPipelineRunner import SlotPipelineRunner  # noqa: F401
from back.core.streaming.SlotPipelineSpec import SlotPipelineSpec  # noqa: F401
from back.core.streaming.SlotProjectionCompiler import (  # noqa: F401
    SlotProjectionCompiler,
)
from back.core.streaming.SlotReconciler import SlotReconciler  # noqa: F401
from back.core.streaming.SlotRowProjector import SlotRowProjector  # noqa: F401
from back.core.streaming.SlotUpdateMetrics import SlotUpdateMetrics  # noqa: F401
from back.core.streaming.SourceReadiness import SourceReadiness  # noqa: F401
from back.core.streaming.TopicSlotSource import TopicSlotSource  # noqa: F401
from back.core.streaming.TopicSource import TopicSource  # noqa: F401

__all__ = [
    "DebeziumValueText",
    "SlotChange",
    "SlotDiffProcessor",
    "SlotPipelineRunner",
    "SlotPipelineSpec",
    "SlotProjectionCompiler",
    "SlotReconciler",
    "SlotRowProjector",
    "SlotUpdateMetrics",
    "SourceReadiness",
    "TopicSlotSource",
    "TopicSource",
]
