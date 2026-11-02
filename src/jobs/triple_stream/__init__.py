"""Real-Time-Mode triple stream: source-row change events to the Lakebase ``_live`` table.

:mod:`jobs.triple_stream.processor` holds the ``transformWithState`` adapter
of :class:`back.core.streaming.SlotDiffProcessor`; the job entry point that
reads the change events and writes the slot changes lives next to it.
"""
