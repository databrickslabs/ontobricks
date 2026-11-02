"""Where a domain's slot pipeline reads its source-row changes: the change feed or the topic."""

from __future__ import annotations

from typing import Any

from back.objects.digitaltwin.build.TwinBuildPostMaterialize import TwinBuildPostMaterialize


class TwinSlotFeed:
    """The slot pipeline's feed: the sources' change data feed, or a ``realtime`` domain's topic.

    One decision for every compile of the domain (:meth:`TwinIncrementalBuild.compile_for`:
    the build, the scheduled tick, the MERGE fingerprint, the readiness
    preflight, the DAB renderer), so all of them compile the same SQL.
    """

    @staticmethod
    def compile_kwargs(domain: Any, settings: Any, row_local: list, client: Any) -> dict:
        """``source`` / ``topic`` / ``column_types`` of ``SlotProjectionCompiler.compile``.

        *row_local* are the row-local mappings (``SlotProjectionCompiler.row_local_mappings``).
        A topic-fed pipeline types the Debezium values with their tables'
        column types, read on *client* (:meth:`column_types`); without a client
        (the DAB renderer, which renders no SQL) nothing is typed.
        """
        from back.core.streaming.SlotPipelineSpec import SOURCE_CDF, SOURCE_TOPIC

        topic = TwinSlotFeed.topic(domain, settings)
        if topic is None:
            return {"source": SOURCE_CDF, "topic": None, "column_types": None}
        types = None
        if client is not None:
            types = TwinSlotFeed.column_types(client, {m["table"] for m in row_local if m.get("table")})
        return {"source": SOURCE_TOPIC, "topic": topic, "column_types": types}

    @staticmethod
    def topic(domain: Any, settings: Any):
        """The topic a ``realtime`` domain's slot pipeline reads; ``None``: the change data feed.

        A domain whose best mode is ``realtime`` (its topic available,
        serverless, a Lakebase backend: :meth:`TwinBuildPostMaterialize.gate_decision`)
        reads the Debezium topic its stream job reads, so ``…_slots`` / ``…_data``
        and ``_live`` follow the same changes — except a Kinesis stream, which
        ``read_kafka`` cannot read: that slot pipeline keeps the change feed.
        """
        from back.core.graphdb import RealtimeSource
        from back.core.streaming import TopicSource

        if TwinBuildPostMaterialize.gate_decision(domain, settings).effective != "realtime":
            return None
        config = RealtimeSource.of(domain)
        if config.get("kind") == "kinesis":
            return None
        return TopicSource.from_realtime_source(config)

    @staticmethod
    def column_types(client: Any, tables: Any) -> dict:
        """``{table: {column: type}}`` of *tables* from ``DESCRIBE TABLE``.

        The type strings ``spark.table(t).schema`` gives the stream job
        (``int``, ``decimal(10,2)``, ``timestamp``…); the column list ends at
        the first ``#`` section. A table that cannot be described raises
        :class:`InfrastructureError`: compiling without its types would change
        the SQL fingerprint, i.e. replay the topic.
        """
        from back.core.helpers import WarehouseStatements, validate_table_name

        types: dict = {}
        for table in sorted(tables):
            validate_table_name(table)
            rows = WarehouseStatements.query(client, f"DESCRIBE TABLE {table}", f"describe {table}")
            columns: dict = {}
            for row in rows:
                name = str((row or {}).get("col_name") or "").strip()
                if not name or name.startswith("#"):
                    break
                columns[name.lower()] = str(row.get("data_type") or "").strip().lower()
            types[table] = columns
        return types

    @staticmethod
    def feed_tables(spec: Any) -> list:
        """The sources whose change data feed the slot pipeline reads; none for a topic-fed one.

        Their row tracking is probed (build, tick, readiness, the "enable row
        tracking" action) and their history checked for rewrites
        (:class:`TwinSlotRefreshPlan`); a topic-fed pipeline needs neither (a
        full refresh replays the topic).
        """
        return list(spec.source_tables) if spec.needs_row_tracking else []
