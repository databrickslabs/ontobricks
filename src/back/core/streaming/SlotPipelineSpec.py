"""Compiled Lakeflow SDP statements for the row-local slot pipeline."""

from dataclasses import dataclass, field


@dataclass
class SlotPipelineSpec:
    """Output of :meth:`SlotProjectionCompiler.compile`.

    ``statements`` is the ordered SQL (slots table, then a view and an AUTO CDC
    flow per row-local mapping). ``batch_mappings`` holds the mappings that are
    not row-local (joins, ``sql_query`` sources): the original mapping dicts,
    each listed once, so they can be matched by identity.
    """

    slots_fqn: str
    statements: list[str] = field(default_factory=list)
    row_local_map_ids: list[str] = field(default_factory=list)
    batch_mappings: list[dict] = field(default_factory=list)
    source_tables: list[str] = field(default_factory=list)

    def to_sql(self) -> str:
        """Return the statements joined with ``;`` and a blank line."""
        return ";\n\n".join(self.statements)

    def mv_source_count(self) -> int | None:
        """Relations the ``_data`` MV reads: ``…_slots`` and the batch mappings' tables.

        ``…_slots`` counts when a mapping is row-local. ``None`` when a batch
        mapping reads a ``sql_query`` or joins another table (a non-column
        predicate): its sources are not known.
        """
        tables = {self.slots_fqn} if self.row_local_map_ids else set()
        for mapping in self.batch_mappings:
            table = (mapping.get("table") or "").strip()
            joins = any(
                not isinstance(info, dict) or info.get("type") != "column"
                for info in (mapping.get("predicates") or {}).values()
            )
            if not table or (mapping.get("sql_query") or "").strip() or joins:
                return None
            tables.add(table)
        return len(tables)
