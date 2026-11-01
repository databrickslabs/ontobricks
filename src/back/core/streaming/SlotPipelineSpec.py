"""Compiled Lakeflow SDP statements for the row-local slot pipeline."""

from dataclasses import dataclass, field


@dataclass
class SlotPipelineSpec:
    """Output of :meth:`SlotProjectionCompiler.compile`.

    ``statements`` is the ordered SQL (slots table, then a view and an AUTO CDC
    flow per row-local mapping). ``batch_mappings`` holds the mappings that are
    not row-local (joins, ``sql_query`` sources); entity entries are copies
    carrying their ``class_uri``, relationship entries are passed through.
    """

    slots_fqn: str
    statements: list[str] = field(default_factory=list)
    row_local_map_ids: list[str] = field(default_factory=list)
    batch_mappings: list[dict] = field(default_factory=list)
    source_tables: list[str] = field(default_factory=list)

    def to_sql(self) -> str:
        """Return the statements joined with ``;`` and a blank line."""
        return ";\n\n".join(self.statements)
