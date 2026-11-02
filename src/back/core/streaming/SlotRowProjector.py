"""Pure-Python projection of one row-local mapping: a source row to its slots.

The Real-Time-Mode stream applies source-row changes outside Lakeflow SDP, so
it cannot run the slot-event SQL of
:meth:`SlotProjectionCompiler.slot_event_view_sql`. This class evaluates the
same projection in Python, from the same mapping dict and through the
compiler's own projection facts (``map_id``, ``key_columns``,
``subject_parts``, ``slot_sources``), so the two cannot drift:

* the source key is ``CAST(<key> AS STRING)`` (``concat_ws('|', ...)`` for a
  composite template), and a row with a NULL key column has no slots
  (``WHERE <key> IS NOT NULL``);
* the subject is the template prefix followed by ``TRY_CAST(<id> AS STRING)``;
* an object is the class literal for ``rdf:type``, else
  ``COALESCE(TRY_CAST(<column> AS STRING), '')`` — never ``None``, an unset
  attribute is ``''``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from back.core.errors import ValidationError
from back.core.streaming.SlotProjectionCompiler import SlotProjectionCompiler


@dataclass(frozen=True)
class SlotRowProjector:
    """Project the rows of one row-local mapping into ``predicate -> object`` slots."""

    map_id: str
    source_table: str
    key_columns: tuple[str, ...]
    subject_prefix: str
    id_column: str
    #: ``(predicate, literal, column)`` per slot, in ``stack()`` order.
    sources: tuple[tuple[str, str | None, str | None], ...]

    @classmethod
    def from_mapping(cls, mapping: dict, *, base_uri: str = "") -> SlotRowProjector:
        """Build the projector of a row-local entity *mapping* (with its ``class_uri``).

        *base_uri* is accepted for symmetry with the compiler and unused:
        subject templates carry their own prefix. Raises
        :class:`ValidationError` for a mapping that is not row-local.
        """
        compiler = SlotProjectionCompiler
        if compiler.classify(mapping) != compiler.ROW_LOCAL:
            raise ValidationError(
                f"Mapping {mapping.get('class_uri') or mapping.get('table')!r} is not row-local"
            )
        prefix, id_column = compiler.subject_parts(mapping)
        return cls(
            map_id=compiler.map_id(mapping),
            source_table=mapping["table"],
            key_columns=tuple(compiler.key_columns(mapping)),
            subject_prefix=prefix,
            id_column=id_column,
            sources=tuple(compiler.slot_sources(mapping)),
        )

    @staticmethod
    def for_mappings(
        entity_mappings: dict, relationship_mappings: list, *, base_uri: str = ""
    ) -> list[SlotRowProjector]:
        """One projector per mapping :meth:`SlotProjectionCompiler.compile` compiles.

        Relationship and other batch mappings are excluded, a TriplesMap with
        several classes gives one projector per class, in the compiler's order.
        """
        return [
            SlotRowProjector.from_mapping(mapping, base_uri=base_uri)
            for mapping in SlotProjectionCompiler.row_local_mappings(
                entity_mappings, relationship_mappings
            )
        ]

    @property
    def predicates(self) -> tuple[str, ...]:
        """Every predicate a row of this mapping has a slot for, in ``stack()`` order."""
        return tuple(predicate for predicate, _literal, _column in self.sources)

    def source_key(self, row: Mapping) -> str | None:
        """The slot key of *row*; ``None`` when a key column is NULL (the row is ignored)."""
        values = [row.get(column) for column in self.key_columns]
        if any(value is None for value in values):
            return None
        return "|".join(self.spark_string(value) for value in values)

    def subject(self, row: Mapping) -> str | None:
        """The subject URI of *row*; ``None`` when its id column is NULL."""
        text = self.spark_string(row.get(self.id_column))
        return None if text is None else self.subject_prefix + text

    def slots(self, row: Mapping) -> dict[str, str]:
        """``predicate -> object`` for every slot of *row*; an unset attribute is ``''``."""
        return {
            predicate: literal if column is None else (self.spark_string(row.get(column)) or "")
            for predicate, literal, column in self.sources
        }

    @staticmethod
    def spark_string(value) -> str | None:
        """``CAST(<value> AS STRING)`` as Spark renders it: booleans are ``true`` / ``false``."""
        if value is None or isinstance(value, str):
            return value
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)
