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

Columns are looked up case-insensitively, as Spark resolves them. Row values
must be strings (or ``None``): the stream casts every source column to STRING
in Spark, from the table schema, before it serialises the row, because
Python's rendering of floats, decimals, dates or nested values differs from
Spark's ``CAST(... AS STRING)``. Integers and booleans are accepted (they
render alike); any other type raises :class:`ValidationError`.
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

    def predicates(self) -> tuple[str, ...]:
        """Every predicate a row of this mapping can have a slot for, in ``stack()`` order.

        ``rdf:type``, ``rdfs:label`` when the mapping has a label column, then
        the attribute predicates.
        """
        return tuple(predicate for predicate, _literal, _column in self.sources)

    def project(self, row: Mapping) -> tuple[str | None, str | None, dict[str, str]]:
        """``(source_key, subject, slots)`` of *row* in one pass (one lower-cased copy)."""
        values = self._lowered(row)
        return self._key(values), self._subject(values), self._slots(values)

    def source_key(self, row: Mapping) -> str | None:
        """The slot key of *row*; ``None`` when a key column is NULL (the row is ignored)."""
        return self._key(self._lowered(row))

    def subject(self, row: Mapping) -> str | None:
        """The subject URI of *row*; ``None`` when its id column is NULL."""
        return self._subject(self._lowered(row))

    def slots(self, row: Mapping) -> dict[str, str]:
        """``predicate -> object`` for every slot of *row*; an unset attribute is ``''``."""
        return self._slots(self._lowered(row))

    @staticmethod
    def spark_string(value, column: str = "") -> str | None:
        """``CAST(<value> AS STRING)`` for the values whose Spark rendering Python matches.

        Strings pass through, ``None`` stays ``None``, integers render alike
        and booleans are ``true`` / ``false``. Anything else (floats, decimals,
        dates, nested values) raises :class:`ValidationError` naming
        *column*: the stream must cast the row to strings in Spark, from the
        table schema, before it serialises it.
        """
        if value is None or isinstance(value, str):
            return value
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, int):
            return str(value)
        raise ValidationError(
            f"Column {column!r} holds a {type(value).__name__} value; the realtime stream "
            "expects every source column cast to STRING in Spark before serialisation"
        )

    # ------------------------------------------------------------------
    # Projection on a lower-cased row
    # ------------------------------------------------------------------

    @staticmethod
    def _lowered(row: Mapping) -> dict:
        """*row* keyed by lower-cased column names: Spark resolves columns case-insensitively."""
        return {str(column).lower(): value for column, value in row.items()}

    def _value(self, values: dict, column: str) -> str | None:
        return self.spark_string(values.get(column.lower()), column)

    def _key(self, values: dict) -> str | None:
        parts = [self._value(values, column) for column in self.key_columns]
        if any(part is None for part in parts):
            return None
        return "|".join(parts)

    def _subject(self, values: dict) -> str | None:
        text = self._value(values, self.id_column)
        return None if text is None else self.subject_prefix + text

    def _slots(self, values: dict) -> dict[str, str]:
        return {
            predicate: literal if column is None else (self._value(values, column) or "")
            for predicate, literal, column in self.sources
        }
