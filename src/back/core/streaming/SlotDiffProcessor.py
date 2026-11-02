"""Stateful diff of one source row's slot set, the core of the Real-Time-Mode stream.

Each source-row change (an upsert with the row's after-image, or a delete)
is projected through the row's :class:`SlotRowProjector` and compared with
the slots the row had: changed or new slots become upserts, slots that
vanished or went blank become tombstones. It is the pure, Spark-free logic of
the ``transformWithState`` processor in ``jobs/triple_stream``, keyed by
``(map_id, source_key)``.

State per key is ``{predicate: (subject, object, seq)}``. A slot that was
removed stays in the state as ``(None, None, seq)``: that watermark is what
makes a late or replayed event (``seq`` not above the highest one in the
state) a no-op even after the row was deleted, instead of resurrecting it.

A key with no state at all — never seen, or evicted by the state TTL — is
*unknown*: any slot of it may still be live in ``_live``. Its first event then
emits an upsert for every non-blank slot and a tombstone for every other
predicate the mapping can produce (a delete tombstones them all); the sink's
``seq`` guard makes the extra tombstones harmless. Two consequences for the
caller that configures the TTL:

* the TTL must exceed the maximum event lateness — after an eviction a late
  event is applied again, and only the sink's ``seq`` guard stands between
  it and ``_live``;
* without a TTL (the default) the state keeps one watermark per predicate of
  every key it ever saw, deleted rows included: it only grows.

A blank object (``None`` or only spaces, the full build's
``TRIM(object) != ''`` filter) is never a slot: the AUTO CDC ``_slots`` table
keeps such rows, the ``_data`` relation and this diff do not.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from back.core.errors import ValidationError
from back.core.streaming.SlotChange import SlotChange
from back.core.streaming.SlotRowProjector import SlotRowProjector

#: ``{predicate: (subject, object, seq)}``; ``(None, None, seq)`` is a removed slot.
SlotState = dict[str, tuple[str | None, str | None, int]]


class SlotDiffProcessor:
    """Diff a source row's new slots against its state; emit the slot changes."""

    UPSERT = "upsert"
    DELETE = "delete"
    #: ``seq`` of a slot loaded from the ``_slots`` snapshot, which has none: any event is newer.
    SNAPSHOT_SEQ = -1

    def __init__(self, projectors: list[SlotRowProjector]) -> None:
        self._projectors = {projector.map_id: projector for projector in projectors}

    def initial_state(self, slots_rows: Iterable[Mapping]) -> dict[tuple[str, str], SlotState]:
        """Per-key state from ``_slots`` rows (``map_id, source_key, predicate, subject, object``).

        A row may carry ``seq``; without it the slot counts as older than any
        event (:attr:`SNAPSHOT_SEQ`). Blank-object rows are not slots.
        """
        state: dict[tuple[str, str], SlotState] = {}
        for row in slots_rows:
            obj = row.get("object")
            if self.is_blank(obj):
                continue
            seq = row.get("seq")
            key = (row["map_id"], row["source_key"])
            state.setdefault(key, {})[row["predicate"]] = (
                row.get("subject"),
                obj,
                self.SNAPSHOT_SEQ if seq is None else int(seq),
            )
        return state

    def apply(
        self,
        state: SlotState | None,
        *,
        map_id: str,
        source_key: str,
        op: str,
        row: Mapping | None,
        seq: int,
    ) -> tuple[SlotState, list[SlotChange]]:
        """Apply one change of the source row ``(map_id, source_key)``: ``(new state, changes)``.

        *op* is ``"upsert"`` (*row* is the after-image, string-valued: see
        :class:`SlotRowProjector`) or ``"delete"``. A *seq* not above the
        highest one in *state* returns *state* unchanged and no change. An
        upsert whose row has a NULL key is ignored, as the slot-event view's
        ``WHERE <key> IS NOT NULL`` does. An empty or ``None`` *state* is
        unknown (see the module docstring). Changes are sorted by predicate.
        """
        current = state or {}
        if current and seq <= max(entry[2] for entry in current.values()):
            return current, []
        if op == self.UPSERT:
            if row is None:
                raise ValidationError(f"An upsert of {map_id}/{source_key} needs a row")
            projector = self._projector(map_id)
            key, subject, slots = projector.project(row)
            if key is None:
                return current, []
            live = {
                predicate: (subject, obj)
                for predicate, obj in slots.items()
                if not self.is_blank(obj)
            }
            known = projector.predicates()
        elif op == self.DELETE:
            live = {}
            projector = self._projectors.get(map_id)
            known = projector.predicates() if projector else ()
        else:
            raise ValidationError(f"Unknown slot change op {op!r} (expected upsert or delete)")
        return self._diff(current, live, known, map_id=map_id, source_key=source_key, seq=seq)

    @staticmethod
    def is_blank(obj: str | None) -> bool:
        """``None`` or only spaces: ``TRIM(object) = ''`` in Spark and Postgres."""
        return obj is None or obj.strip(" ") == ""

    def _projector(self, map_id: str) -> SlotRowProjector:
        try:
            return self._projectors[map_id]
        except KeyError:
            raise ValidationError(f"No row-local mapping {map_id!r} in this stream") from None

    @staticmethod
    def _diff(
        current: SlotState,
        live: dict[str, tuple[str | None, str]],
        known: Iterable[str],
        *,
        map_id: str,
        source_key: str,
        seq: int,
    ) -> tuple[SlotState, list[SlotChange]]:
        """Compare *live* slots with *current*; removed or unseen predicates keep a watermark.

        With no *current* state every predicate may be live in ``_live``: each
        non-live one is tombstoned.
        """
        unknown = not current
        new_state: SlotState = {}
        changes: list[SlotChange] = []
        for predicate in sorted(set(current) | set(live) | set(known)):
            old = current.get(predicate)
            had_slot = old is not None and old[1] is not None
            if predicate in live:
                subject, obj = live[predicate]
                new_state[predicate] = (subject, obj, seq)
                if not had_slot or (old[0], old[1]) != (subject, obj):
                    changes.append(
                        SlotChange.upsert(
                            map_id=map_id,
                            source_key=source_key,
                            predicate=predicate,
                            subject=subject,
                            object=obj,
                            seq=seq,
                        )
                    )
                continue
            new_state[predicate] = (None, None, seq)
            if had_slot or unknown:
                changes.append(
                    SlotChange.tombstone(
                        map_id=map_id, source_key=source_key, predicate=predicate, seq=seq
                    )
                )
        return new_state, changes
