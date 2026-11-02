"""Property: folding the slot events of any change history gives the full build's triples.

A pure-Python model of what the slot pipeline does with a source's change data
feed — the event projection of ``SlotProjectionCompiler.slot_event_view_sql``
and the AUTO CDC SCD TYPE 1 fold of ``auto_cdc_flow_sql`` — checked against
the final state of the source table:

1. the folded slots equal the slots of the final rows (one per
   ``(map_id, source_key, predicate)``, blank objects kept as upserts);
2. their distinct non-blank triples equal what a full build projects from the
   final table (``NON_BLANK_OBJECT_SQL``).

The model reads the mapping's ``stack()`` pairs from the compiler itself and
asserts the rules it encodes against the emitted SQL, so it cannot drift from
the pipeline unnoticed.
"""

from __future__ import annotations

import re

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from back.core.helpers.SQLHelpers import NON_BLANK_OBJECT_SQL
from back.core.streaming import SlotProjectionCompiler

pytestmark = [pytest.mark.unit, pytest.mark.property]

_MAPPING = {
    "table": "main.sales.customers",
    "sql_query": None,
    "uri_template": "http://ex.org/Customer/{id}",
    "id_column": "id",
    "label_column": "name",
    "class_uri": "http://ex.org/Customer",
    "predicates": {
        "http://www.w3.org/2000/01/rdf-schema#label": {"type": "column", "column": "name"},
        "http://ex.org/age": {"type": "column", "column": "age"},
    },
}
_MAP_ID = SlotProjectionCompiler.map_id(_MAPPING)
_SUBJECT_PREFIX = "http://ex.org/Customer/"
_LITERAL = re.compile(r"\A'(?P<value>[^']*)'\Z")
_COLUMN = re.compile(r"\ACOALESCE\(TRY_CAST\((?P<column>\w+) AS STRING\), ''\)\Z")
_DELETES = ("delete", "update_preimage")


def _projection():
    """``(predicate, row -> object)`` per ``stack()`` pair the compiler emits."""
    pairs = []
    for predicate, expr in SlotProjectionCompiler._stack_pairs(_MAPPING):
        literal, column = _LITERAL.match(expr), _COLUMN.match(expr)
        if literal:
            pairs.append((predicate, lambda row, v=literal["value"]: v))
        elif column:
            # COALESCE(..., ''): an unset attribute is an empty object, never NULL.
            pairs.append((predicate, lambda row, c=column["column"]: row[c] or ""))
        else:
            raise AssertionError(f"unmodelled stack() expression: {expr}")
    return pairs


_PAIRS = _projection()


def _events(change_type: str, row: dict, version: int):
    """The slot events of one change-feed row (``slot_event_view_sql``)."""
    key = str(row["id"])
    for predicate, project in _PAIRS:
        obj = project(row)
        op = "DELETE" if change_type in _DELETES or obj is None else "UPSERT"
        seq = (version, 0 if change_type == "update_preimage" else 1)
        yield (_MAP_ID, key, predicate), seq, op, (_SUBJECT_PREFIX + key, obj)


def _fold(events) -> dict:
    """AUTO CDC SCD TYPE 1: per key, the event with the highest sequence wins."""
    latest: dict = {}
    for key, seq, op, value in events:
        if key not in latest or seq > latest[key][0]:
            latest[key] = (seq, op, value)
    return {key: value for key, (_seq, op, value) in latest.items() if op == "UPSERT"}


def _slots_of(table: dict) -> dict:
    return {
        (_MAP_ID, str(row["id"]), predicate): (_SUBJECT_PREFIX + str(row["id"]), project(row))
        for row in table.values()
        for predicate, project in _PAIRS
    }


def _non_blank(obj) -> bool:
    return obj is not None and obj.strip() != ""


def _full_build(table: dict) -> set:
    return {
        (_SUBJECT_PREFIX + str(row["id"]), predicate, project(row))
        for row in table.values()
        for predicate, project in _PAIRS
        if _non_blank(project(row))
    }


_VALUES = st.sampled_from([None, "", " ", "a", "b"])
#: Per commit, distinct source keys with what to do with each: a key not in the
#: table is inserted; one in it is deleted or updated.
_COMMITS = st.lists(
    st.dictionaries(
        st.integers(0, 4),
        st.tuples(st.sampled_from(["delete", "update"]), _VALUES, _VALUES),
        max_size=4,
    ),
    max_size=8,
)


def _replay(commits) -> tuple[dict, list]:
    """Apply *commits* to an empty table; return the final table and the change feed."""
    table: dict = {}
    feed: list = []
    for version, commit in enumerate(commits, start=1):
        for key, (action, name, age) in commit.items():
            new = {"id": key, "name": name, "age": age}
            if key not in table:
                feed.append(("insert", new, version))
                table[key] = new
            elif action == "delete":
                feed.append(("delete", table.pop(key), version))
            else:
                feed.append(("update_preimage", table[key], version))
                feed.append(("update_postimage", new, version))
                table[key] = new
    return table, feed


def _folded(feed, rnd) -> dict:
    """Fold the feed's events delivered in any order (``SEQUENCE BY`` restores it)."""
    events = [event for change in feed for event in _events(*change)]
    rnd.shuffle(events)
    return _fold(events)


@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(_COMMITS, st.randoms(use_true_random=False))
def test_the_folded_slots_are_the_slots_of_the_final_rows(commits, rnd):
    table, feed = _replay(commits)

    assert _folded(feed, rnd) == _slots_of(table)


@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(_COMMITS, st.randoms(use_true_random=False))
def test_the_distinct_non_blank_slot_triples_are_the_full_build(commits, rnd):
    table, feed = _replay(commits)

    triples = {
        (subject, predicate, obj)
        for (_map, _key, predicate), (subject, obj) in _folded(feed, rnd).items()
        if _non_blank(obj)
    }
    assert triples == _full_build(table)


class TestTheModelMatchesTheCompiler:
    """The rules the model encodes, read off the SQL the pipeline runs."""

    def test_events_are_keyed_and_sequenced_like_the_auto_cdc_flow(self):
        flow = SlotProjectionCompiler.auto_cdc_flow_sql(
            flow_name="f", target_fqn="main.sales.t_slots", view_name="v"
        )
        assert "KEYS (map_id, source_key, predicate)" in flow
        assert "APPLY AS DELETE WHEN op = 'DELETE'" in flow
        assert "SEQUENCE BY STRUCT(_commit_version, seq_rank)" in flow
        assert "STORED AS SCD TYPE 1" in flow

    def test_the_op_and_rank_are_derived_like_the_event_view(self):
        view = SlotProjectionCompiler.slot_event_view_sql(
            _MAPPING, base_uri="", view_name="v", source_fqn="main.sales.customers"
        )
        assert (
            "CASE WHEN _change_type IN ('delete','update_preimage') OR s.object IS NULL"
            in view
        )
        assert "CASE WHEN _change_type = 'update_preimage' THEN 0 ELSE 1 END AS seq_rank" in view
        assert "CAST(id AS STRING) AS source_key" in view
        assert f"CONCAT('{_SUBJECT_PREFIX}', TRY_CAST(id AS STRING)) AS subject" in view

    def test_the_full_build_filter_is_the_one_modelled(self):
        assert NON_BLANK_OBJECT_SQL == "WHERE object IS NOT NULL AND TRIM(object) != ''"
