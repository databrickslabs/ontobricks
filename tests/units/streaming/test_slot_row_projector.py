"""SlotRowProjector: the Python projection of one row-local mapping equals its slot-event SQL.

The unit tests pin each rule of ``SlotProjectionCompiler.slot_event_view_sql``
(NULL key, composite key, subject template, ``rdf:type`` / ``rdfs:label`` /
attribute pairs, ``COALESCE(TRY_CAST(... AS STRING), '')``). The property test
generates random row-local mappings and rows, evaluates the SQL the compiler
emits for them (each ``stack()`` expression, the subject and key expressions
and the ``WHERE`` clause), and checks the projector against that evaluation.
"""

from __future__ import annotations

import re

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from back.core.errors import ValidationError
from back.core.streaming import SlotProjectionCompiler, SlotRowProjector

pytestmark = pytest.mark.unit

_RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
_RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
_CUSTOMER = "http://ex.org/Customer"


def _customer(**overrides) -> dict:
    mapping = {
        "table": "main.sales.customers",
        "sql_query": None,
        "uri_template": "http://ex.org/Customer/{id}",
        "id_column": "id",
        "label_column": "name",
        "class_uri": _CUSTOMER,
        "predicates": {
            _RDFS_LABEL: {"type": "column", "column": "name"},
            "http://ex.org/age": {"type": "column", "column": "age"},
        },
    }
    mapping.update(overrides)
    return mapping


class TestIdentity:
    def test_the_map_id_is_the_compiler_map_id(self):
        mapping = _customer()

        assert SlotRowProjector.from_mapping(mapping, base_uri="").map_id == (
            SlotProjectionCompiler.map_id(mapping)
        )

    def test_the_source_table_is_the_mapping_table(self):
        projector = SlotRowProjector.from_mapping(_customer(), base_uri="")

        assert projector.source_table == "main.sales.customers"

    def test_a_batch_mapping_is_rejected(self):
        with pytest.raises(ValidationError, match="not row-local"):
            SlotRowProjector.from_mapping(_customer(table=None, sql_query="SELECT 1"), base_uri="")


class TestSourceKey:
    def test_the_key_is_the_id_as_a_string(self):
        projector = SlotRowProjector.from_mapping(_customer(), base_uri="")

        assert projector.source_key({"id": 7}) == "7"

    def test_a_null_key_ignores_the_row(self):
        projector = SlotRowProjector.from_mapping(_customer(), base_uri="")

        assert projector.source_key({"id": None, "name": "a"}) is None

    def test_a_missing_key_column_ignores_the_row(self):
        projector = SlotRowProjector.from_mapping(_customer(), base_uri="")

        assert projector.source_key({"name": "a"}) is None

    def test_a_composite_template_joins_its_columns_with_a_pipe(self):
        projector = SlotRowProjector.from_mapping(
            _customer(uri_template="http://ex.org/Order/{region}/{num}", id_column="region"),
            base_uri="",
        )

        assert projector.source_key({"region": "eu", "num": 3}) == "eu|3"
        assert projector.source_key({"region": "eu", "num": None}) is None


class TestSubject:
    def test_the_subject_is_the_template_prefix_and_the_id(self):
        projector = SlotRowProjector.from_mapping(_customer(), base_uri="")

        assert projector.subject({"id": 7}) == "http://ex.org/Customer/7"

    def test_a_null_id_has_no_subject(self):
        projector = SlotRowProjector.from_mapping(_customer(), base_uri="")

        assert projector.subject({"id": None}) is None


class TestSlots:
    def test_the_class_label_and_attributes_are_projected(self):
        projector = SlotRowProjector.from_mapping(_customer(), base_uri="")

        assert projector.slots({"id": 1, "name": "Ann", "age": 41}) == {
            _RDF_TYPE: _CUSTOMER,
            _RDFS_LABEL: "Ann",
            "http://ex.org/age": "41",
        }

    def test_a_null_attribute_is_an_empty_object(self):
        """``COALESCE(TRY_CAST(age AS STRING), '')``: never NULL."""
        projector = SlotRowProjector.from_mapping(_customer(), base_uri="")

        assert projector.slots({"id": 1, "name": None, "age": None})["http://ex.org/age"] == ""

    def test_the_label_column_is_projected_once(self):
        projector = SlotRowProjector.from_mapping(_customer(), base_uri="")

        assert list(projector.slots({"id": 1, "name": "Ann", "age": 1})).count(_RDFS_LABEL) == 1

    def test_without_a_label_column_there_is_no_label_slot(self):
        mapping = _customer(
            label_column=None, predicates={"http://ex.org/age": {"type": "column", "column": "age"}}
        )
        projector = SlotRowProjector.from_mapping(mapping, base_uri="")

        assert set(projector.slots({"id": 1, "age": 3})) == {_RDF_TYPE, "http://ex.org/age"}

    @pytest.mark.parametrize(
        ("value", "text"),
        [(True, "true"), (False, "false"), (0, "0"), (-7, "-7"), ("x", "x"), (None, None)],
    )
    def test_strings_integers_and_booleans_render_like_a_spark_cast(self, value, text):
        assert SlotRowProjector.spark_string(value) == text

    @pytest.mark.parametrize("value", [1.5, 1e20, float("nan"), {"a": 1}, [1], b"x"])
    def test_other_values_must_be_cast_in_spark_first(self, value):
        """Python renders them unlike Spark (``1e+20`` vs ``1.0E20``, ``nan`` vs ``NaN``)."""
        with pytest.raises(ValidationError, match="'age'"):
            SlotRowProjector.spark_string(value, "age")

    def test_a_float_attribute_in_a_row_names_its_column(self):
        projector = SlotRowProjector.from_mapping(_customer(), base_uri="")

        with pytest.raises(ValidationError, match="'age'"):
            projector.slots({"id": "1", "name": "Ann", "age": 41.5})

    def test_columns_are_looked_up_case_insensitively_like_spark(self):
        projector = SlotRowProjector.from_mapping(_customer(), base_uri="")
        row = {"ID": "1", "Name": "Ann", "AGE": "41"}

        assert projector.project(row) == (
            "1",
            "http://ex.org/Customer/1",
            {_RDF_TYPE: _CUSTOMER, _RDFS_LABEL: "Ann", "http://ex.org/age": "41"},
        )

    def test_project_equals_the_three_projections(self):
        projector = SlotRowProjector.from_mapping(_customer(), base_uri="")
        row = {"id": "1", "name": None, "age": "41"}

        assert projector.project(row) == (
            projector.source_key(row),
            projector.subject(row),
            projector.slots(row),
        )

    def test_the_predicates_are_the_slot_predicates_in_stack_order(self):
        projector = SlotRowProjector.from_mapping(_customer(), base_uri="")

        assert projector.predicates() == (_RDF_TYPE, _RDFS_LABEL, "http://ex.org/age")


class TestForMappings:
    def test_one_projector_per_class_of_a_shared_triples_map(self):
        shared = _customer()
        projectors = SlotRowProjector.for_mappings(
            {_CUSTOMER: shared, "http://ex.org/Person": shared}, [], base_uri=""
        )

        assert [p.map_id for p in projectors] == [
            SlotProjectionCompiler.map_id({**shared, "class_uri": _CUSTOMER}),
            SlotProjectionCompiler.map_id({**shared, "class_uri": "http://ex.org/Person"}),
        ]

    def test_batch_mappings_and_relationships_are_excluded(self):
        relationship = {
            "predicate": "http://ex.org/buys",
            "subject_template": "http://ex.org/Customer/{id}",
            "table": "main.sales.orders",
        }
        projectors = SlotRowProjector.for_mappings(
            {_CUSTOMER: _customer(), "http://ex.org/Order": _customer(sql_query="SELECT 1")},
            [relationship],
            base_uri="",
        )

        assert [p.source_table for p in projectors] == ["main.sales.customers"]

    def test_the_projectors_are_the_mappings_the_compiler_compiles(self):
        entity_mappings = {
            _CUSTOMER: _customer(),
            "http://ex.org/Order": _customer(sql_query="SELECT 1"),
        }
        spec = SlotProjectionCompiler.compile(
            entity_mappings, [], base_uri="", slots_fqn="main.sales.t_slots"
        )

        projectors = SlotRowProjector.for_mappings(entity_mappings, [], base_uri="")

        assert [p.map_id for p in projectors] == spec.row_local_map_ids


# ---------------------------------------------------------------------------
# Property: the projector evaluates the compiled SQL
# ---------------------------------------------------------------------------

_LITERAL = re.compile(r"\A'(?P<value>(?:[^']|'')*)'\Z")
_COALESCED = re.compile(r"\ACOALESCE\(TRY_CAST\((?P<column>\w+) AS STRING\), ''\)\Z")
_SUBJECT = re.compile(
    r"CONCAT\('(?P<prefix>(?:[^']|'')*)', TRY_CAST\((?P<column>\w+) AS STRING\)\) AS subject"
)
_KEY_ONE = re.compile(r"\ACAST\((?P<column>\w+) AS STRING\)\Z")
_KEY_MANY = re.compile(r"\Aconcat_ws\('\|', (?P<casts>.*)\)\Z")
_WHERE = re.compile(r"WHERE (?P<conditions>.*)\Z")


def _unquote(sql_literal: str) -> str:
    return sql_literal.replace("''", "'")


def _sql_oracle(mapping: dict):
    """Evaluate *mapping*'s compiled slot-event SQL on a row: ``(key, subject, slots)``."""
    view = SlotProjectionCompiler.slot_event_view_sql(
        mapping, base_uri="", view_name="v", source_fqn=mapping["table"]
    )
    pairs = []
    for predicate, expr in SlotProjectionCompiler._stack_pairs(mapping):
        literal, coalesced = _LITERAL.match(expr), _COALESCED.match(expr)
        if literal:
            pairs.append((predicate, ("literal", _unquote(literal["value"]))))
        elif coalesced:
            pairs.append((predicate, ("column", coalesced["column"])))
        else:
            raise AssertionError(f"unmodelled stack() expression: {expr}")
    subject = _SUBJECT.search(view)
    key_expr = SlotProjectionCompiler.source_key_expr(mapping)
    one, many = _KEY_ONE.match(key_expr), _KEY_MANY.match(key_expr)
    key_columns = (
        [one["column"]]
        if one
        else [_KEY_ONE.match(cast.strip())["column"] for cast in many["casts"].split(",")]
    )
    not_null = [
        cond.replace(" IS NOT NULL", "").strip()
        for cond in _WHERE.search(view)["conditions"].split(" AND ")
    ]

    def cast(value):
        """``CAST(<string> AS STRING)``: the stream's rows hold strings or NULL only."""
        assert value is None or isinstance(value, str)
        return value

    def evaluate(row: dict):
        if any(row.get(column) is None for column in not_null):
            return None
        key = "|".join(cast(row[column]) for column in key_columns)
        id_text = cast(row.get(subject["column"]))
        subject_value = None if id_text is None else _unquote(subject["prefix"]) + id_text
        slots = {}
        for predicate, (kind, value) in pairs:
            slots[predicate] = value if kind == "literal" else (cast(row.get(value)) or "")
        return key, subject_value, slots

    return evaluate


_COLUMNS = ["id", "a", "b", "name", "age"]
_VALUES = st.one_of(st.none(), st.sampled_from(["", " ", "\t", "x", "it's", "Ann", "7"]))


@st.composite
def _mappings(draw):
    composite = draw(st.booleans())
    if composite:
        first, second = draw(st.permutations(["a", "b"]))
        template, id_column = f"http://ex.org/T/{{{first}}}/{{{second}}}", first
    else:
        template, id_column = "http://ex.org/T/{id}", "id"
    label = draw(st.sampled_from([None, "name", "age"]))
    predicate_columns = draw(st.lists(st.sampled_from(_COLUMNS), max_size=3))
    predicates = {
        f"http://ex.org/p{i}": {"type": "column", "column": column}
        for i, column in enumerate(predicate_columns)
    }
    return {
        "table": "main.sales.things",
        "sql_query": None,
        "uri_template": template,
        "id_column": id_column,
        "label_column": label,
        "class_uri": draw(st.sampled_from(["http://ex.org/Thing", "http://ex.org/O'Brien"])),
        "predicates": predicates,
    }


@pytest.mark.property
@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(_mappings(), st.lists(st.fixed_dictionaries({c: _VALUES for c in _COLUMNS}), max_size=5))
def test_the_projection_equals_the_compiled_slot_event_sql(mapping, rows):
    projector = SlotRowProjector.from_mapping(mapping, base_uri="")
    oracle = _sql_oracle(mapping)

    for row in rows:
        expected = oracle(row)
        key = projector.source_key(row)
        if expected is None:
            assert key is None
            continue
        assert (key, projector.subject(row), projector.slots(row)) == expected
