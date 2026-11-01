"""SlotProjectionCompiler: row-local mappings -> Lakeflow slot-event SQL."""

import hashlib
import json
import logging
import re

import pytest

from back.core.errors import ValidationError
from back.core.logging import get_logger
from back.core.streaming import SlotPipelineSpec, SlotProjectionCompiler

pytestmark = pytest.mark.unit

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
CUSTOMER = "http://ex.org/Customer"
EMAIL = "http://ex.org/email"
PLACED_BY = "http://ex.org/placedBy"


def _entity(**overrides):
    """An extractor-shaped, row-local entity mapping (carries ``class_uri``)."""
    mapping = {
        "table": "main.sales.customers",
        "sql_query": None,
        "uri_template": "http://ex.org/Customer/{id}",
        "id_column": "id",
        "label_column": "name",
        "class_uri": CUSTOMER,
        "predicates": {
            RDFS_LABEL: {"type": "column", "column": "name"},
            EMAIL: {"type": "column", "column": "email"},
        },
    }
    mapping.update(overrides)
    return mapping


def _relationship(**overrides):
    """A hand-built table-based relationship (self-contained FK columns)."""
    rel = {
        "predicate": PLACED_BY,
        "table": "main.sales.orders",
        "subject_template": "http://ex.org/Order/{order_id}",
        "object_template": "http://ex.org/Customer/{customer_id}",
        "subject_column": "order_id",
        "object_column": "customer_id",
    }
    rel.update(overrides)
    return rel


def _extractor_relationship():
    """Shape produced by ``extract_r2rml_mappings``: sql_query-based, no table."""
    return {
        "predicate": PLACED_BY,
        "sql_query": "SELECT o.order_id, c.id AS customer_id FROM orders o JOIN c",
        "subject_template": "http://ex.org/Order/{order_id}",
        "object_template": "http://ex.org/Customer/{customer_id}",
        "subject_column": "order_id",
        "object_column": "customer_id",
    }


def _view_sql(mapping=None, **kwargs):
    return SlotProjectionCompiler.slot_event_view_sql(
        mapping or _entity(),
        base_uri="http://ex.org/",
        view_name=kwargs.pop("view_name", "slot_events_x"),
        source_fqn=kwargs.pop("source_fqn", "main.sales.customers"),
    )


class TestClassify:
    def test_plain_table_entity_is_row_local(self):
        assert SlotProjectionCompiler.classify(_entity()) == "row_local"

    def test_second_argument_is_optional_and_ignored(self):
        assert SlotProjectionCompiler.classify(_entity(), []) == "row_local"
        assert SlotProjectionCompiler.classify(_entity(), None) == "row_local"

    def test_reference_predicate_is_a_join_hence_batch(self):
        predicates = {
            EMAIL: {"type": "column", "column": "email"},
            PLACED_BY: {
                "type": "reference",
                "parent_map": "#Other",
                "child_column": "customer_id",
                "parent_column": "id",
            },
        }
        assert (
            SlotProjectionCompiler.classify(_entity(predicates=predicates)) == "batch"
        )

    def test_sql_query_entity_is_batch(self):
        mapping = _entity(sql_query="SELECT id, name FROM t", table=None)
        assert SlotProjectionCompiler.classify(mapping) == "batch"

    def test_sql_query_alongside_table_is_still_batch(self):
        assert SlotProjectionCompiler.classify(_entity(sql_query="SELECT 1")) == "batch"

    def test_entity_without_table_is_batch(self):
        assert SlotProjectionCompiler.classify(_entity(table=None)) == "batch"

    def test_subject_template_without_column_is_batch(self):
        mapping = _entity(uri_template="http://ex.org/Customer/fixed")
        assert SlotProjectionCompiler.classify(mapping) == "batch"

    @pytest.mark.parametrize(
        "overrides",
        [
            {"table": "main.sales.cust omers"},
            {"id_column": "id; DROP TABLE x"},
            {"uri_template": "http://ex.org/Customer/{id-1}"},
            {
                "predicates": {
                    EMAIL: {"type": "column", "column": "e mail"},
                }
            },
        ],
    )
    def test_unsafe_identifier_falls_back_to_batch(self, overrides):
        assert SlotProjectionCompiler.classify(_entity(**overrides)) == "batch"

    def test_table_based_relationship_is_row_local(self):
        assert SlotProjectionCompiler.classify(_relationship()) == "row_local"

    def test_extractor_shaped_relationship_is_batch(self):
        assert SlotProjectionCompiler.classify(_extractor_relationship()) == "batch"

    def test_relationship_missing_object_column_is_batch(self):
        assert (
            SlotProjectionCompiler.classify(_relationship(object_column=None))
            == "batch"
        )

    def test_relationship_with_sql_query_and_table_is_batch(self):
        rel = _relationship(sql_query="SELECT 1")
        assert SlotProjectionCompiler.classify(rel) == "batch"


class TestMapId:
    def test_shape(self):
        assert re.fullmatch(
            r"map_[0-9a-f]{12}", SlotProjectionCompiler.map_id(_entity())
        )

    def test_golden_value_is_stable_across_runs(self):
        payload = {
            "kind": "entity",
            "source": "main.sales.customers",
            "subject": "http://ex.org/Customer/{id}",
            "class_uri": CUSTOMER,
            "label_column": "name",
            "predicates": {
                RDFS_LABEL: {"type": "column", "column": "name"},
                EMAIL: {"type": "column", "column": "email"},
            },
        }
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        expected = "map_" + hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:12]
        assert SlotProjectionCompiler.map_id(_entity()) == expected

    def test_independent_of_predicate_and_key_order(self):
        reordered = _entity(
            predicates={
                EMAIL: {"column": "email", "type": "column"},
                RDFS_LABEL: {"column": "name", "type": "column"},
            }
        )
        assert SlotProjectionCompiler.map_id(
            reordered
        ) == SlotProjectionCompiler.map_id(_entity())

    def test_changes_when_a_predicate_is_added(self):
        predicates = {
            **_entity()["predicates"],
            "http://ex.org/phone": {"type": "column", "column": "phone"},
        }
        assert SlotProjectionCompiler.map_id(
            _entity(predicates=predicates)
        ) != SlotProjectionCompiler.map_id(_entity())

    def test_changes_with_source_template_and_class(self):
        base = SlotProjectionCompiler.map_id(_entity())
        assert SlotProjectionCompiler.map_id(_entity(table="main.sales.other")) != base
        assert (
            SlotProjectionCompiler.map_id(_entity(uri_template="http://ex.org/C/{id}"))
            != base
        )
        assert (
            SlotProjectionCompiler.map_id(_entity(class_uri="http://ex.org/Other"))
            != base
        )

    def test_sql_query_is_the_source_when_present(self):
        a = SlotProjectionCompiler.map_id(_entity(table=None, sql_query="SELECT 1"))
        b = SlotProjectionCompiler.map_id(_entity(table=None, sql_query="SELECT 2"))
        assert a != b

    def test_relationship_and_entity_ids_differ(self):
        assert SlotProjectionCompiler.map_id(
            _relationship()
        ) != SlotProjectionCompiler.map_id(_entity())

    def test_relationship_id_changes_with_predicate(self):
        assert SlotProjectionCompiler.map_id(
            _relationship(predicate="http://ex.org/other")
        ) != SlotProjectionCompiler.map_id(_relationship())


class TestSourceKeyExpr:
    def test_single_column(self):
        assert SlotProjectionCompiler.source_key_expr(_entity()) == "CAST(id AS STRING)"

    def test_composite_template_concatenates_in_template_order(self):
        mapping = _entity(
            uri_template="http://ex.org/Line/{order_id}/{line_no}", id_column="order_id"
        )
        assert SlotProjectionCompiler.source_key_expr(mapping) == (
            "concat_ws('|', CAST(order_id AS STRING), CAST(line_no AS STRING))"
        )

    def test_relationship_keys_on_the_subject_template(self):
        assert (
            SlotProjectionCompiler.source_key_expr(_relationship())
            == "CAST(order_id AS STRING)"
        )

    def test_unsafe_column_raises(self):
        with pytest.raises(ValidationError):
            SlotProjectionCompiler.source_key_expr(_entity(id_column="id) --"))


class TestSlotEventViewSql:
    def test_core_fragments(self):
        sql = _view_sql()
        assert sql.startswith("CREATE TEMPORARY VIEW slot_events_x AS")
        assert "readChangeFeed = true" in sql
        assert "FROM STREAM main.sales.customers WITH (readChangeFeed = true)" in sql
        assert "stack(" in sql
        assert "update_preimage" in sql
        assert "'DELETE'" in sql and "'UPSERT'" in sql
        assert "_commit_version" in sql
        assert (
            "CASE WHEN _change_type = 'update_preimage' THEN 0 ELSE 1 END AS seq_rank"
            in sql
        )
        assert "CAST(id AS STRING) AS source_key" in sql
        assert "LATERAL VIEW stack(" in sql and ") s AS predicate, object" in sql

    def test_contains_the_map_id_literal(self):
        mapping = _entity()
        sql = _view_sql(mapping)
        assert f"'{SlotProjectionCompiler.map_id(mapping)}' AS map_id" in sql

    def test_one_stack_pair_per_predicate_including_rdf_type(self):
        sql = _view_sql()
        assert "stack(3," in sql
        assert f"'{RDF_TYPE}', '{CUSTOMER}'" in sql
        assert f"'{RDFS_LABEL}', COALESCE(TRY_CAST(name AS STRING), '')" in sql
        assert f"'{EMAIL}', COALESCE(TRY_CAST(email AS STRING), '')" in sql
        for predicate in (RDF_TYPE, RDFS_LABEL, EMAIL):
            assert sql.count(f"'{predicate}'") == 1

    def test_subject_expression_uses_the_uri_template(self):
        sql = _view_sql()
        assert (
            "CONCAT('http://ex.org/Customer/', TRY_CAST(id AS STRING)) AS subject"
            in sql
        )

    def test_delete_when_preimage_or_null_object(self):
        sql = _view_sql()
        assert re.search(
            r"CASE WHEN _change_type IN \('delete','update_preimage'\) "
            r"OR s\.object IS NULL\s+THEN 'DELETE' ELSE 'UPSERT' END AS op",
            sql,
        )

    def test_unsafe_source_fqn_raises(self):
        with pytest.raises(ValidationError):
            _view_sql(source_fqn="main.sales.customers; DROP TABLE x")

    def test_quotes_in_class_uri_predicate_and_template_are_escaped(self):
        mapping = _entity(
            class_uri="http://ex.org/O'Brien",
            uri_template="http://ex.org/it's/{id}",
            predicates={"http://ex.org/it's": {"type": "column", "column": "name"}},
            label_column="",
        )
        sql = _view_sql(mapping)
        assert "'http://ex.org/O''Brien'" in sql
        assert "'http://ex.org/it''s'" in sql
        assert "CONCAT('http://ex.org/it''s/', TRY_CAST(id AS STRING))" in sql
        assert "O'Brien" not in sql

    def test_label_column_falls_back_to_rdfs_label_pair(self):
        mapping = _entity(predicates={EMAIL: {"type": "column", "column": "email"}})
        sql = _view_sql(mapping)
        assert sql.count(f"'{RDFS_LABEL}'") == 1
        assert "stack(3," in sql

    @pytest.mark.parametrize(
        "kwargs",
        [{"view_name": "bad name"}, {"source_fqn": ""}, {"source_fqn": "a b.c"}],
    )
    def test_bad_identifiers_raise(self, kwargs):
        with pytest.raises(ValidationError):
            _view_sql(**kwargs)

    def test_unsafe_predicate_column_raises(self):
        mapping = _entity(
            predicates={EMAIL: {"type": "column", "column": "email) FROM x --"}}
        )
        with pytest.raises(ValidationError):
            _view_sql(mapping)

    def test_relationship_projects_a_single_object_pair(self):
        sql = _view_sql(_relationship(), source_fqn="main.sales.orders")
        assert "stack(1, " in sql
        assert (
            f"'{PLACED_BY}', CONCAT('http://ex.org/Customer/', TRY_CAST(customer_id AS STRING))"
            in sql
        )
        assert (
            "CONCAT('http://ex.org/Order/', TRY_CAST(order_id AS STRING)) AS subject"
            in sql
        )
        assert "CAST(order_id AS STRING) AS source_key" in sql
        assert f"'{SlotProjectionCompiler.map_id(_relationship())}' AS map_id" in sql


class TestAutoCdcFlowSql:
    def _sql(self):
        return SlotProjectionCompiler.auto_cdc_flow_sql(
            flow_name="slot_flow_x",
            target_fqn="main.ob.triplestore_d_V1_slots",
            view_name="slot_events_x",
        )

    def test_exact_statement(self):
        assert self._sql() == (
            "CREATE FLOW slot_flow_x AS AUTO CDC INTO main.ob.triplestore_d_V1_slots\n"
            "FROM STREAM(slot_events_x)\n"
            "KEYS (map_id, source_key, predicate)\n"
            "APPLY AS DELETE WHEN op = 'DELETE'\n"
            "SEQUENCE BY STRUCT(_commit_version, seq_rank)\n"
            "COLUMNS * EXCEPT (op, _commit_version, seq_rank)\n"
            "STORED AS SCD TYPE 1"
        )

    def test_clause_order_is_binding(self):
        sql = self._sql()
        clauses = [
            "KEYS (map_id, source_key, predicate)",
            "APPLY AS DELETE WHEN op = 'DELETE'",
            "SEQUENCE BY STRUCT(_commit_version, seq_rank)",
            "COLUMNS * EXCEPT (op, _commit_version, seq_rank)",
            "STORED AS SCD TYPE 1",
        ]
        positions = [sql.index(clause) for clause in clauses]
        assert positions == sorted(positions)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"flow_name": "bad flow"},
            {"view_name": "v; x"},
            {"target_fqn": ""},
            {"target_fqn": "main.ob.t) --"},
        ],
    )
    def test_bad_identifiers_raise(self, kwargs):
        args = dict(flow_name="f", target_fqn="main.ob.t", view_name="v")
        args.update(kwargs)
        with pytest.raises(ValidationError):
            SlotProjectionCompiler.auto_cdc_flow_sql(**args)


class TestSlotsTableSql:
    def test_exact_statement(self):
        assert SlotProjectionCompiler.slots_table_sql(
            "main.ob.triplestore_d_V1_slots"
        ) == (
            "CREATE OR REFRESH STREAMING TABLE main.ob.triplestore_d_V1_slots\n"
            "CLUSTER BY (predicate, subject)\n"
            "TBLPROPERTIES (delta.enableRowTracking = true, "
            "delta.enableDeletionVectors = true)"
        )

    def test_bad_fqn_raises(self):
        with pytest.raises(ValidationError):
            SlotProjectionCompiler.slots_table_sql("main.ob.t; DROP")


class TestCompile:
    SLOTS = "main.ob.triplestore_d_V1_slots"

    def _compile(self, entities, relationships=()):
        return SlotProjectionCompiler.compile(
            entities,
            list(relationships),
            base_uri="http://ex.org/",
            slots_fqn=self.SLOTS,
        )

    def test_returns_a_spec_with_the_slots_table_first(self):
        spec = self._compile({CUSTOMER: _entity()})
        assert isinstance(spec, SlotPipelineSpec)
        assert spec.slots_fqn == self.SLOTS
        assert spec.statements[0].startswith(
            f"CREATE OR REFRESH STREAMING TABLE {self.SLOTS}"
        )

    def test_one_view_and_flow_per_row_local_mapping(self):
        spec = self._compile({CUSTOMER: _entity()})
        map_id = SlotProjectionCompiler.map_id(_entity())
        assert spec.row_local_map_ids == [map_id]
        assert len(spec.statements) == 3
        assert spec.statements[1].startswith(
            f"CREATE TEMPORARY VIEW slot_events_{map_id} AS"
        )
        assert spec.statements[2].startswith(
            f"CREATE FLOW slot_flow_{map_id} AS AUTO CDC INTO {self.SLOTS}"
        )
        assert f"FROM STREAM(slot_events_{map_id})" in spec.statements[2]
        assert spec.source_tables == ["main.sales.customers"]
        assert spec.batch_mappings == []

    def test_join_and_sql_query_mappings_go_to_batch(self):
        join = _entity(
            predicates={
                PLACED_BY: {
                    "type": "reference",
                    "parent_map": "#P",
                    "child_column": "a",
                    "parent_column": "b",
                }
            }
        )
        sqlq = _entity(table=None, sql_query="SELECT 1 AS id")
        spec = self._compile(
            {
                CUSTOMER: _entity(),
                "http://ex.org/Joined": join,
                "http://ex.org/Query": sqlq,
            },
            [_extractor_relationship()],
        )
        assert len(spec.row_local_map_ids) == 1
        assert len(spec.statements) == 3
        assert len(spec.batch_mappings) == 3
        batch_classes = {m.get("class_uri") for m in spec.batch_mappings}
        assert {"http://ex.org/Joined", "http://ex.org/Query"} <= batch_classes
        assert _extractor_relationship() in spec.batch_mappings

    def test_classes_sharing_one_mapping_get_distinct_ids_views_and_flows(self):
        shared = _entity()
        shared.pop("class_uri")
        spec = self._compile({CUSTOMER: shared, "http://ex.org/Person": shared})
        assert len(set(spec.row_local_map_ids)) == 2
        views = [s for s in spec.statements if s.startswith("CREATE TEMPORARY VIEW")]
        flows = [s for s in spec.statements if s.startswith("CREATE FLOW")]
        assert len(views) == 2 and len(flows) == 2
        joined = "\n".join(views)
        assert f"'{RDF_TYPE}', '{CUSTOMER}'" in joined
        assert f"'{RDF_TYPE}', 'http://ex.org/Person'" in joined
        assert "class_uri" not in shared

    def test_row_local_relationship_is_compiled(self):
        spec = self._compile({}, [_relationship()])
        rel_id = SlotProjectionCompiler.map_id(_relationship())
        assert spec.row_local_map_ids == [rel_id]
        assert spec.source_tables == ["main.sales.orders"]
        assert spec.batch_mappings == []

    def test_duplicate_relationship_is_compiled_once(self):
        spec = self._compile({}, [_relationship(), _relationship()])
        assert len(spec.row_local_map_ids) == 1
        assert len(spec.statements) == 3

    def test_source_tables_are_unique_and_sorted(self):
        other = _entity(table="main.sales.accounts", class_uri="http://ex.org/Account")
        spec = self._compile(
            {
                "http://ex.org/Z": _entity(),
                "http://ex.org/Account": other,
                "http://ex.org/Y": _entity(uri_template="http://ex.org/Y/{id}"),
            }
        )
        assert spec.source_tables == ["main.sales.accounts", "main.sales.customers"]

    def test_is_deterministic_for_equal_inputs(self):
        a = self._compile({CUSTOMER: _entity()}, [_relationship()])
        b = self._compile({CUSTOMER: _entity()}, [_relationship()])
        assert a.to_sql() == b.to_sql()
        assert a.row_local_map_ids == b.row_local_map_ids

    def test_to_sql_round_trips_the_statements(self):
        spec = self._compile({CUSTOMER: _entity()}, [_relationship()])
        text = spec.to_sql()
        assert text == ";\n\n".join(spec.statements)
        assert text.split(";\n\n") == spec.statements

    def test_all_batch_input_still_emits_the_slots_table(self):
        spec = self._compile({CUSTOMER: _entity(table=None, sql_query="SELECT 1")})
        assert len(spec.statements) == 1
        assert spec.row_local_map_ids == []
        assert spec.source_tables == []

    def test_empty_input(self):
        spec = self._compile({}, [])
        assert len(spec.statements) == 1
        assert len(spec.batch_mappings) == 0

    def test_bad_slots_fqn_raises(self):
        with pytest.raises(ValidationError):
            SlotProjectionCompiler.compile(
                {}, [], base_uri="http://ex.org/", slots_fqn="main.ob.t; DROP"
            )


class TestSlotPipelineSpec:
    def test_to_sql_joins_with_semicolon_and_blank_line(self):
        spec = SlotPipelineSpec(
            slots_fqn="c.s.t",
            statements=["A", "B", "C"],
            row_local_map_ids=[],
            batch_mappings=[],
            source_tables=[],
        )
        assert spec.to_sql() == "A;\n\nB;\n\nC"

    def test_empty_spec_renders_empty_string(self):
        spec = SlotPipelineSpec(slots_fqn="c.s.t")
        assert spec.to_sql() == ""
        assert spec.statements == [] and spec.batch_mappings == []


class TestNullKeyGuard:
    """The view drops rows whose key is NULL: they cannot form a subject URI."""

    LATERAL_TAIL = ") s AS predicate, object\n"

    def test_single_key_adds_one_not_null_predicate_after_the_lateral_view(self):
        sql = _view_sql()
        assert sql.endswith(self.LATERAL_TAIL + "WHERE id IS NOT NULL")
        assert sql.count("IS NOT NULL") == 1
        assert sql.index("LATERAL VIEW") < sql.index("WHERE id IS NOT NULL")

    def test_composite_key_ands_every_template_column(self):
        mapping = _entity(
            uri_template="http://ex.org/Line/{order_id}/{line_no}", id_column="order_id"
        )
        sql = _view_sql(mapping)
        assert sql.endswith(
            self.LATERAL_TAIL + "WHERE order_id IS NOT NULL AND line_no IS NOT NULL"
        )

    def test_relationship_guards_the_subject_column(self):
        sql = _view_sql(_relationship(), source_fqn="main.sales.orders")
        assert sql.endswith(self.LATERAL_TAIL + "WHERE order_id IS NOT NULL")
        assert "customer_id IS NOT NULL" not in sql

    def test_guard_follows_the_same_columns_as_source_key(self):
        mapping = _entity(
            uri_template="http://ex.org/Line/{order_id}/{line_no}", id_column="order_id"
        )
        sql = _view_sql(mapping)
        assert (
            "concat_ws('|', CAST(order_id AS STRING), CAST(line_no AS STRING))" in sql
        )
        assert sql.count("IS NOT NULL") == 2


class TestClassifyAgreesWithEmitters:
    def test_entity_without_class_uri_is_batch(self):
        assert SlotProjectionCompiler.classify(_entity(class_uri=None)) == "batch"
        assert SlotProjectionCompiler.classify(_entity(class_uri="")) == "batch"

    def test_relationship_without_predicate_is_batch(self):
        assert SlotProjectionCompiler.classify(_relationship(predicate="")) == "batch"
        assert SlotProjectionCompiler.classify(_relationship(predicate=None)) == "batch"

    @pytest.fixture
    def compiler_logs(self, caplog):
        """Capture the compiler's own logger, whatever other tests configured.

        ``LogManager.setup()`` (run by other tests in the suite) sets
        ``propagate=False`` on the app logger, so caplog's root-level capture
        would miss these records; attach its handler to the module logger.
        """
        target = get_logger("back.core.streaming.SlotProjectionCompiler")
        previous_level = target.level
        target.addHandler(caplog.handler)
        target.setLevel(logging.DEBUG)
        try:
            yield caplog
        finally:
            target.removeHandler(caplog.handler)
            target.setLevel(previous_level)

    def test_batch_reason_is_logged_at_debug_level(self, compiler_logs):
        SlotProjectionCompiler.classify(_entity(class_uri=""))
        SlotProjectionCompiler.classify(_relationship(predicate=""))
        messages = [record.getMessage() for record in compiler_logs.records]
        assert any("entity without class_uri" in m for m in messages)
        assert any("relationship without predicate" in m for m in messages)

    def test_compile_routes_a_predicateless_relationship_to_batch(self):
        rel = _relationship(predicate="")
        spec = SlotProjectionCompiler.compile(
            {}, [rel], base_uri="http://ex.org/", slots_fqn="main.ob.t_slots"
        )
        assert spec.batch_mappings == [rel]
        assert spec.row_local_map_ids == []

    def test_compile_still_injects_class_uri_into_entities_without_one(self):
        shared = _entity()
        shared.pop("class_uri")
        spec = SlotProjectionCompiler.compile(
            {CUSTOMER: shared},
            [],
            base_uri="http://ex.org/",
            slots_fqn="main.ob.t_slots",
        )
        assert len(spec.row_local_map_ids) == 1

    @pytest.mark.parametrize(
        "mapping",
        [
            _entity(),
            _entity(class_uri=""),
            _entity(id_column=None),
            _entity(id_column=""),
            _entity(label_column="bad label"),
            _entity(label_column=""),
            _entity(predicates={}),
            _entity(predicates={EMAIL: {"type": "column"}}),
            _entity(predicates={EMAIL: {"type": "column", "column": None}}),
            _entity(uri_template=None),
            _entity(uri_template="http://ex.org/Line/{a}/{b}", id_column="a"),
            _entity(uri_template="http://ex.org/Line/{a}/{b-1}", id_column="a"),
            _entity(uri_template="http://ex.org/Line/{a}", id_column="other"),
            _entity(table="main.sales.t", sql_query="  "),
            _entity(table=" "),
            _relationship(),
            _relationship(predicate=""),
            _relationship(subject_column=None),
            _relationship(object_column="bad col"),
            _relationship(subject_template="http://ex.org/Order/fixed"),
            _relationship(object_template=None),
            _relationship(table="main.sales.o rders"),
        ],
    )
    def test_every_row_local_mapping_compiles_without_raising(self, mapping):
        kind = SlotProjectionCompiler.classify(mapping)
        if kind != "row_local":
            return
        SlotProjectionCompiler.slot_event_view_sql(
            mapping,
            base_uri="http://ex.org/",
            view_name="v",
            source_fqn=mapping["table"],
        )
        spec = SlotProjectionCompiler.compile(
            {mapping["class_uri"]: mapping} if "class_uri" in mapping else {},
            [] if "class_uri" in mapping else [mapping],
            base_uri="http://ex.org/",
            slots_fqn="main.ob.t_slots",
        )
        assert spec.batch_mappings == []
        assert len(spec.row_local_map_ids) == 1

    def test_the_agreement_matrix_is_not_vacuous(self):
        row_local = [
            _entity(),
            _entity(label_column=""),
            _entity(uri_template="http://ex.org/Line/{a}/{b}", id_column="a"),
            _relationship(),
        ]
        assert all(SlotProjectionCompiler.classify(m) == "row_local" for m in row_local)
