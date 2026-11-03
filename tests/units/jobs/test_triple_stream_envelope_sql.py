"""The Spark-native envelope explode of the realtime stream.

Real-Time Mode refuses ``mapInPandas`` and a ``UNION`` of several reads of the
same stream (both measured live), so the explode is one pass: every candidate
row is a ``named_struct`` in one ``array``, filtered on ``keep`` and exploded.
"""

from __future__ import annotations

import pytest

from back.core.streaming.SlotRowProjector import SlotRowProjector
from jobs.triple_stream.TripleStreamEnvelope import TripleStreamEnvelope

pytestmark = pytest.mark.unit

_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"


def _mapping(table: str, class_uri: str, *, uri_template: str, id_column: str = "id") -> dict:
    return {
        "table": table,
        "sql_query": None,
        "uri_template": uri_template,
        "id_column": id_column,
        "label_column": "name",
        "class_uri": class_uri,
        "predicates": {_LABEL: {"type": "column", "column": "name"}},
    }


def _by_table(*mappings: dict) -> dict:
    projectors = SlotRowProjector.for_mappings(
        {m["class_uri"]: m for m in mappings}, [], base_uri="http://ex.org/"
    )
    return TripleStreamEnvelope.projectors_by_table(projectors)


_CUSTOMERS = _mapping("cat.sch.customers", "http://ex.org/Customer", uri_template="http://ex.org/Customer/{id}")


class TestExplodeSql:
    def test_the_envelope_is_read_with_and_without_its_payload_wrapper(self):
        sql = TripleStreamEnvelope.explode_sql(_by_table(_CUSTOMERS), {}, "records")

        assert "get_json_object(value, '$.payload.op'), get_json_object(value, '$.op')" in sql
        assert "get_json_object(value, '$.payload.after'), get_json_object(value, '$.after')" in sql
        assert "get_json_object(value, '$.payload.source.table'), get_json_object(value, '$.source.table')" in sql
        assert "FROM records)" in sql

    def test_the_stream_is_read_once_and_exploded_not_unioned(self):
        second = _mapping("cat.sch.customers", "http://ex.org/Person", uri_template="http://ex.org/Person/{id}")
        sql = TripleStreamEnvelope.explode_sql(_by_table(_CUSTOMERS, second), {}, "records")

        assert "UNION" not in sql
        assert sql.count("FROM records") == 1
        assert "LATERAL VIEW explode(filter(array(" in sql and "), x -> x.keep)) rows AS r" in sql
        assert "SELECT r.map_id, r.source_key, r.op, seq, r.row_json FROM typed" in sql

    def test_a_unique_leaf_matches_the_table_by_leaf_or_full_name(self):
        sql = TripleStreamEnvelope.explode_sql(_by_table(_CUSTOMERS), {}, "records")

        assert "(src = 'cat.sch.customers' OR element_at(split(src, '[.]'), -1) = 'customers')" in sql

    def test_an_ambiguous_leaf_matches_by_full_name_only(self):
        other = _mapping("other.sch.customers", "http://ex.org/Account", uri_template="http://ex.org/Account/{id}")
        sql = TripleStreamEnvelope.explode_sql(_by_table(_CUSTOMERS, other), {}, "records")

        assert "src = 'cat.sch.customers' AND" in sql and "src = 'other.sch.customers' AND" in sql
        assert "element_at" not in sql

    def test_the_images_are_typed_by_the_described_columns_and_cast_to_text(self):
        types = {"cat.sch.customers": {"id": "bigint", "name": "string", "score": "double"}}
        sql = TripleStreamEnvelope.explode_sql(_by_table(_CUSTOMERS), types, "records")

        assert "from_json(after, '`id` bigint, `name` string, `score` double') AS a0" in sql
        assert "from_json(before, '`id` bigint, `name` string, `score` double') AS b0" in sql
        assert "to_json(struct(CAST(a0.`id` AS STRING) AS `id`, CAST(a0.`name` AS STRING) AS `name`, CAST(a0.`score` AS STRING) AS `score`))" in sql

    def test_a_column_the_types_do_not_list_is_a_string(self):
        sql = TripleStreamEnvelope.explode_sql(_by_table(_CUSTOMERS), {"cat.sch.customers": {"name": "string"}}, "records")

        assert "from_json(after, '`name` string, `id` STRING') AS a0" in sql

    def test_an_upsert_keeps_the_after_image_with_the_compiler_key_cast(self):
        sql = TripleStreamEnvelope.explode_sql(_by_table(_CUSTOMERS), {}, "records")

        assert (
            "'source_key', CAST(a0.`id` AS STRING), 'op', 'upsert', 'row_json', to_json(struct(CAST(a0.`id` AS STRING) AS `id`"
        ) in sql
        assert "AND op IN ('c', 'u', 'r') AND a0 IS NOT NULL AND CAST(a0.`id` AS STRING) IS NOT NULL))" in sql

    def test_a_composite_key_is_concatenated_in_template_order(self):
        composite = _mapping(
            "cat.sch.orders", "http://ex.org/Order", uri_template="http://ex.org/Order/{region}/{nr}", id_column="nr"
        )
        sql = TripleStreamEnvelope.explode_sql(_by_table(composite), {}, "records")

        assert "concat_ws('|', CAST(a0.`region` AS STRING), CAST(a0.`nr` AS STRING))" in sql

    def test_a_delete_tombstones_the_before_image(self):
        sql = TripleStreamEnvelope.explode_sql(_by_table(_CUSTOMERS), {}, "records")

        assert "'source_key', CAST(b0.`id` AS STRING), 'op', 'delete', 'row_json', to_json(struct(CAST(b0.`id` AS STRING) AS `id`" in sql
        assert "AND op IN ('d') AND b0 IS NOT NULL AND CAST(b0.`id` AS STRING) IS NOT NULL))" in sql

    def test_an_update_that_changes_the_key_also_tombstones_the_old_key(self):
        sql = TripleStreamEnvelope.explode_sql(_by_table(_CUSTOMERS), {}, "records")

        assert (
            "AND op IN ('c', 'u', 'r') AND a0 IS NOT NULL AND b0 IS NOT NULL AND CAST(b0.`id` AS STRING) IS NOT NULL "
            "AND CAST(b0.`id` AS STRING) <> coalesce(CAST(a0.`id` AS STRING), '')))"
        ) in sql

    def test_every_mapping_gets_three_candidates(self):
        second = _mapping("cat.sch.customers", "http://ex.org/Person", uri_template="http://ex.org/Person/{id}")
        by_table = _by_table(_CUSTOMERS, second)
        sql = TripleStreamEnvelope.explode_sql(by_table, {}, "records")

        [projectors] = by_table.values()
        assert sql.count("named_struct(") == 3 * len(projectors) == 6
        for projector in projectors:
            assert sql.count(f"named_struct('map_id', '{projector.map_id}'") == 3

    def test_two_tables_are_typed_separately_in_the_one_pass(self):
        orders = _mapping("cat.sch.orders", "http://ex.org/Order", uri_template="http://ex.org/Order/{id}")
        sql = TripleStreamEnvelope.explode_sql(_by_table(_CUSTOMERS, orders), {}, "records")

        assert "AS a0, from_json(before" in sql and "AS a1, from_json(before" in sql
        assert sql.count("FROM env") == 1

    def test_without_a_row_local_mapping_the_sql_yields_no_rows(self):
        sql = TripleStreamEnvelope.explode_sql({}, {}, "records")

        assert sql.endswith("FROM env WHERE false")
        assert "map_id" in sql and "row_json" in sql
