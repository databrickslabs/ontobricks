"""TwinMapping: SELECT column extraction, VIEW error diagnosis, entity /
relationship mapping augmentation from config, and SPARQL-on-Spark."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import InfrastructureError, ValidationError
from back.objects.digitaltwin.query.TwinMapping import TwinMapping

pytestmark = pytest.mark.unit

BASE = "http://ex.org/onto#"
NB = "http://ex.org/onto/"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"


# ---------------------------------------------------------------------------
# _extract_select_columns
# ---------------------------------------------------------------------------


class TestExtractSelectColumns:
    def test_unparseable_query_returns_none(self):
        assert TwinMapping._extract_select_columns("SHOW TABLES") is None

    def test_function_call_commas_do_not_split_columns(self):
        sql = "SELECT concat(a, '-', b) AS full_id, coalesce(x, y) z FROM t"
        assert TwinMapping._extract_select_columns(sql) == {"full_id", "z"}

    def test_qualified_column_keeps_local_name(self):
        assert TwinMapping._extract_select_columns("SELECT t.id FROM t") == {"id"}

    def test_star_is_not_reliable(self):
        assert TwinMapping._extract_select_columns("SELECT * FROM t") is None


# ---------------------------------------------------------------------------
# diagnose_view_error
# ---------------------------------------------------------------------------


_ENTITY = {
    f"{NB}Customer": {
        "sql_query": "SELECT cid, cname, email FROM c",
        "id_column": "cid",
        "label_column": "cname",
        "predicates": {f"{NB}email": {"column": "email"}},
    }
}


def _unresolved(col, suggestions=""):
    msg = f"[UNRESOLVED_COLUMN] A column with name `{col}` cannot be resolved."
    if suggestions:
        msg += f" Did you mean one of the following? [{suggestions}]"
    return msg


class TestDiagnoseViewError:
    def test_label_column_is_attributed_with_suggestions(self):
        out = TwinMapping.diagnose_view_error(_unresolved("cname", "`cid`, `name`"), _ENTITY)
        assert "Role: label_column" in out
        assert "Entity: Customer" in out
        assert "Available columns: `cid`, `name`" in out

    def test_attribute_column_is_attributed(self):
        out = TwinMapping.diagnose_view_error(_unresolved("email"), _ENTITY)
        assert "Attribute: email" in out
        assert "Available columns" not in out

    def test_relationship_column_is_attributed(self):
        rels = [{"property": "hasOrder", "sql_query": "SELECT a, b FROM r",
                 "source_column": "a", "target_column": "order_fk"}]
        out = TwinMapping.diagnose_view_error(
            "Column 'order_fk' does not exist", {}, rels
        )
        assert "relationship 'hasOrder'" in out
        assert "Role: target_column" in out

    def test_unknown_column_points_to_server_logs(self):
        out = TwinMapping.diagnose_view_error(_unresolved("ghost", "`a`"), _ENTITY, [])
        assert "Column 'ghost' not found in any source table." in out
        assert "Available columns: `a`" in out


# ---------------------------------------------------------------------------
# augment_mappings_from_config
# ---------------------------------------------------------------------------


def _entity_cfg(**over):
    base = {
        "ontology_class": f"{NB}Customer",
        "ontology_class_label": "Customer",
        "sql_query": "SELECT cid, cname, email, city AS town FROM c",
        "id_column": "cid",
        "label_column": "cname",
        "attribute_mappings": {"email": "email", "town": "town"},
    }
    base.update(over)
    return base


class TestAugmentEntityMappings:
    def test_empty_config_returns_input_untouched(self):
        mappings = {"x": {}}
        assert TwinMapping.augment_mappings_from_config(mappings, {}, BASE) is mappings

    def test_new_entity_is_created_with_label_and_attributes(self):
        out = TwinMapping.augment_mappings_from_config(
            {}, {"entities": [_entity_cfg()]}, BASE
        )
        m = out[f"{NB}Customer"]
        assert m["uri_template"].split("{")[0] == f"{NB}Customer/"
        assert m["predicates"][RDFS_LABEL] == {"type": "column", "column": "cname"}
        assert m["predicates"][f"{NB}email"]["column"] == "email"
        assert m["predicates"][f"{NB}town"]["column"] == "town"

    def test_user_defined_mapping_is_not_overwritten(self):
        user_label = {"type": "column", "column": "display_name"}
        existing = {
            f"{NB}Customer": {
                "table": None,
                "sql_query": "SELECT cid, display_name, cname, email, town FROM v",
                "id_column": "cid",
                "label_column": "display_name",
                "uri_template": f"{NB}Client/{{cid}}",
                "predicates": {RDFS_LABEL: user_label},
            }
        }
        out = TwinMapping.augment_mappings_from_config(
            existing, {"entities": [_entity_cfg()]}, BASE
        )
        m = out[f"{NB}Customer"]
        assert m["sql_query"] == "SELECT cid, display_name, cname, email, town FROM v"
        assert m["label_column"] == "display_name"
        assert m["uri_template"] == f"{NB}Client/{{cid}}"
        assert m["predicates"][RDFS_LABEL] == user_label
        assert m["predicates"][f"{NB}email"]["column"] == "email"

    def test_missing_query_and_label_on_existing_mapping_are_filled(self):
        existing = {f"{NB}Customer": {"id_column": "cid", "predicates": {}}}
        out = TwinMapping.augment_mappings_from_config(
            existing, {"entities": [_entity_cfg()]}, BASE
        )
        m = out[f"{NB}Customer"]
        assert m["sql_query"].startswith("SELECT cid")
        assert m["label_column"] == "cname"

    def test_aliased_away_attribute_column_is_skipped(self):
        cfg = _entity_cfg(attribute_mappings={"city": "city", "blank": ""})
        out = TwinMapping.augment_mappings_from_config({}, {"entities": [cfg]}, BASE)
        preds = out[f"{NB}Customer"]["predicates"]
        assert f"{NB}city" not in preds
        assert f"{NB}blank" not in preds

    def test_final_pass_drops_r2rml_predicates_not_in_select(self):
        existing = {
            f"{NB}Order": {
                "sql_query": "SELECT oid, total FROM o",
                "id_column": "oid",
                "predicates": {
                    f"{NB}total": {"type": "column", "column": "total"},
                    f"{NB}raw": {"type": "column", "column": "raw_col"},
                    f"{NB}const": {"type": "constant", "value": "x"},
                },
            }
        }
        out = TwinMapping.augment_mappings_from_config(
            existing, {"entities": []}, BASE
        )
        preds = out[f"{NB}Order"]["predicates"]
        assert set(preds) == {f"{NB}total", f"{NB}const"}

    def test_excluded_and_incomplete_entities_are_skipped(self):
        cfg = {
            "data_source_mappings": [
                _entity_cfg(excluded=True),
                _entity_cfg(ontology_class=f"{NB}Ghost", sql_query=""),
            ]
        }
        assert TwinMapping.augment_mappings_from_config({}, cfg, BASE) == {}

    def test_relative_class_uri_is_resolved_against_base(self):
        cfg = _entity_cfg(ontology_class="Customer", ontology_class_label="")
        out = TwinMapping.augment_mappings_from_config({}, {"entities": [cfg]}, BASE)
        assert f"{NB}Customer" in out
        assert out[f"{NB}Customer"]["uri_template"].split("{")[0] == f"{NB}Customer/"

    def test_new_entity_uri_template_is_well_formed(self):
        out = TwinMapping.augment_mappings_from_config(
            {}, {"entities": [_entity_cfg()]}, BASE
        )
        assert out[f"{NB}Customer"]["uri_template"] == f"{NB}Customer/{{cid}}"


# ---------------------------------------------------------------------------
# augment_relationships_from_config
# ---------------------------------------------------------------------------


_ENTITIES = [
    {"ontology_class": f"{NB}Customer", "ontology_class_label": "Customer",
     "id_column": "cid"},
    {"ontology_class": f"{NB}Order", "ontology_class_label": "Sales Order",
     "id_column": "oid"},
]


def _rel(**over):
    base = {
        "property": f"{NB}placed",
        "sql_query": "SELECT cid, oid FROM o",
        "source_class": f"{NB}Customer",
        "target_class": f"{NB}Order",
        "source_id_column": "cid",
        "target_id_column": "oid",
    }
    base.update(over)
    return base


def _aug_rels(rels, existing=None, ontology=None, entities=_ENTITIES):
    return TwinMapping.augment_relationships_from_config(
        existing if existing is not None else [],
        {"entities": entities, "relationships": rels},
        BASE,
        ontology,
    )


class TestAugmentRelationships:
    def test_empty_config_returns_input(self):
        rels = [{"x": 1}]
        assert TwinMapping.augment_relationships_from_config(rels, None, BASE) is rels

    def test_new_relationship_uses_entity_uri_templates(self):
        out = _aug_rels([_rel()])
        assert out == [{
            "predicate": f"{NB}placed",
            "sql_query": "SELECT cid, oid FROM o",
            "subject_template": f"{NB}Customer/{{cid}}",
            "object_template": f"{NB}Sales_Order/{{oid}}",
            "subject_column": "cid",
            "object_column": "oid",
        }]

    def test_incomplete_relationships_are_skipped(self):
        assert _aug_rels([_rel(sql_query=""), _rel(target_id_column="")]) == []

    def test_explicitly_excluded_property_is_skipped(self):
        assert _aug_rels([_rel(excluded=True)]) == []

    def test_properties_touching_an_excluded_class_are_skipped(self):
        entities = [dict(_ENTITIES[0], excluded=True), _ENTITIES[1]]
        ontology = {
            "classes": [{"uri": f"{NB}Customer", "name": "Customer"}],
            "properties": [
                {"uri": f"{NB}placed", "domain": "Customer", "range": "Order"},
                {"domain": "Customer", "range": "Order"},
            ],
        }
        assert _aug_rels([_rel()], ontology=ontology, entities=entities) == []

    @pytest.mark.parametrize(
        "over, expected",
        [
            ({"property": "http://other.org/vocab#buys"}, f"{NB}buys"),
            ({"property": "has order"}, f"{NB}has_order"),
            ({"property": "", "property_label": "made by"}, f"{NB}made_by"),
            ({"property": ""}, f"{NB}relatesTo"),
        ],
    )
    def test_predicate_uri_is_normalised_onto_the_base(self, over, expected):
        assert _aug_rels([_rel(**over)])[0]["predicate"] == expected

    def test_labels_fall_back_to_domain_and_range_respecting_direction(self):
        rel = _rel(source_class="", target_class="", domain=f"{NB}Customer",
                   range=f"{NB}Order", direction="reverse",
                   source_id_column="oid", target_id_column="cid")
        out = _aug_rels([rel])[0]
        assert out["subject_template"] == f"{NB}Sales_Order/{{oid}}"
        assert out["object_template"] == f"{NB}Customer/{{cid}}"

    def test_labels_fall_back_to_ontology_property_domain_and_range(self):
        ontology = {
            "classes": [
                {"uri": f"{NB}Customer", "name": "Customer", "label": "Customer"},
                {"uri": f"{NB}Order", "name": "Order", "label": "Sales Order"},
            ],
            "properties": [
                {"uri": f"{NB}placed", "label": "placed",
                 "domain": f"{NB}Customer", "range": "Order"},
            ],
        }
        rel = _rel(source_class="", target_class="")
        out = _aug_rels([rel], ontology=ontology)[0]
        assert out["subject_template"] == f"{NB}Customer/{{cid}}"
        assert out["object_template"] == f"{NB}Sales_Order/{{oid}}"

    def test_unknown_classes_get_a_label_based_or_generic_base(self):
        rel = _rel(source_class="", target_class="", source_class_label="Shop Front")
        out = _aug_rels([rel], entities=[])[0]
        assert out["subject_template"] == f"{NB}Shop_Front/{{cid}}"
        assert out["object_template"] == f"{NB}Entity/{{oid}}"

    def test_existing_placeholder_templates_are_repaired(self):
        existing = [{
            "predicate": f"{NB}placed",
            "sql_query": "SELECT cid, oid FROM o",
            "subject_template": f"{NB}Source/{{cid}}",
            "object_template": f"{NB}UnknownEntity/{{oid}}",
        }]
        out = _aug_rels([_rel()], existing=existing)
        assert len(out) == 1
        assert out[0]["subject_template"] == f"{NB}Customer/{{cid}}"
        assert out[0]["object_template"] == f"{NB}Sales_Order/{{oid}}"

    def test_existing_user_templates_are_not_overwritten(self):
        existing = [{
            "predicate": f"{NB}placed",
            "sql_query": "SELECT cid, oid FROM o",
            "subject_template": f"{NB}Client/{{cid}}",
            "object_template": f"{NB}Purchase/{{oid}}",
        }]
        out = _aug_rels([_rel()], existing=existing)
        assert len(out) == 1
        assert out[0]["subject_template"] == f"{NB}Client/{{cid}}"
        assert out[0]["object_template"] == f"{NB}Purchase/{{oid}}"


# ---------------------------------------------------------------------------
# execute_spark_query
# ---------------------------------------------------------------------------


def _client(host="https://h", warehouse_id="wh", auth=True, rows=None, exc=None):
    c = MagicMock()
    c.host = host
    c.warehouse_id = warehouse_id
    c.has_valid_auth.return_value = auth
    if exc:
        c.execute_query.side_effect = exc
    else:
        c.execute_query.return_value = rows
    return c


async def _run_blocking(fn, *args, **kwargs):
    return fn(*args, **kwargs)


@contextmanager
def _spark(client, *, mappings=None, translation=None, extract_exc=None):
    ents = {f"{NB}Customer": {"table": "c.s.customers", "predicates": {}}}
    if mappings is None:
        mappings = (ents, [])
    with patch("back.core.helpers.get_data_plane_client", return_value=client), patch(
        "back.core.helpers.run_blocking", _run_blocking
    ), patch(
        "back.core.w3c.sparql.extract_r2rml_mappings",
        return_value=mappings,
        side_effect=extract_exc,
    ), patch(
        "back.core.w3c.sparql.translate_sparql_to_spark",
        return_value=translation
        or {"success": True, "sql": "SELECT 1", "variables": ["s"]},
    ) as translate:
        yield translate


def _mapping_domain():
    return SimpleNamespace(ontology={"base_uri": BASE}, assignment={})


async def _execute(limit=10):
    return await TwinMapping(_mapping_domain()).execute_spark_query(
        "SELECT ?s WHERE { ?s ?p ?o }", "r2rml", limit, MagicMock()
    )


class TestExecuteSparkQuery:
    async def test_results_are_returned_with_generated_sql_and_tables(self):
        rows = [{"s": "http://ex.org/1"}]
        with _spark(_client(rows=rows)) as translate:
            out = await _execute(limit=25)
        assert out["results"] == rows
        assert out["columns"] == ["s"]
        assert out["count"] == 1
        assert out["engine"] == "spark"
        assert out["generated_sql"] == "SELECT 1"
        assert out["tables_queried"] == ["c.s.customers"]
        assert translate.call_args.args[2] == 25

    async def test_columns_default_to_row_keys_without_select_vars(self):
        with _spark(
            _client(rows=[{"a": 1, "b": 2}]),
            translation={"success": True, "sql": "S", "variables": []},
        ):
            out = await _execute()
        assert out["columns"] == ["a", "b"]

    async def test_empty_results(self):
        with _spark(_client(rows=[])):
            out = await _execute()
        assert out == {"success": True, "results": [], "columns": ["s"], "count": 0,
                       "engine": "spark", "generated_sql": "SELECT 1"}

    @pytest.mark.parametrize(
        "client, match",
        [
            (None, "not configured"),
            (_client(warehouse_id=""), "No SQL warehouse"),
            (_client(host=""), "Missing: host"),
            (_client(auth=False), "authentication"),
        ],
    )
    async def test_unusable_client_is_a_validation_error(self, client, match):
        with _spark(client):
            with pytest.raises(ValidationError, match=match):
                await _execute()

    async def test_no_mappings_is_a_validation_error(self):
        with _spark(_client(), mappings=({}, [])):
            with pytest.raises(ValidationError, match="No valid R2RML"):
                await _execute()

    async def test_translation_failure_surfaces_message(self):
        with _spark(_client(), translation={"success": False, "message": "bad SPARQL"}):
            with pytest.raises(ValidationError, match="bad SPARQL"):
                await _execute()

    async def test_connection_error_is_reported_as_connection_failure(self):
        with _spark(_client(exc=RuntimeError("'NoneType' object has no attribute"))):
            with pytest.raises(InfrastructureError, match="connection failed"):
                await _execute()

    async def test_sql_error_is_reported_as_execution_failure(self):
        with _spark(_client(exc=RuntimeError("PARSE_SYNTAX_ERROR"))):
            with pytest.raises(InfrastructureError, match="Spark SQL execution failed"):
                await _execute()

    async def test_value_error_becomes_validation_error(self):
        with _spark(_client(), extract_exc=ValueError("bad turtle")):
            with pytest.raises(ValidationError, match="invalid"):
                await _execute()

    async def test_unexpected_error_becomes_infrastructure_error(self):
        with _spark(_client(), extract_exc=KeyError("x")):
            with pytest.raises(InfrastructureError, match="unexpected"):
                await _execute()
