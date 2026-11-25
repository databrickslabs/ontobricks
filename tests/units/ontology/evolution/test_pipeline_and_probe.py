"""Pipeline stages, warehouse probe SQL, and remaining coverage branches."""

from __future__ import annotations

import copy

import pytest

from back.core.errors import ConflictError, ValidationError
from back.objects.ontology.evolution import (
    CoverageService,
    EvolutionPipeline,
    EvolutionService,
    Intent,
    OntologyIndex,
    ProposalBuilder,
    ProposalValidator,
    SourceAccessDenied,
    SourceUnavailable,
    WarehouseSourceProbe,
)
from back.objects.ontology.evolution.WarehouseSourceProbe import quote_column, split_fqn
from tests.fixtures.evolution.memory_probe import MemorySourceProbe
from tests.fixtures.evolution.memory_registry import FakeRegistry
from tests.fixtures.evolution.variants import NS, variant_document

pytestmark = pytest.mark.unit

ACME = {
    "output_class": "Order",
    "mentions": ["orders", "products", "Acme"],
    "filters": [{"class": "Supplier", "attribute": "supplierName", "value": "Acme"}],
}


def _pipeline(variant="base"):
    registry = FakeRegistry({"1": variant_document(variant)})
    svc = EvolutionService(registry, "EvoSupplyChain")
    return EvolutionPipeline(svc, MemorySourceProbe()), svc, registry


def _run(svc, intent=ACME, key="k"):
    return svc.create_run("1", intent, question="q", owner="o", idempotency_key=key)


class TestPipeline:
    def test_assess_then_propose(self):
        pipe, svc, _ = _pipeline()
        run = pipe.assess(*_ids(_run(svc)))
        assert (
            run["status"] == "ASSESSED"
            and run["coverage"]["decision_hint"] == "PROPOSE"
        )
        run = pipe.propose(*_ids(run))
        assert run["status"] == "PROPOSED"
        assert run["proposal"]["validation"]["passed"] is True
        assert len(run["candidate"]["design_hash"]) == 64

    def test_propose_requires_coverage(self):
        pipe, svc, _ = _pipeline()
        with pytest.raises(ValidationError):
            pipe.propose(*_ids(_run(svc)))

    def test_nothing_to_propose_for_a_supported_question(self):
        pipe, svc, _ = _pipeline()
        run = pipe.assess(*_ids(_run(svc, {**ACME, "output_class": "Product"})))
        with pytest.raises(ValidationError):
            pipe.propose(*_ids(run))

    def test_manual_ops_are_validated_and_bad_ops_reported(self):
        pipe, svc, _ = _pipeline()
        run = pipe.assess(*_ids(_run(svc)))
        bad = [
            {
                "op": "add_relationship",
                "target": {"name": "x"},
                "params": {"uri": "u:x", "domain": "OrderLine", "range": "Nope"},
            }
        ]
        run = pipe.propose(run["id"], run["revision"], bad)
        assert run["proposal"]["source"] == "manual"
        assert run["proposal"]["validation"]["passed"] is False
        assert run["candidate"] == {}

    def test_stage_refuses_a_changed_parent(self):
        pipe, svc, registry = _pipeline()
        run = _run(svc)
        registry.documents["1"]["versions"]["1"]["ontology"]["classes"].pop()
        with pytest.raises(ConflictError):
            pipe.assess(*_ids(run))

    def test_stage_refuses_a_cancelled_run(self):
        pipe, svc, _ = _pipeline()
        run = svc.update(*_ids(_run(svc)), {"status": "RUNNING"})
        run = svc.cancel(*_ids(run))
        with pytest.raises(ConflictError):
            pipe.assess(*_ids(run))

    def test_parent_document_is_never_written(self):
        pipe, svc, registry = _pipeline()
        before = copy.deepcopy(registry.documents)
        run = pipe.assess(*_ids(_run(svc)))
        pipe.propose(*_ids(run))
        assert registry.documents == before


def _ids(run):
    return run["id"], run["revision"]


class FakeExec:
    def __init__(self, responses=None, error=None):
        self.sql = []
        self.responses = list(responses or [])
        self.error = error

    def __call__(self, sql):
        self.sql.append(" ".join(sql.split()))
        if self.error:
            raise self.error
        return self.responses.pop(0) if self.responses else []


T = "c.s.order_lines"
P = "c.s.products"


class TestWarehouseProbe:
    def test_identifiers_are_validated(self):
        assert split_fqn("c.s.t") == ["c", "s", "t"]
        for bad in ("c.s", "c.s.t;drop", "c.`s`.t", "a.b.c.d"):
            with pytest.raises(ValidationError):
                split_fqn(bad)
        with pytest.raises(ValidationError):
            quote_column("x y")

    def test_columns_checks_access_then_reads_information_schema(self):
        ex = FakeExec([[], [{"column_name": "line_id", "data_type": "string"}]])
        assert WarehouseSourceProbe(ex).columns(T) == {"line_id": "string"}
        assert ex.sql[0] == "SELECT 1 FROM `c`.`s`.`order_lines` LIMIT 0"
        assert "table_schema = 's' AND table_name = 'order_lines'" in ex.sql[1]

    def test_value_is_a_quoted_literal(self):
        ex = FakeExec([[{"n": 0}]])
        WarehouseSourceProbe(ex).count_value(T, "line_id", "x'; DROP TABLE t; --")
        assert "= 'x''; DROP TABLE t; --'" in ex.sql[0]

    def test_foreign_keys_and_profiles(self):
        ex = FakeExec(
            [
                [
                    {
                        "column_name": "product_id",
                        "ref_catalog": "c",
                        "ref_schema": "s",
                        "ref_table": "products",
                        "ref_column": "product_id",
                    }
                ],
                [{"n": 3, "d": 3}],
                [{"source_rows": 8, "matched": 8, "target_column_unique": True}],
                [{"rows": 8, "dangling_source": 0, "dangling_target": 1}],
            ]
        )
        probe = WarehouseSourceProbe(ex)
        assert probe.foreign_keys(T) == [
            {"column": "product_id", "ref_table": P, "ref_column": "product_id"}
        ]
        assert probe.key_is_unique(P, "product_id") is True
        assert probe.join_profile(T, "product_id", P, "product_id")["matched"] == 8
        out = probe.mapping_profile(
            "SELECT line_id, product_id FROM c.s.order_lines",
            "line_id",
            "product_id",
            {"table": T, "id_column": "line_id"},
            {"table": P, "id_column": "product_id"},
        )
        assert out == {"rows": 8, "dangling_source": 0, "dangling_target": 1}
        assert ex.sql[-1].startswith(
            "WITH m AS (SELECT line_id, product_id FROM c.s.order_lines)"
        )

    @pytest.mark.parametrize("sql", ["DELETE FROM t", "SELECT 1; DROP TABLE t"])
    def test_mapping_sql_must_be_one_select(self, sql):
        with pytest.raises(ValidationError):
            WarehouseSourceProbe(FakeExec()).mapping_profile(
                sql,
                "a",
                "b",
                {"table": T, "id_column": "a"},
                {"table": P, "id_column": "b"},
            )

    @pytest.mark.parametrize(
        "message,exc",
        [
            ("[INSUFFICIENT_PERMISSIONS] no", SourceAccessDenied),
            ("[TABLE_OR_VIEW_NOT_FOUND] x", SourceUnavailable),
            ("socket closed", SourceUnavailable),
        ],
    )
    def test_errors_are_classified(self, message, exc):
        with pytest.raises(exc):
            WarehouseSourceProbe(FakeExec(error=RuntimeError(message))).key_is_unique(
                T, "line_id"
            )


def _index(variant="base", mutate=None):
    doc = variant_document(variant)
    sl = doc["versions"]["1"]
    if mutate:
        mutate(sl)
    return OntologyIndex(sl["ontology"], sl["assignment"])


def _assess(intent, index=None):
    return CoverageService(index or _index(), MemorySourceProbe()).assess(
        Intent.from_dict(intent)
    )


class TestCoverageBranches:
    def test_output_class_from_an_unambiguous_mention(self):
        report = _assess({"output_class": None, "mentions": ["vendors"], "filters": []})
        assert report["states"] == {}
        assert report["ready"] is False

    def test_unknown_output_class(self):
        report = _assess({**ACME, "output_class": "Invoice"})
        assert report["states"]["class:Invoice"] == "MISSING_SEMANTICS"

    def test_no_output_class_at_all(self):
        report = _assess({"output_class": None, "mentions": ["xyz"], "filters": []})
        assert report["states"]["output"] == "MISSING_SEMANTICS"

    def test_unknown_selected_attribute(self):
        report = _assess({**ACME, "select": ["Product.unitCost"]})
        assert report["states"]["attribute:Product.unitCost"] == "MISSING_SEMANTICS"

    def test_unknown_and_unmapped_filter_attributes(self):
        report = _assess(
            {
                **ACME,
                "filters": [{"class": "Supplier", "attribute": "rating", "value": "A"}],
            }
        )
        assert report["states"]["attribute:Supplier.rating"] == "MISSING_SEMANTICS"

        def drop_mapping(sl):
            sl["assignment"]["entities"][3]["attribute_mappings"] = {}

        report = _assess(ACME, _index(mutate=drop_mapping))
        assert (
            report["states"]["filter:Supplier.supplierName=Acme"] == "MISSING_SEMANTICS"
        )

    def test_unmapped_class(self):
        def drop_supplier(sl):
            sl["assignment"]["entities"] = sl["assignment"]["entities"][:3]

        report = _assess(
            {**ACME, "output_class": "Product"}, _index(mutate=drop_supplier)
        )
        assert report["states"]["entity:Supplier"] == "MISSING_SEMANTICS"

    def test_relationship_without_mapping_proposes_a_mapping(self):
        def unmap(sl):
            sl["assignment"]["relationships"] = sl["assignment"]["relationships"][:1]

        report = _assess({**ACME, "output_class": "Product"}, _index(mutate=unmap))
        assert report["states"]["mapping:suppliedBy"] == "MISSING_SEMANTICS"
        assert report["gap"] == {"kind": "mapping", "property": "suppliedBy"}
        proposal = ProposalBuilder(_index(mutate=unmap)).build(report)
        assert [op["op"] for op in proposal["alternatives"][0]["operations"]] == [
            "set_relationship_mapping"
        ]

    def test_two_paths_are_ambiguous_and_a_hint_picks_one(self):
        def second_path(sl):
            sl["ontology"]["properties"].append(
                {
                    "uri": NS + "altSupplied",
                    "name": "altSupplied",
                    "type": "ObjectProperty",
                    "domain": "Product",
                    "range": "Supplier",
                }
            )

        intent = {**ACME, "output_class": "Product"}
        assert (
            _assess(intent, _index(mutate=second_path))["states"][
                "path:Product->Supplier"
            ]
            == "AMBIGUOUS"
        )
        hinted = _assess(
            {**intent, "path_hint": ["suppliedBy"]}, _index(mutate=second_path)
        )
        assert hinted["ready"] is True

    def test_disconnected_classes_without_join_evidence(self):
        def isolate(sl):
            # An unmapped class: no table, so no join can be evidenced.
            sl["ontology"]["classes"].append(
                {"uri": NS + "Warehouse", "name": "Warehouse", "label": "Warehouse"}
            )

        intent = {
            "output_class": "Order",
            "filters": [{"class": "Warehouse", "attribute": "id", "value": "W1"}],
        }
        report = _assess(intent, _index(mutate=isolate))
        assert report["decision_hint"] == "ABSTAIN"
        assert report["states"]["relationship:Order->Warehouse"] == "MISSING_SEMANTICS"
        assert report["gap"] == {
            "kind": "relationship",
            "source": "Order",
            "target": "Warehouse",
        }


class TestBuilderAndValidatorBranches:
    def test_value_overlap_join_builds_a_joined_mapping(self):
        chosen = {
            "source_class": "OrderLine",
            "target_class": "Product",
            "source_table": "cat.fix.order_lines",
            "source_id_column": "line_id",
            "source_column": "alt_product_ref",
            "target_table": "cat.fix.products",
            "target_column": "sku",
            "target_id_column": "product_id",
            "evidence": ["value_overlap"],
        }
        report = {
            "decision_hint": "PROPOSE",
            "gap": {"kind": "relationship"},
            "join": {"chosen": chosen},
        }
        ops = ProposalBuilder(_index()).build(report)["alternatives"][0]["operations"]
        assert ops[1]["params"]["sql_query"] == (
            "SELECT s.line_id AS line_id, t.product_id AS product_id FROM cat.fix.order_lines s "
            "JOIN cat.fix.products t ON s.alt_product_ref = t.sku"
        )
        assert ops[1]["params"]["target_id_column"] == "product_id"
        assert MemorySourceProbe().run_pairs(ops[1]["params"]["sql_query"])[0] == (
            "L1",
            "P1",
        )

    def test_name_clash_gets_a_suffix(self):
        def taken(sl):
            sl["ontology"]["properties"].append(
                {
                    "name": "hasProduct",
                    "type": "ObjectProperty",
                    "domain": "Order",
                    "range": "Product",
                }
            )

        builder = ProposalBuilder(_index(mutate=taken))
        assert builder._relationship_name("OrderLine", "Product") == "hasProduct2"

    def test_class_gap_has_no_proposal(self):
        assert (
            ProposalBuilder(_index()).build(
                {
                    "decision_hint": "PROPOSE",
                    "gap": {"kind": "class"},
                    "join": {"chosen": {}},
                }
            )
            is None
        )

    @pytest.mark.parametrize(
        "exc,reason",
        [
            (SourceAccessDenied("x"), "source_access_denied"),
            (SourceUnavailable("x"), "source_unavailable"),
        ],
    )
    def test_validator_reports_source_errors(self, exc, reason):
        class Failing(MemorySourceProbe):
            def columns(self, table):
                raise exc

        pipe, svc, _ = _pipeline()
        run = pipe.propose(*_ids(pipe.assess(*_ids(_run(svc)))))
        ops = run["proposal"]["alternatives"][0]["operations"]
        result = ProposalValidator(Failing()).validate(ops, run["candidate"])
        assert reason in result["failures"]


class TestWarehouseVersionsAndGold:
    def test_table_version_reads_delta_history(self):
        ex = FakeExec([[{"version": 7, "operation": "WRITE"}]])
        assert WarehouseSourceProbe(ex).table_version(T) == 7
        assert ex.sql[0] == "DESCRIBE HISTORY `c`.`s`.`order_lines` LIMIT 1"

    def test_table_without_history_is_unavailable(self):
        with pytest.raises(SourceUnavailable):
            WarehouseSourceProbe(FakeExec([[]])).table_version(T)

    @pytest.mark.parametrize(
        "ids", [["O3", "O1"], '["O3", "O1"]', __import__("numpy").array(["O3", "O1"])]
    )
    def test_gold_ids_bind_the_question_and_sort(self, ids):
        ex = FakeExec([[{"expected_ids": ids}]])
        out = WarehouseSourceProbe(ex).gold_ids("c.s.gold", "acme'; --")
        assert out == ["O1", "O3"]
        assert "question_id = 'acme''; --'" in ex.sql[0]

    @pytest.mark.parametrize("rows", [[], [{"expected_ids": []}, {"expected_ids": []}]])
    def test_missing_or_duplicate_gold_question(self, rows):
        from back.core.errors import NotFoundError

        with pytest.raises(NotFoundError):
            WarehouseSourceProbe(FakeExec([rows])).gold_ids("c.s.gold", "q")


class TestViewsHaveNoVersion:
    def _probe(self, table_type):
        calls = []

        def execute(sql):
            calls.append(sql)
            if sql.startswith("DESCRIBE HISTORY"):
                raise RuntimeError(
                    "[DELTA_ONLY_OPERATION] DESCRIBE HISTORY is only supported for Delta tables"
                )
            return [{"table_type": table_type}] if table_type else []

        return WarehouseSourceProbe(execute), calls

    def test_view_is_unpinned(self):
        probe, calls = self._probe("VIEW")
        assert probe.table_version(T) is None
        assert "information_schema.tables" in calls[1]

    @pytest.mark.parametrize("table_type", ["MANAGED", ""])
    def test_a_table_without_history_still_fails(self, table_type):
        probe, _ = self._probe(table_type)
        with pytest.raises(SourceUnavailable):
            probe.table_version(T)
