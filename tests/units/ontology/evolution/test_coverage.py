"""Coverage, join evidence, proposals, candidate application and validation (offline)."""

from __future__ import annotations

import pytest

from back.objects.ontology.autofix_apply import apply_operations
from back.objects.ontology.evolution import (
    CandidateFactory,
    CoverageService,
    Intent,
    JoinCandidateFinder,
    OntologyIndex,
    ProposalBuilder,
    ProposalValidator,
)
from back.objects.ontology.evolution.OntologyIndex import normalise
from tests.fixtures.evolution.memory_probe import MemorySourceProbe, table_rows
from tests.fixtures.evolution.variants import variant_document

pytestmark = pytest.mark.unit

ACME_ORDERS = {
    "output_class": "Order",
    "mentions": ["orders", "products", "Acme"],
    "filters": [
        {"class": "Supplier", "attribute": "supplierName", "op": "eq", "value": "Acme"}
    ],
}


def _index(variant="base"):
    sl = variant_document(variant)["versions"]["1"]
    return OntologyIndex(sl["ontology"], sl["assignment"])


def _assess(intent, variant="base", denied=()):
    return CoverageService(_index(variant), MemorySourceProbe(denied=denied)).assess(
        Intent.from_dict(intent)
    )


def test_memory_probe_matches_gold():
    """The offline rows reproduce the hand-authored gold answer."""
    products = {
        p["product_id"] for p in table_rows("products") if p["supplier_id"] == "S1"
    }
    orders = sorted(
        {
            ol["order_id"]
            for ol in table_rows("order_lines")
            if ol["product_id"] in products
        }
    )
    assert orders == ["O1", "O3", "O5"]


class TestNormalise:
    @pytest.mark.parametrize(
        "text,expected",
        [
            ("OrderLine", "order line"),
            ("purchase orders", "purchase order"),
            ("items", "item"),
            ("address", "address"),
        ],
    )
    def test_forms(self, text, expected):
        assert normalise(text) == expected

    def test_mention_resolution_is_ambiguous_across_classes(self):
        assert _index().resolve_mention("items") == ["OrderLine", "Product"]
        assert _index().resolve_mention("vendors") == ["Supplier"]


class TestCoverage:
    def test_missing_relationship_is_named_with_declared_key_evidence(self):
        report = _assess(ACME_ORDERS)
        assert report["ready"] is False
        assert report["decision_hint"] == "PROPOSE"
        assert report["gap"] == {
            "kind": "relationship",
            "source": "OrderLine",
            "target": "Product",
        }
        chosen = report["join"]["chosen"]
        assert (chosen["source_column"], chosen["target_column"]) == (
            "product_id",
            "product_id",
        )
        assert "declared_fk" in chosen["evidence"]
        kinds = {e["type"] for e in report["evidence"]}
        assert {"declared_fk", "mapping_profile"} <= kinds

    def test_supported_question_is_ready_without_change(self):
        report = _assess({**ACME_ORDERS, "output_class": "Product"})
        assert report["ready"] is True and report["decision_hint"] == "NO_CHANGE"
        assert report["states"]["relationship:suppliedBy"] == "RESOLVED"

    def test_two_valid_joins_without_a_key_are_ambiguous(self):
        report = _assess(ACME_ORDERS, "nofk")
        assert report["states"]["relationship:OrderLine->Product"] == "AMBIGUOUS"
        assert report["decision_hint"] == "BLOCKED"
        assert len(report["join"]["candidates"]) == 2

    def test_duplicate_supplier_names_block_the_filter(self):
        report = _assess(
            {**ACME_ORDERS, "output_class": "Product"}, "ambiguous_supplier"
        )
        assert report["states"]["filter:Supplier.supplierName=Acme"] == "AMBIGUOUS"

    def test_non_label_attribute_may_match_many(self):
        intent = {
            "output_class": "OrderLine",
            "filters": [
                {"class": "Order", "attribute": "customerName", "value": "Northwind"}
            ],
        }
        assert _assess(intent)["ready"] is True

    def test_denied_source_is_inaccessible(self):
        report = _assess(
            {**ACME_ORDERS, "output_class": "Product"}, denied=["suppliers"]
        )
        assert report["states"]["source:suppliers"] == "INACCESSIBLE"
        assert report["decision_hint"] == "BLOCKED"

    def test_missing_table_is_inconclusive(self):
        report = _assess({**ACME_ORDERS, "output_class": "Product"}, "missing_source")
        assert report["states"]["source:suppliers_missing"] == "SOURCE_UNAVAILABLE"
        assert report["decision_hint"] == "INCONCLUSIVE"

    @pytest.mark.parametrize(
        "extra,key",
        [
            (
                {"aggregate": {"fn": "sum", "attribute": "OrderLine.quantity"}},
                "aggregate:sum",
            ),
            (
                {
                    "filters": [
                        {
                            "class": "Order",
                            "attribute": "orderDate",
                            "op": "gt",
                            "value": "2026-03-01",
                        }
                    ]
                },
                "filter:Order.orderDate>2026-03-01",
            ),
        ],
    )
    def test_unsupported_forms_block(self, extra, key):
        report = _assess({**ACME_ORDERS, **extra})
        assert report["states"][key] == "UNSUPPORTED"
        assert report["decision_hint"] == "BLOCKED"

    def test_path_beyond_budget_is_unsupported(self):
        intent = {
            "output_class": "Supplier",
            "filters": [
                {"class": "Order", "attribute": "customerName", "value": "Northwind"}
            ],
            "max_edges": 2,
        }
        report = _assess(intent)
        assert report["states"]["path:Supplier->Order"] == "UNSUPPORTED"

    def test_unknown_class_abstains(self):
        intent = {
            "output_class": "Order",
            "filters": [
                {"class": "Shipment", "attribute": "carrier", "value": "FedEx"}
            ],
        }
        report = _assess(intent)
        assert report["gap"] == {"kind": "class", "name": "Shipment"}
        assert report["decision_hint"] == "ABSTAIN"

    def test_broken_mapping_is_detected_by_data(self):
        report = _assess(ACME_ORDERS, "broken_mapping")
        assert report["states"]["mapping:lineProduct"] == "MISSING_SEMANTICS"
        assert report["gap"] == {"kind": "mapping", "property": "lineProduct"}

    def test_injection_value_is_bound_as_a_literal(self):
        captured = []

        class Spy(MemorySourceProbe):
            def count_value(self, table, column, value):
                captured.append(value)
                return 0

        intent = {
            **ACME_ORDERS,
            "filters": [
                {
                    "class": "Supplier",
                    "attribute": "supplierName",
                    "value": "x'; DROP TABLE t; --",
                }
            ],
        }
        CoverageService(_index(), Spy()).assess(Intent.from_dict(intent))
        assert captured == ["x'; DROP TABLE t; --"]


class TestJoinChoice:
    def _cand(self, evidence, valid=True):
        return {"evidence": evidence, "data_valid": valid}

    def test_single_declared_key_wins_over_valid_alternatives(self):
        fk = self._cand(["declared_fk"])
        chosen, ambiguous = JoinCandidateFinder.choose(
            [fk, self._cand(["value_overlap"])]
        )
        assert chosen is fk and not ambiguous

    def test_invalid_name_match_is_still_proposed_for_validation_to_block(self):
        named = self._cand(["name_match"], valid=False)
        assert JoinCandidateFinder.choose([named]) == (named, False)

    def test_nothing_to_choose(self):
        assert JoinCandidateFinder.choose([]) == (None, False)


class TestProposalAndCandidate:
    def _proposal(self, variant="base"):
        index = _index(variant)
        report = CoverageService(index, MemorySourceProbe()).assess(
            Intent.from_dict(ACME_ORDERS)
        )
        return ProposalBuilder(index).build(report), report

    def test_builds_two_typed_ops_with_server_built_sql(self):
        proposal, _ = self._proposal()
        add, mapping = proposal["alternatives"][0]["operations"]
        assert (
            add["op"] == "add_relationship" and add["params"]["domain"] == "OrderLine"
        )
        assert (
            add["params"]["uri"] == "http://ontobricks.local/evo/supplychain#hasProduct"
        )
        assert (
            mapping["params"]["sql_query"]
            == "SELECT line_id, product_id FROM cat.fix.order_lines"
        )

    def test_candidate_applies_ops_without_touching_the_parent(self):
        proposal, _ = self._proposal()
        parent = variant_document("base")
        before = repr(parent)
        candidate = CandidateFactory.build(
            parent, "1", proposal["alternatives"][0]["operations"]
        )
        assert repr(parent) == before
        names = [p["name"] for p in candidate["ontology"]["properties"]]
        assert "hasProduct" in names
        rels = [r["property"] for r in candidate["assignment"]["relationships"]]
        assert "http://ontobricks.local/evo/supplychain#hasProduct" in rels
        assert len(candidate["design_hash"]) == 64

    def test_valid_proposal_passes_with_review_still_required(self):
        proposal, report = self._proposal()
        ops = proposal["alternatives"][0]["operations"]
        result = ProposalValidator(MemorySourceProbe()).validate(
            ops,
            CandidateFactory.build(variant_document("base"), "1", ops),
            "",
            report["join"]["chosen"],
        )
        assert result["passed"] is True
        assert {
            "name": "business_meaning",
            "status": "needs_review",
            "reason": "",
        } in result["checks"]

    @pytest.mark.parametrize(
        "variant,reason",
        [("orphan", "dangling_target_pct"), ("dup", "target_identity_not_unique")],
    )
    def test_bad_data_blocks(self, variant, reason):
        proposal, report = self._proposal(variant)
        ops = proposal["alternatives"][0]["operations"]
        result = ProposalValidator(MemorySourceProbe()).validate(
            ops,
            CandidateFactory.build(variant_document(variant), "1", ops),
            "",
            report["join"]["chosen"],
        )
        assert result["passed"] is False and reason in result["failures"]

    def test_ops_outside_the_evolve_allowlist_are_rejected(self):
        result = ProposalValidator(MemorySourceProbe()).validate(
            [{"op": "merge_into"}], None
        )
        assert result["failures"] == ["operations_outside_allowlist_or_budget"]


class TestAutofixAllowlists:
    def test_sanity_checks_cannot_apply_evolve_ops(self):
        domain = CandidateFactory.session_for(variant_document("base"), "1")
        out = apply_operations(
            domain, [{"op": "add_relationship", "target": {"name": "x"}, "params": {}}]
        )
        assert out["success"] is False and "Unknown operation" in out["error"]

    def test_add_relationship_rejects_unknown_class_and_duplicate_name(self):
        from back.objects.ontology.autofix_ops import EVOLVE_OPS

        domain = CandidateFactory.session_for(variant_document("base"), "1")
        bad_class = {
            "op": "add_relationship",
            "target": {"name": "rel"},
            "params": {"uri": "u:rel", "domain": "OrderLine", "range": "Nope"},
        }
        dup = {
            "op": "add_relationship",
            "target": {"name": "hasLine"},
            "params": {"uri": "u:x", "domain": "OrderLine", "range": "Product"},
        }
        assert (
            apply_operations(domain, [bad_class], allowed=EVOLVE_OPS)["success"]
            is False
        )
        assert apply_operations(domain, [dup], allowed=EVOLVE_OPS)["success"] is False


class TestUnmodelledDirectLink:
    """A path exists, but the catalog declares a shorter link the ontology lacks."""

    def _index(self):
        sl = variant_document("base")["versions"]["1"]
        sl["ontology"]["properties"].append(
            {
                "uri": "http://ontobricks.local/evo/supplychain#featured",
                "name": "featured",
                "type": "ObjectProperty",
                "domain": "Order",
                "range": "Product",
            }
        )
        return OntologyIndex(sl["ontology"], sl["assignment"])

    INTENT = {
        "output_class": "OrderLine",
        "filters": [{"class": "Product", "attribute": "sku", "value": "SKU-1"}],
    }

    def test_shorter_declared_key_is_proposed_instead_of_ready(self):
        report = CoverageService(self._index(), MemorySourceProbe()).assess(
            Intent.from_dict(self.INTENT)
        )
        assert report["ready"] is False and report["decision_hint"] == "PROPOSE"
        assert report["states"]["path:OrderLine->Product"] == "MISSING_SEMANTICS"
        assert report["gap"] == {
            "kind": "relationship",
            "source": "OrderLine",
            "target": "Product",
        }
        assert report["join"]["chosen"]["source_column"] == "product_id"

    def test_path_hint_keeps_the_existing_path(self):
        intent = {**self.INTENT, "path_hint": ["hasLine", "featured"]}
        report = CoverageService(self._index(), MemorySourceProbe()).assess(
            Intent.from_dict(intent)
        )
        assert "path:OrderLine->Product" not in report["states"]
        assert report["states"]["mapping:featured"] == "MISSING_SEMANTICS"


class TestRealisticSchemaRules:
    """Rules found on TPC-H: prefixed key names, shared-reference paths, computed ids."""

    @pytest.mark.parametrize(
        "column,stem",
        [
            ("l_suppkey", "suppkey"),
            ("s_suppkey", "suppkey"),
            ("product_id", "product_id"),
            ("id", ""),
            ("c_name", "name"),
        ],
    )
    def test_key_stem(self, column, stem):
        from back.objects.ontology.evolution.JoinCandidateFinder import key_stem

        assert key_stem(column) == stem

    def _shared_reference_index(self):
        sl = variant_document("nofk")["versions"]["1"]
        sl["ontology"]["properties"].append(
            {
                "uri": "http://ontobricks.local/evo/supplychain#lineSupplier",
                "name": "lineSupplier",
                "type": "ObjectProperty",
                "domain": "OrderLine",
                "range": "Supplier",
            }
        )
        return OntologyIndex(sl["ontology"], sl["assignment"])

    INTENT = {
        "output_class": "Product",
        "filters": [{"class": "OrderLine", "attribute": "quantity", "value": "2"}],
    }

    def test_path_meeting_at_a_shared_reference_is_not_ready(self):
        report = CoverageService(
            self._shared_reference_index(), MemorySourceProbe()
        ).assess(Intent.from_dict(self.INTENT))
        assert report["states"]["path:Product->OrderLine"] == "AMBIGUOUS"
        assert report["ready"] is False and report["decision_hint"] == "BLOCKED"

    def test_path_hint_confirms_the_shared_reference(self):
        intent = {**self.INTENT, "path_hint": ["suppliedBy", "lineSupplier"]}
        report = CoverageService(
            self._shared_reference_index(), MemorySourceProbe()
        ).assess(Intent.from_dict(intent))
        assert "path:Product->OrderLine" not in report["states"]

    def test_entity_id_computed_in_mapping_sql_is_unsupported(self):
        sl = variant_document("base")["versions"]["1"]
        sl["assignment"]["entities"][3]["id_column"] = "supplier_key"
        index = OntologyIndex(sl["ontology"], sl["assignment"])
        report = CoverageService(index, MemorySourceProbe()).assess(
            Intent.from_dict({**ACME_ORDERS, "output_class": "Product"})
        )
        assert report["states"]["entity:Supplier"] == "UNSUPPORTED"
        assert report["decision_hint"] == "BLOCKED"

    def test_prefixed_key_without_declaration_is_found_and_checked(self):
        cands = JoinCandidateFinder(_index("nofk"), MemorySourceProbe()).candidates(
            "OrderLine", "Product"
        )
        assert any("name_match" in c["evidence"] for c in cands)
