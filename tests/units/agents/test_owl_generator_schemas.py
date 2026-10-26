"""Structured-schema parsing/validation for the staged owl-generator.

Covers the Stage-1 detection candidate schema and the Stage-3 completion
(relations/attributes/axioms) schemas parsed by
``agents.agent_owl_generator.schemas``. These parsers are the reject-only
boundary for the staged contract: a malformed or out-of-schema LLM answer
raises :class:`SchemaValidationError` (never a silent rewrite), and the
referenced-id extractors feed the Stage-3 entity-closure check.
"""

from __future__ import annotations

import json

import pytest

from back.objects.ontology.generate.GenerateDraft import (
    GenerateEntity,
    ORIGIN_DETECTED,
    ORIGIN_MANUAL,
    TYPE_CLASS,
)
from agents.agent_owl_generator import schemas


# ---------------------------------------------------------------------------
# Detection candidate schema
# ---------------------------------------------------------------------------


class TestParseDetectionPayload:
    def test_parses_minimal_candidate(self):
        payload = '{"candidate_entities": [{"canonical_label": "Carrier"}]}'
        candidates = schemas.parse_detection_payload(payload)
        assert len(candidates) == 1
        c = candidates[0]
        assert isinstance(c, GenerateEntity)
        assert c.canonical_label == "Carrier"
        # Detection defaults every candidate to included and origin=detected.
        assert c.included is True
        assert c.origin == ORIGIN_DETECTED
        assert c.type_hint == TYPE_CLASS
        # A detection-time id is minted (the LLM does not supply one).
        assert c.id.startswith("cand-")

    def test_strips_markdown_fences(self):
        payload = (
            "```json\n"
            '{"candidate_entities": [{"canonical_label": "Order"}]}\n'
            "```"
        )
        candidates = schemas.parse_detection_payload(payload)
        assert [c.canonical_label for c in candidates] == ["Order"]

    def test_malformed_json_is_rejected(self):
        with pytest.raises(schemas.SchemaValidationError):
            schemas.parse_detection_payload("not json at all {{{")

    def test_missing_candidate_list_is_rejected(self):
        with pytest.raises(schemas.SchemaValidationError):
            schemas.parse_detection_payload('{"stuff": []}')

    def test_synonyms_kept_as_alternate_labels_not_separate_entities(self):
        payload = (
            '{"candidate_entities": [{"canonical_label": "Customer", '
            '"alternate_labels": ["Client", "Account Holder"]}]}'
        )
        candidates = schemas.parse_detection_payload(payload)
        assert len(candidates) == 1
        assert candidates[0].alternate_labels == ["Client", "Account Holder"]

    def test_dedup_against_anchor_canonical_label(self):
        anchors = [GenerateEntity.locked_anchor("cls-Customer-a1", "Customer")]
        payload = (
            '{"candidate_entities": ['
            '{"canonical_label": "Customer"}, {"canonical_label": "Carrier"}]}'
        )
        candidates = schemas.parse_detection_payload(payload, existing_anchors=anchors)
        assert [c.canonical_label for c in candidates] == ["Carrier"]

    def test_dedup_against_anchor_alternate_label(self):
        anchors = [
            GenerateEntity.locked_anchor(
                "cls-Order-b2", "Order", alternate_labels=["PurchaseOrder"]
            )
        ]
        payload = (
            '{"candidate_entities": ['
            '{"canonical_label": "PurchaseOrder"}, {"canonical_label": "Invoice"}]}'
        )
        candidates = schemas.parse_detection_payload(payload, existing_anchors=anchors)
        assert [c.canonical_label for c in candidates] == ["Invoice"]

    def test_dedup_when_candidate_alt_label_matches_anchor(self):
        anchors = [GenerateEntity.locked_anchor("cls-Customer-a1", "Customer")]
        payload = (
            '{"candidate_entities": ['
            '{"canonical_label": "Buyer", "alternate_labels": ["Customer"]}]}'
        )
        candidates = schemas.parse_detection_payload(payload, existing_anchors=anchors)
        assert candidates == []

    def test_dedup_between_candidates(self):
        payload = (
            '{"candidate_entities": ['
            '{"canonical_label": "Carrier"}, {"canonical_label": "carrier"}]}'
        )
        candidates = schemas.parse_detection_payload(payload)
        assert len(candidates) == 1

    def test_empty_candidate_list_is_accepted_not_rejected(self):
        # Live bug fix: the mandatory answer when every grounded entity is
        # already a locked anchor (or nothing new exists) is an explicit
        # empty list — that is a valid, successful parse, not a schema
        # violation.
        candidates = schemas.parse_detection_payload('{"candidate_entities": []}')
        assert candidates == []

    def test_empty_candidate_list_accepted_even_with_anchors_present(self):
        anchors = [GenerateEntity.locked_anchor("cls-Customer-a1", "Customer")]
        candidates = schemas.parse_detection_payload(
            '{"candidate_entities": []}', existing_anchors=anchors
        )
        assert candidates == []

    def test_unknown_type_hint_coerced_to_class(self):
        payload = (
            '{"candidate_entities": [{"canonical_label": "Order", '
            '"type_hint": "table"}]}'
        )
        candidates = schemas.parse_detection_payload(payload)
        assert candidates[0].type_hint == TYPE_CLASS

    def test_max_candidates_bounds_result(self):
        items = ",".join(
            '{"canonical_label": "E%d"}' % i for i in range(20)
        )
        payload = '{"candidate_entities": [%s]}' % items
        candidates = schemas.parse_detection_payload(payload, max_candidates=5)
        assert len(candidates) == 5

    def test_evidence_carried_through(self):
        payload = (
            '{"candidate_entities": [{"canonical_label": "Carrier", '
            '"evidence": [{"source": "spec.pdf", "excerpt": "Order ships via Carrier."}]}]}'
        )
        candidates = schemas.parse_detection_payload(payload)
        assert candidates[0].evidence == [
            {"source": "spec.pdf", "excerpt": "Order ships via Carrier."}
        ]


# ---------------------------------------------------------------------------
# Transport-level structured-output schema (response_format json_schema)
#
# Live-reliability fix: prompt-only "JSON only" instructions cannot force a
# compliant model to skip a visible reasoning preamble (observed live: 4/5
# calls against the user's own endpoint still narrated prose before the
# JSON, despite the strengthened prompt). The user confirmed that same
# endpoint DOES honour OpenAI/Databricks-style
# ``response_format={"type": "json_schema", "json_schema": {...}}`` and
# returns exactly the schema-shaped JSON. This constant is the strict
# json_schema Stage 1 passes as that transport-level ``response_format`` on
# its single finalization call — never on the tool-gathering calls, which
# the endpoint rejects when combined with ``tools``.
# ---------------------------------------------------------------------------


class TestDetectionResponseFormat:
    def test_is_a_json_schema_response_format(self):
        rf = schemas.DETECTION_RESPONSE_FORMAT
        assert rf["type"] == "json_schema"
        assert "json_schema" in rf
        assert rf["json_schema"]["strict"] is True

    def test_schema_requires_candidate_entities_array(self):
        schema = schemas.DETECTION_RESPONSE_FORMAT["json_schema"]["schema"]
        assert schema["type"] == "object"
        assert schema["required"] == ["candidate_entities"]
        assert schema["additionalProperties"] is False
        items = schema["properties"]["candidate_entities"]["items"]
        assert items["type"] == "object"

    def test_item_schema_matches_the_parser_contract(self):
        # The exact fields `parse_detection_payload` reads: canonical_label,
        # description, type_hint (class-only for Stage 1), evidence
        # (source/excerpt), alternate_labels.
        items = schemas.DETECTION_RESPONSE_FORMAT["json_schema"]["schema"][
            "properties"
        ]["candidate_entities"]["items"]
        assert set(items["properties"]) == {
            "canonical_label",
            "description",
            "type_hint",
            "evidence",
            "alternate_labels",
        }
        assert set(items["required"]) == set(items["properties"])
        assert items["additionalProperties"] is False
        assert items["properties"]["type_hint"]["enum"] == [TYPE_CLASS]

        evidence_item = items["properties"]["evidence"]["items"]
        assert set(evidence_item["properties"]) == {"source", "excerpt"}
        assert set(evidence_item["required"]) == {"source", "excerpt"}
        assert evidence_item["additionalProperties"] is False

    def test_response_format_produces_an_endpoint_ready_empty_answer(self):
        # Sanity check: an answer that is exactly what the schema demands for
        # zero new candidates must still parse cleanly through the same
        # parser used for every other detection answer.
        candidates = schemas.parse_detection_payload('{"candidate_entities": []}')
        assert candidates == []


# ---------------------------------------------------------------------------
# Completion schemas + referenced-id extraction
# ---------------------------------------------------------------------------


class TestParseRelationsPayload:
    def test_parses_relations(self):
        payload = (
            '{"relations": [{"label": "placesOrder", '
            '"domain": "cls-Customer-a1", "range": "cand-6"}]}'
        )
        result = schemas.parse_relations_payload(payload)
        assert result["relations"][0]["label"] == "placesOrder"

    def test_missing_relations_key_rejected(self):
        with pytest.raises(schemas.SchemaValidationError):
            schemas.parse_relations_payload('{"nope": []}')

    def test_relation_missing_domain_rejected(self):
        with pytest.raises(schemas.SchemaValidationError):
            schemas.parse_relations_payload(
                '{"relations": [{"label": "x", "range": "cand-6"}]}'
            )

    def test_referenced_ids_union_domain_range(self):
        payload = (
            '{"relations": [{"label": "placesOrder", '
            '"domain": "cls-Customer-a1", "range": "cand-6"}]}'
        )
        result = schemas.parse_relations_payload(payload)
        assert schemas.relations_referenced_ids(result) == {
            "cls-Customer-a1",
            "cand-6",
        }

    def test_inverse_pair_keeps_active_voice_only(self):
        payload = json.dumps(
            {
                "relations": [
                    {
                        "label": "handled",
                        "domain": "cand-Claim",
                        "range": "cand-Agent",
                        "evidence": "passive",
                    },
                    {
                        "label": "handles",
                        "domain": "cand-Agent",
                        "range": "cand-Claim",
                        "evidence": "active",
                    },
                ]
            }
        )
        result = schemas.parse_relations_payload(payload)
        assert len(result["relations"]) == 1
        kept = result["relations"][0]
        assert kept["label"] == "handles"
        assert kept["domain"] == "cand-Agent"
        assert kept["range"] == "cand-Claim"

    def test_unrelated_pairs_are_not_collapsed(self):
        payload = json.dumps(
            {
                "relations": [
                    {"label": "placesOrder", "domain": "cand-Customer", "range": "cand-Order"},
                    {"label": "shipsVia", "domain": "cand-Order", "range": "cand-Carrier"},
                ]
            }
        )
        result = schemas.parse_relations_payload(payload)
        assert len(result["relations"]) == 2

    def test_distinct_reverse_predicates_on_same_pair_are_kept(self):
        payload = json.dumps(
            {
                "relations": [
                    {"label": "issues", "domain": "cand-Company", "range": "cand-Invoice"},
                    {"label": "references", "domain": "cand-Invoice", "range": "cand-Company"},
                ]
            }
        )
        result = schemas.parse_relations_payload(payload)
        assert [r["label"] for r in result["relations"]] == ["issues", "references"]

    def test_settlement_paraphrases_collapse_to_active_voice(self):
        payload = json.dumps(
            {
                "relations": [
                    {"label": "settledBy", "domain": "cand-Contract", "range": "cand-Payment"},
                    {"label": "isSettledBy", "domain": "cand-Contract", "range": "cand-Payment"},
                    {"label": "settles", "domain": "cand-Payment", "range": "cand-Contract"},
                ]
            }
        )
        result = schemas.parse_relations_payload(payload)
        assert result["relations"] == [
            {"label": "settles", "domain": "cand-Payment", "range": "cand-Contract"}
        ]

    def test_paraphrase_of_existing_relation_is_dropped(self):
        payload = json.dumps(
            {
                "relations": [
                    {"label": "isSettledBy", "domain": "Contract", "range": "Payment"},
                ]
            }
        )
        result = schemas.parse_relations_payload(
            payload,
            existing=[{"label": "settles", "domain": "Payment", "range": "Contract"}],
        )
        assert result["relations"] == []


class TestParseAttributesPayload:
    def test_parses_attributes(self):
        payload = (
            '{"attributes": [{"label": "orderDate", '
            '"domain": "cand-6", "datatype": "xsd:date"}]}'
        )
        result = schemas.parse_attributes_payload(payload)
        assert result["attributes"][0]["label"] == "orderDate"

    def test_referenced_ids_are_domains(self):
        payload = (
            '{"attributes": ['
            '{"label": "orderDate", "domain": "cand-6", "datatype": "xsd:date"},'
            '{"label": "name", "domain": "cls-Customer-a1", "datatype": "xsd:string"}]}'
        )
        result = schemas.parse_attributes_payload(payload)
        assert schemas.attributes_referenced_ids(result) == {
            "cand-6",
            "cls-Customer-a1",
        }

    def test_attribute_missing_domain_rejected(self):
        with pytest.raises(schemas.SchemaValidationError):
            schemas.parse_attributes_payload(
                '{"attributes": [{"label": "x", "datatype": "xsd:string"}]}'
            )


class TestParseAxiomsPayload:
    def test_parses_axioms(self):
        payload = (
            '{"axioms": [{"kind": "subClassOf", '
            '"subject": "cand-6", "object": "cls-Customer-a1"}]}'
        )
        result = schemas.parse_axioms_payload(payload)
        assert result["axioms"][0]["kind"] == "subClassOf"

    def test_referenced_ids_union_subject_object(self):
        payload = (
            '{"axioms": [{"kind": "disjointWith", '
            '"subject": "cand-6", "object": "cls-UnknownGhost"}]}'
        )
        result = schemas.parse_axioms_payload(payload)
        assert schemas.axioms_referenced_ids(result) == {
            "cand-6",
            "cls-UnknownGhost",
        }

    def test_axiom_missing_subject_rejected(self):
        with pytest.raises(schemas.SchemaValidationError):
            schemas.parse_axioms_payload(
                '{"axioms": [{"kind": "subClassOf", "object": "cls-Customer-a1"}]}'
            )

    def test_drops_second_parent_in_payload(self):
        payload = json.dumps(
            {
                "axioms": [
                    {
                        "kind": "subClassOf",
                        "subject": "cand-6",
                        "object": "Vehicle",
                    },
                    {
                        "kind": "subClassOf",
                        "subject": "cand-6",
                        "object": "Boat",
                    },
                    {
                        "kind": "disjointWith",
                        "subject": "cand-6",
                        "object": "Boat",
                    },
                ]
            }
        )
        result = schemas.parse_axioms_payload(payload)
        subclass = [a for a in result["axioms"] if a["kind"] == "subClassOf"]
        assert subclass == [
            {"kind": "subClassOf", "subject": "cand-6", "object": "Vehicle"}
        ]
        assert result["axioms"][-1]["kind"] == "disjointWith"

    def test_drops_subclassof_when_subject_already_has_parent(self):
        payload = json.dumps(
            {
                "axioms": [
                    {
                        "kind": "subClassOf",
                        "subject": "Customer",
                        "object": "Agent",
                    },
                    {
                        "kind": "disjointWith",
                        "subject": "Customer",
                        "object": "Supplier",
                    },
                ]
            }
        )
        result = schemas.parse_axioms_payload(
            payload, existing_parents={"Customer": "Party"}
        )
        assert result["axioms"] == [
            {
                "kind": "disjointWith",
                "subject": "Customer",
                "object": "Supplier",
            }
        ]


# ---------------------------------------------------------------------------
# Residual live-reliability bug: transport-level JSON double-encoding.
#
# Under `response_format` json_schema, Claude Sonnet endpoints
# (`databricks-claude-sonnet-5`, and the user's own
# `benoit_cayla.ontobricks-todrop.monclaudesonnetamoi`) intermittently
# double-encode the structured answer: a list-typed field's value comes
# back as a JSON-encoded STRING instead of the raw JSON array (e.g.
# `{"attributes": "[{...}]"}`), or the ENTIRE payload comes back as a
# JSON-encoded string holding the real object as its value (e.g.
# `content = "{\"attributes\": [...]}"`). This is an endpoint/transport
# quirk, not free-text prose — the tolerance is exactly ONE extra
# `json.loads`, accepted only if it yields the required type; anything
# else (not a str, a second decode that still isn't the right shape, a
# second decode that fails outright) is rejected exactly as before, with
# no further decoding and no loop.
# ---------------------------------------------------------------------------


class TestWholePayloadDoubleEncodingTolerance:
    def test_whole_payload_double_encoded_json_string_is_accepted(self):
        # The wire content is a JSON string whose value IS the real object
        # — exactly the reported shape (`content = "{\"attributes\": [...]}"`).
        real = {
            "attributes": [
                {"label": "orderDate", "domain": "cand-6", "datatype": "xsd:date"}
            ]
        }
        double_encoded = json.dumps(json.dumps(real))
        result = schemas.parse_attributes_payload(double_encoded)
        assert result["attributes"][0]["label"] == "orderDate"

    def test_whole_payload_still_rejects_triple_encoded(self):
        # A second level of encoding beyond the tolerated one still isn't a
        # dict after the single extra decode — rejected, not decoded again.
        real = {"attributes": []}
        triple_encoded = json.dumps(json.dumps(json.dumps(real)))
        with pytest.raises(schemas.SchemaValidationError):
            schemas.parse_attributes_payload(triple_encoded)

    def test_whole_payload_still_rejects_a_plain_json_encoded_sentence(self):
        # After the one tolerated decode this is a plain string, not a
        # dict — rejected with the same clear message, no further attempts.
        payload = json.dumps("just a sentence, not an object")
        with pytest.raises(schemas.SchemaValidationError):
            schemas.parse_json_object(payload)

    def test_whole_payload_rejects_a_json_encoded_number(self):
        # Not a str in the first place (json.loads("42") == 42) — no decode
        # tolerance applies; rejected immediately as before.
        with pytest.raises(schemas.SchemaValidationError):
            schemas.parse_json_object("42")

    def test_normal_already_correct_payload_still_parses_unchanged(self):
        payload = '{"attributes": [{"label": "x", "domain": "cand-6", "datatype": "xsd:string"}]}'
        result = schemas.parse_attributes_payload(payload)
        assert result["attributes"][0]["label"] == "x"


class TestFieldLevelDoubleEncodingTolerance:
    def test_attributes_field_double_encoded_string_is_unwrapped(self):
        inner = [{"label": "orderDate", "domain": "cand-6", "datatype": "xsd:date"}]
        payload = json.dumps({"attributes": json.dumps(inner)})
        result = schemas.parse_attributes_payload(payload)
        assert result["attributes"][0]["label"] == "orderDate"

    def test_relations_field_double_encoded_string_is_unwrapped(self):
        inner = [{"label": "placesOrder", "domain": "cls-Customer-a1", "range": "cand-6"}]
        payload = json.dumps({"relations": json.dumps(inner)})
        result = schemas.parse_relations_payload(payload)
        assert result["relations"][0]["label"] == "placesOrder"

    def test_axioms_field_double_encoded_string_is_unwrapped(self):
        inner = [{"kind": "subClassOf", "subject": "cand-6", "object": "cls-Customer-a1"}]
        payload = json.dumps({"axioms": json.dumps(inner)})
        result = schemas.parse_axioms_payload(payload)
        assert result["axioms"][0]["kind"] == "subClassOf"

    def test_detection_candidate_entities_field_double_encoded_string_is_unwrapped(self):
        inner = [{"canonical_label": "Carrier"}]
        payload = json.dumps({"candidate_entities": json.dumps(inner)})
        candidates = schemas.parse_detection_payload(payload)
        assert [c.canonical_label for c in candidates] == ["Carrier"]

    def test_field_level_still_rejects_triple_encoded(self):
        # One extra decode of a triple-encoded field yields a STRING (the
        # single-encoded form), not a list — rejected, not decoded again.
        inner = [{"label": "x", "domain": "cand-6", "datatype": "xsd:string"}]
        triple_encoded_field = json.dumps(json.dumps(inner))
        payload = json.dumps({"attributes": triple_encoded_field})
        with pytest.raises(schemas.SchemaValidationError):
            schemas.parse_attributes_payload(payload)

    def test_field_level_still_rejects_a_string_that_is_not_json_at_all(self):
        payload = json.dumps({"attributes": "not json at all {{{"})
        with pytest.raises(schemas.SchemaValidationError):
            schemas.parse_attributes_payload(payload)

    def test_field_level_still_rejects_a_json_string_decoding_to_a_number(self):
        # Decodes fine but isn't a list — rejected exactly like before.
        payload = json.dumps({"attributes": "42"})
        with pytest.raises(schemas.SchemaValidationError):
            schemas.parse_attributes_payload(payload)

    def test_field_level_tolerance_still_validates_item_shape(self):
        # The unwrapped list must still contain JSON objects — a decoded
        # list of non-dict items is rejected exactly like a non-encoded one.
        payload = json.dumps({"attributes": json.dumps(["not-an-object"])})
        with pytest.raises(schemas.SchemaValidationError):
            schemas.parse_attributes_payload(payload)


# ---------------------------------------------------------------------------
# Transport-level structured output for Stage 3 (live id-bracketing bug fix)
#
# Root cause: the old catalog/prompt rendered each entity id in brackets
# (``[<id>]``) and the closure rule said "reference entities ONLY by the
# ids listed above" — the model copied the bracketed token verbatim as
# domain/range, and `GenerateDraft.validate_references` (comparing against
# the *bare* id) rejected every reference. These enum-constrained
# `response_format` builders make a bracketed/invented id structurally
# impossible on an endpoint that honours `response_format` — see
# `agents.agent_owl_generator.staged._completion_response_format`.
# ---------------------------------------------------------------------------


class TestRelationsResponseFormat:
    def test_is_a_strict_json_schema(self):
        rf = schemas.build_relations_response_format({"cls-Customer-a1", "cand-6"})
        assert rf["type"] == "json_schema"
        assert rf["json_schema"]["strict"] is True

    def test_domain_and_range_are_enum_constrained_to_closed_ids(self):
        rf = schemas.build_relations_response_format({"cls-Customer-a1", "cand-6"})
        item = rf["json_schema"]["schema"]["properties"]["relations"]["items"]
        assert item["properties"]["domain"]["enum"] == ["cand-6", "cls-Customer-a1"]
        assert item["properties"]["range"]["enum"] == ["cand-6", "cls-Customer-a1"]
        assert item["additionalProperties"] is False
        assert set(item["required"]) == {"label", "domain", "range", "evidence"}

    def test_domain_and_range_enums_are_distinct_objects(self):
        # Two separately-mutable schema objects, never the same dict
        # reference shared by identity between the two fields.
        rf = schemas.build_relations_response_format({"cand-6"})
        item = rf["json_schema"]["schema"]["properties"]["relations"]["items"]
        assert item["properties"]["domain"] is not item["properties"]["range"]


class TestAttributesResponseFormat:
    def test_domain_is_enum_constrained_to_closed_ids(self):
        rf = schemas.build_attributes_response_format({"cls-Customer-a1", "cand-6"})
        item = rf["json_schema"]["schema"]["properties"]["attributes"]["items"]
        assert item["properties"]["domain"]["enum"] == ["cand-6", "cls-Customer-a1"]
        assert item["additionalProperties"] is False
        assert set(item["required"]) == {"label", "domain", "datatype", "evidence"}


class TestAxiomsResponseFormat:
    def test_subject_and_object_are_enum_constrained_to_closed_ids(self):
        rf = schemas.build_axioms_response_format({"cls-Customer-a1", "cand-6"})
        item = rf["json_schema"]["schema"]["properties"]["axioms"]["items"]
        assert item["properties"]["subject"]["enum"] == ["cand-6", "cls-Customer-a1"]
        assert item["properties"]["object"]["enum"] == ["cand-6", "cls-Customer-a1"]
        assert item["additionalProperties"] is False
        assert set(item["required"]) == {"kind", "subject", "object"}

    def test_kind_is_enum_constrained_to_the_three_axiom_kinds(self):
        rf = schemas.build_axioms_response_format({"cand-6"})
        item = rf["json_schema"]["schema"]["properties"]["axioms"]["items"]
        assert set(item["properties"]["kind"]["enum"]) == {
            "subClassOf",
            "disjointWith",
            "equivalentClass",
        }
