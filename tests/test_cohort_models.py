"""Tests for cohort discovery dataclasses (validation + round-trip)."""

import pytest

from back.core.graph_analysis.models import (
    CohortCompat,
    CohortHop,
    CohortLink,
    CohortOutput,
    CohortRule,
    CohortUCTarget,
)


class TestCohortLink:
    def test_round_trip(self):
        d = {"shared_class": ":Project", "via": ":assignedTo"}
        lk = CohortLink.from_dict(d)
        assert lk.shared_class == ":Project"
        assert lk.via == ":assignedTo"
        assert lk.to_dict() == d

    def test_strips_whitespace(self):
        lk = CohortLink.from_dict({"shared_class": "  :P  ", "via": "  :v  "})
        assert lk.shared_class == ":P"
        assert lk.via == ":v"

    def test_legacy_single_hop_is_promoted_to_path(self):
        lk = CohortLink(shared_class=":Project", via=":assignedTo")
        hops = lk.hops()
        assert len(hops) == 1
        assert hops[0].via == ":assignedTo"
        assert hops[0].target_class == ":Project"
        assert lk.terminal_class == ":Project"

    def test_multi_hop_path_round_trip(self):
        d = {
            "path": [
                {"via": ":assignedTo", "target_class": ":Project"},
                {"via": ":governedBy", "target_class": ":ComplianceType"},
            ]
        }
        lk = CohortLink.from_dict(d)
        assert len(lk.path) == 2
        assert lk.path[0].via == ":assignedTo"
        assert lk.path[1].target_class == ":ComplianceType"
        assert lk.terminal_class == ":ComplianceType"
        assert lk.to_dict() == d

    def test_path_takes_precedence_over_legacy_fields(self):
        lk = CohortLink(
            shared_class=":LegacyClass",
            via=":legacyVia",
            path=[CohortHop(via=":a", target_class=":B")],
        )
        hops = lk.hops()
        assert len(hops) == 1
        assert hops[0].target_class == ":B"
        assert lk.terminal_class == ":B"


class TestCohortHop:
    def test_round_trip(self):
        d = {"via": ":governedBy", "target_class": ":ComplianceType"}
        h = CohortHop.from_dict(d)
        assert h.via == ":governedBy"
        assert h.target_class == ":ComplianceType"
        assert h.to_dict() == d

    def test_strips_whitespace(self):
        h = CohortHop.from_dict({"via": "  :v  ", "target_class": "  :T  "})
        assert h.via == ":v"
        assert h.target_class == ":T"

    def test_default_where_is_empty_list(self):
        h = CohortHop.from_dict({"via": ":v", "target_class": ":T"})
        assert h.where == []
        assert "where" not in h.to_dict()

    def test_where_round_trip(self):
        d = {
            "via": ":governedBy",
            "target_class": ":ComplianceType",
            "where": [
                {
                    "type": "value_equals",
                    "property": ":complianceTypeId",
                    "value": "Individual",
                }
            ],
        }
        h = CohortHop.from_dict(d)
        assert len(h.where) == 1
        assert h.where[0].type == "value_equals"
        assert h.where[0].value == "Individual"
        assert h.to_dict() == d

    def test_where_supports_value_in_and_range(self):
        h = CohortHop.from_dict(
            {
                "via": ":v",
                "target_class": ":T",
                "where": [
                    {"type": "value_in", "property": ":r", "values": ["A", "B"]},
                    {"type": "value_range", "property": ":age", "min": 18},
                ],
            }
        )
        assert h.where[0].values == ["A", "B"]
        assert h.where[1].min == 18.0


class TestCohortCompat:
    def test_value_equals(self):
        d = {"type": "value_equals", "property": ":status", "value": "Exempt"}
        cc = CohortCompat.from_dict(d)
        assert cc.type == "value_equals"
        assert cc.value == "Exempt"
        assert cc.to_dict() == d

    def test_value_in(self):
        d = {"type": "value_in", "property": ":region", "values": ["EMEA", "AMER"]}
        cc = CohortCompat.from_dict(d)
        assert cc.values == ["EMEA", "AMER"]
        assert cc.to_dict()["values"] == ["EMEA", "AMER"]

    def test_value_range_floats(self):
        cc = CohortCompat.from_dict(
            {"type": "value_range", "property": ":age", "min": 18, "max": 65}
        )
        assert cc.min == 18.0
        assert cc.max == 65.0

    def test_allow_missing_omitted_when_false(self):
        cc = CohortCompat(type="same_value", property=":x")
        assert "allow_missing" not in cc.to_dict()


class TestCohortUCTarget:
    def test_fq_name_quotes_identifiers(self):
        t = CohortUCTarget(catalog="c", schema="s", table_name="t")
        assert t.fq_name() == "`c`.`s`.`t`"


class TestCohortOutput:
    def test_uc_table_dropped_when_table_name_missing(self):
        out = CohortOutput.from_dict(
            {"graph": True, "uc_table": {"catalog": "c", "schema": "s"}}
        )
        assert out.uc_table is None

    def test_uc_table_kept_when_complete(self):
        out = CohortOutput.from_dict(
            {
                "graph": False,
                "uc_table": {"catalog": "c", "schema": "s", "table_name": "t"},
            }
        )
        assert out.graph is False
        assert out.uc_table is not None
        assert out.uc_table.fq_name() == "`c`.`s`.`t`"


class TestCohortRule:
    def _good(self) -> CohortRule:
        return CohortRule(
            id="exempt-pool",
            label="Exempt staffing pool",
            class_uri="http://acme/Person",
            links=[
                CohortLink(
                    shared_class="http://acme/Project",
                    via="http://acme/assignedTo",
                )
            ],
            compatibility=[
                CohortCompat(type="same_value", property="http://acme/status"),
                CohortCompat(
                    type="value_equals",
                    property="http://acme/status",
                    value="Exempt",
                ),
            ],
        )

    def test_validate_ok(self):
        assert self._good().validate() == []

    def test_validate_missing_id(self):
        r = self._good()
        r.id = ""
        errs = r.validate()
        assert any("id" in e.lower() for e in errs)

    def test_validate_bad_group_type(self):
        r = self._good()
        r.group_type = "loose"
        errs = r.validate()
        assert any("group_type" in e for e in errs)

    def test_validate_bad_links_combine(self):
        r = self._good()
        r.links_combine = "xor"
        errs = r.validate()
        assert any("links_combine" in e for e in errs)

    def test_validate_link_missing_via(self):
        r = self._good()
        r.links[0].via = ""
        errs = r.validate()
        assert any("via" in e for e in errs)

    def test_validate_value_in_requires_values(self):
        r = self._good()
        r.compatibility = [CohortCompat(type="value_in", property=":x", values=[])]
        errs = r.validate()
        assert any("values" in e for e in errs)

    def test_validate_value_range_requires_bound(self):
        r = self._good()
        r.compatibility = [CohortCompat(type="value_range", property=":x")]
        errs = r.validate()
        assert any("min or max" in e for e in errs)

    def test_round_trip_preserves_shape(self):
        original = self._good()
        d = original.to_dict()
        restored = CohortRule.from_dict(d)
        assert restored.to_dict() == d

    def test_min_size_lower_bound(self):
        r = self._good()
        r.min_size = 1
        errs = r.validate()
        assert any("min_size" in e for e in errs)

    def test_validate_hop_where_same_value_rejected(self):
        r = self._good()
        r.links = [
            CohortLink(
                path=[
                    CohortHop(
                        via=":v",
                        target_class=":T",
                        where=[CohortCompat(type="same_value", property=":x")],
                    )
                ]
            )
        ]
        errs = r.validate()
        assert any("same_value" in e and "hop" in e for e in errs)

    def test_validate_hop_where_value_equals_requires_value(self):
        r = self._good()
        r.links = [
            CohortLink(
                path=[
                    CohortHop(
                        via=":v",
                        target_class=":T",
                        where=[CohortCompat(type="value_equals", property=":x")],
                    )
                ]
            )
        ]
        errs = r.validate()
        assert any("value is required" in e and "hop" in e for e in errs)

    def test_validate_hop_where_value_in_requires_values(self):
        r = self._good()
        r.links = [
            CohortLink(
                path=[
                    CohortHop(
                        via=":v",
                        target_class=":T",
                        where=[CohortCompat(type="value_in", property=":x", values=[])],
                    )
                ]
            )
        ]
        errs = r.validate()
        assert any("values list is required" in e and "hop" in e for e in errs)

    def test_validate_hop_where_ok(self):
        r = self._good()
        r.links = [
            CohortLink(
                path=[
                    CohortHop(
                        via=":assignedTo",
                        target_class=":Project",
                    ),
                    CohortHop(
                        via=":governedBy",
                        target_class=":ComplianceType",
                        where=[
                            CohortCompat(
                                type="value_equals",
                                property=":complianceTypeId",
                                value="Individual",
                            )
                        ],
                    ),
                ]
            )
        ]
        assert r.validate() == []
