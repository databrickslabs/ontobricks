"""Tests for ontology DesignChecks catalog."""

from __future__ import annotations

import time

import pytest

from back.objects.ontology.DesignChecks import DesignChecks

pytestmark = pytest.mark.unit


def _rules(report: dict) -> set[str]:
    return {f["rule"] for f in report["findings"]}


def _by_rule(report: dict, rule: str) -> list[dict]:
    return [f for f in report["findings"] if f["rule"] == rule]


def _cls(**kwargs):
    data = {"name": "Person", "uri": "http://ex.org#Person", "label": "Person",
            "description": "A person"}
    data.update(kwargs)
    return data


def _prop(**kwargs):
    data = {
        "name": "worksAt",
        "uri": "http://ex.org#worksAt",
        "label": "works at",
        "description": "Employment",
        "type": "ObjectProperty",
        "domain": "Person",
        "range": "Organization",
    }
    data.update(kwargs)
    return data


def _org(**kwargs):
    data = {
        "name": "Organization",
        "uri": "http://ex.org#Organization",
        "label": "Organization",
        "description": "An organization",
    }
    data.update(kwargs)
    return data


HEALTHY = [_cls(), _org(parent="Person")]
HEALTHY_PROPS = [_prop()]


class TestIntegrity:
    def test_e01_broken_parent(self):
        report = DesignChecks.run([_cls(parent="Missing")], [], [])
        assert "DC-E01" in _rules(report)

    def test_e01_negative(self):
        report = DesignChecks.run(HEALTHY, HEALTHY_PROPS, [])
        assert "DC-E01" not in _rules(report)

    def test_e02_broken_domain(self):
        report = DesignChecks.run(
            HEALTHY, [_prop(domain="Ghost")], []
        )
        assert "DC-E02" in _rules(report)

    def test_e02_negative(self):
        report = DesignChecks.run(HEALTHY, HEALTHY_PROPS, [])
        assert "DC-E02" not in _rules(report)

    def test_e03_broken_range_object_property(self):
        report = DesignChecks.run(
            HEALTHY, [_prop(range="Ghost")], []
        )
        assert "DC-E03" in _rules(report)

    def test_e03_skips_datatype_range(self):
        report = DesignChecks.run(
            HEALTHY,
            [_prop(type="DatatypeProperty", range="xsd:string", domain="Person")],
            [],
        )
        assert "DC-E03" not in _rules(report)

    def test_e04_broken_group_member(self):
        report = DesignChecks.run(
            HEALTHY, HEALTHY_PROPS, [{"name": "G", "members": ["Ghost"]}]
        )
        assert "DC-E04" in _rules(report)
        assert _by_rule(report, "DC-E04")[0]["kind"] == "group"

    def test_e04_negative(self):
        report = DesignChecks.run(
            HEALTHY, HEALTHY_PROPS, [{"name": "G", "members": ["Person"]}]
        )
        assert "DC-E04" not in _rules(report)

    def test_e05_hierarchy_cycle(self):
        a = _cls(name="A", uri="http://ex.org#A", parent="C")
        b = _cls(name="B", uri="http://ex.org#B", parent="A", label="B", description="b")
        c = _cls(name="C", uri="http://ex.org#C", parent="B", label="C", description="c")
        report = DesignChecks.run([a, b, c], [], [])
        assert "DC-E05" in _rules(report)
        assert len(_by_rule(report, "DC-E05")) == 1

    def test_e05_negative(self):
        report = DesignChecks.run(HEALTHY, HEALTHY_PROPS, [])
        assert "DC-E05" not in _rules(report)

    def test_e06_duplicate_class_uri(self):
        report = DesignChecks.run(
            [_cls(), _org(uri="http://ex.org#Person")],
            [],
            [],
        )
        assert "DC-E06" in _rules(report)

    def test_e06_negative(self):
        report = DesignChecks.run(HEALTHY, HEALTHY_PROPS, [])
        assert "DC-E06" not in _rules(report)


class TestHygiene:
    def test_w01_orphan_class(self):
        report = DesignChecks.run([_cls()], [], [])
        assert "DC-W01" in _rules(report)

    def test_w01_negative_when_used_as_domain(self):
        report = DesignChecks.run(HEALTHY, HEALTHY_PROPS, [])
        assert "DC-W01" not in _rules(report)

    def test_w02_missing_label(self):
        report = DesignChecks.run([_cls(label="")], [], [])
        assert "DC-W02" in _rules(report)

    def test_w02_negative(self):
        report = DesignChecks.run(HEALTHY, HEALTHY_PROPS, [])
        assert "DC-W02" not in _rules(report)

    def test_w03_missing_domain(self):
        report = DesignChecks.run(HEALTHY, [_prop(domain="")], [])
        assert "DC-W03" in _rules(report)
        assert _by_rule(report, "DC-W03")[0]["kind"] == "property"

    def test_w03_datatype_property_is_kind_attribute(self):
        # Attributes must not be tagged as ``property``: the Sanity Checks UI
        # would open Relationships, which only lists ObjectProperties.
        report = DesignChecks.run(
            HEALTHY,
            [_prop(type="DatatypeProperty", domain="", range="xsd:string")],
            [],
        )
        finding = _by_rule(report, "DC-W03")[0]
        assert finding["kind"] == "attribute"
        assert "Attribute" in finding["message"]

    def test_w03_attribute_with_domain_exposes_owner(self):
        report = DesignChecks.run(
            HEALTHY,
            [_prop(type="DatatypeProperty", domain="Person", range="")],
            [],
        )
        finding = _by_rule(report, "DC-W04")[0]
        assert finding["kind"] == "attribute"
        assert finding["domain"] == "Person"

    def test_w03_negative(self):
        report = DesignChecks.run(HEALTHY, HEALTHY_PROPS, [])
        assert "DC-W03" not in _rules(report)

    def test_w04_missing_range(self):
        report = DesignChecks.run(HEALTHY, [_prop(range="")], [])
        assert "DC-W04" in _rules(report)

    def test_w04_negative(self):
        report = DesignChecks.run(HEALTHY, HEALTHY_PROPS, [])
        assert "DC-W04" not in _rules(report)

    def test_w05_duplicate_name(self):
        other = _org(name="person", uri="http://ex.org#Other")
        report = DesignChecks.run([_cls(), other], [], [])
        assert "DC-W05" in _rules(report)

    def test_w05_negative(self):
        report = DesignChecks.run(HEALTHY, HEALTHY_PROPS, [])
        assert "DC-W05" not in _rules(report)

    def test_i01_missing_description(self):
        report = DesignChecks.run([_cls(description="", comment="")], [], [])
        assert "DC-I01" in _rules(report)

    def test_i01_negative(self):
        report = DesignChecks.run(HEALTHY, HEALTHY_PROPS, [])
        assert "DC-I01" not in _rules(report)


class TestNamingAndHierarchy:
    def test_i02_class_name_case(self):
        report = DesignChecks.run([_cls(name="person", localName="person")], [], [])
        assert "DC-I02" in _rules(report)

    def test_i02_negative(self):
        report = DesignChecks.run(HEALTHY, HEALTHY_PROPS, [])
        assert "DC-I02" not in _rules(report)

    def test_i03_property_name_case(self):
        report = DesignChecks.run(
            HEALTHY, [_prop(name="WorksAt", localName="WorksAt")], []
        )
        assert "DC-I03" in _rules(report)

    def test_i03_negative(self):
        report = DesignChecks.run(HEALTHY, HEALTHY_PROPS, [])
        assert "DC-I03" not in _rules(report)

    def test_i04_near_duplicate_name(self):
        other = _org(name="Persom", uri="http://ex.org#Persom", label="Persom")
        report = DesignChecks.run([_cls(), other], [], [])
        assert "DC-I04" in _rules(report)

    def test_i04_skips_exact_duplicates(self):
        other = _org(name="person", uri="http://ex.org#Other")
        report = DesignChecks.run([_cls(), other], [], [])
        assert "DC-W05" in _rules(report)
        assert "DC-I04" not in _rules(report)

    def test_i04_negative(self):
        report = DesignChecks.run(HEALTHY, HEALTHY_PROPS, [])
        assert "DC-I04" not in _rules(report)

    def test_i05_deep_hierarchy(self):
        chain = []
        parent = ""
        for i in range(7):
            name = f"L{i}"
            chain.append(
                _cls(
                    name=name,
                    uri=f"http://ex.org#{name}",
                    label=name,
                    description=name,
                    parent=parent,
                )
            )
            parent = name
        report = DesignChecks.run(chain, [], [])
        assert "DC-I05" in _rules(report)

    def test_i05_negative(self):
        report = DesignChecks.run(HEALTHY, HEALTHY_PROPS, [])
        assert "DC-I05" not in _rules(report)


class TestReportShape:
    def test_summary_counts(self):
        report = DesignChecks.run([_cls(parent="Missing", label="")], [], [])
        assert set(report["summary"]) == {"error", "warning", "info"}
        assert report["summary"]["error"] >= 1
        finding = report["findings"][0]
        assert set(finding) >= {"rule", "severity", "kind", "uri", "name", "message"}

    def test_empty_ontology(self):
        report = DesignChecks.run([], [], [])
        assert report["findings"] == []
        assert report["summary"] == {"error": 0, "warning": 0, "info": 0}

    def test_thousand_class_smoke(self):
        classes = [
            _cls(
                name=f"C{i}",
                uri=f"http://ex.org#C{i}",
                label=f"C{i}",
                description=f"class {i}",
                parent=f"C{i - 1}" if i else "",
            )
            for i in range(1000)
        ]
        start = time.perf_counter()
        report = DesignChecks.run(classes, [], [])
        elapsed = time.perf_counter() - start
        assert elapsed < 2.0
        assert "summary" in report
