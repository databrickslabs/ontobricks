"""Tests for OntologyConflictDetector and Ontology append-mode merge helpers."""

import pytest

from back.core.w3c.owl.OntologyConflictDetector import (
    ConflictReport,
    OntologyConflictDetector,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _cls(uri: str, name: str, parent_uri: str = "") -> dict:
    return {"uri": uri, "name": name, "parent_uri": parent_uri}


def _prop(uri: str, name: str, type_: str = "ObjectProperty", range_: str = "") -> dict:
    return {"uri": uri, "name": name, "type": type_, "range": range_}


EXISTING_ONT = {
    "classes": [
        _cls("http://ex.org/A", "A"),
        _cls("http://ex.org/B", "B", "http://ex.org/A"),
    ],
    "properties": [
        _prop("http://ex.org/hasB", "hasB", range_="http://ex.org/B"),
    ],
    "constraints": [],
    "swrl_rules": [],
    "axioms": [],
    "expressions": [],
    "groups": [],
}


# ---------------------------------------------------------------------------
# ConflictDetector — classification
# ---------------------------------------------------------------------------

class TestConflictDetectorClasses:
    def setup_method(self):
        self.det = OntologyConflictDetector()

    def test_new_class_detected(self):
        incoming = {
            "classes": [_cls("http://ex.org/C", "C")],
            "properties": [], "constraints": [], "swrl_rules": [],
            "axioms": [], "expressions": [], "groups": [],
        }
        report = self.det.analyze(EXISTING_ONT, incoming)
        assert len(report.new_items) == 1
        assert report.new_items[0].entity_type == "class"
        assert report.new_items[0].conflict_type == "new"

    def test_duplicate_class_skipped(self):
        incoming = {
            "classes": [_cls("http://ex.org/A", "A")],
            "properties": [], "constraints": [], "swrl_rules": [],
            "axioms": [], "expressions": [], "groups": [],
        }
        report = self.det.analyze(EXISTING_ONT, incoming)
        assert len(report.duplicates) == 1
        assert report.duplicates[0].conflict_type == "duplicate"

    def test_uri_conflict_different_name(self):
        incoming = {
            "classes": [_cls("http://ex.org/A", "Renamed_A")],
            "properties": [], "constraints": [], "swrl_rules": [],
            "axioms": [], "expressions": [], "groups": [],
        }
        report = self.det.analyze(EXISTING_ONT, incoming)
        assert len(report.conflicts) == 1
        assert report.conflicts[0].conflict_type == "uri_conflict"

    def test_name_conflict_different_uri(self):
        incoming = {
            "classes": [_cls("http://other.org/A", "A")],
            "properties": [], "constraints": [], "swrl_rules": [],
            "axioms": [], "expressions": [], "groups": [],
        }
        report = self.det.analyze(EXISTING_ONT, incoming)
        assert len(report.conflicts) == 1
        assert report.conflicts[0].conflict_type == "name_conflict"

    def test_no_conflicts_flag(self):
        incoming = {
            "classes": [_cls("http://ex.org/C", "C")],
            "properties": [], "constraints": [], "swrl_rules": [],
            "axioms": [], "expressions": [], "groups": [],
        }
        report = self.det.analyze(EXISTING_ONT, incoming)
        assert not report.has_conflicts()

    def test_has_conflicts_flag(self):
        incoming = {
            "classes": [_cls("http://ex.org/A", "Renamed_A")],
            "properties": [], "constraints": [], "swrl_rules": [],
            "axioms": [], "expressions": [], "groups": [],
        }
        report = self.det.analyze(EXISTING_ONT, incoming)
        assert report.has_conflicts()


class TestConflictDetectorProperties:
    def setup_method(self):
        self.det = OntologyConflictDetector()

    def test_new_property(self):
        incoming = {
            "classes": [],
            "properties": [_prop("http://ex.org/hasC", "hasC")],
            "constraints": [], "swrl_rules": [],
            "axioms": [], "expressions": [], "groups": [],
        }
        report = self.det.analyze(EXISTING_ONT, incoming)
        prop_new = [i for i in report.new_items if i.entity_type == "property"]
        assert len(prop_new) == 1

    def test_duplicate_property(self):
        incoming = {
            "classes": [],
            "properties": [_prop("http://ex.org/hasB", "hasB", range_="http://ex.org/B")],
            "constraints": [], "swrl_rules": [],
            "axioms": [], "expressions": [], "groups": [],
        }
        report = self.det.analyze(EXISTING_ONT, incoming)
        prop_dup = [i for i in report.duplicates if i.entity_type == "property"]
        assert len(prop_dup) == 1

    def test_property_uri_conflict_different_range(self):
        incoming = {
            "classes": [],
            "properties": [_prop("http://ex.org/hasB", "hasB", range_="http://ex.org/C")],
            "constraints": [], "swrl_rules": [],
            "axioms": [], "expressions": [], "groups": [],
        }
        report = self.det.analyze(EXISTING_ONT, incoming)
        prop_con = [i for i in report.conflicts if i.entity_type == "property"]
        assert len(prop_con) == 1
        assert prop_con[0].conflict_type == "uri_conflict"


# ---------------------------------------------------------------------------
# ConflictReport — serialisation
# ---------------------------------------------------------------------------

class TestConflictReportToDict:
    def test_to_dict_structure(self):
        det = OntologyConflictDetector()
        incoming = {
            "classes": [_cls("http://ex.org/C", "C"), _cls("http://ex.org/A", "A")],
            "properties": [], "constraints": [], "swrl_rules": [],
            "axioms": [], "expressions": [], "groups": [],
        }
        report = det.analyze(EXISTING_ONT, incoming)
        d = report.to_dict()
        assert "new_items" in d
        assert "duplicates" in d
        assert "conflicts" in d
        assert "has_conflicts" in d
        assert "summary" in d
        assert isinstance(d["summary"]["new"], int)

    def test_conflict_item_to_dict_keys(self):
        det = OntologyConflictDetector()
        incoming = {
            "classes": [_cls("http://ex.org/A", "Renamed_A")],
            "properties": [], "constraints": [], "swrl_rules": [],
            "axioms": [], "expressions": [], "groups": [],
        }
        report = det.analyze(EXISTING_ONT, incoming)
        item_d = report.conflicts[0].to_dict()
        assert set(item_d.keys()) == {"entity_type", "uri", "name", "conflict_type", "incoming", "existing"}


# ---------------------------------------------------------------------------
# Ontology._apply_resolutions (unit-level)
# ---------------------------------------------------------------------------

class TestApplyResolutions:
    """Tests for the static merge helper in Ontology (without a full session)."""

    def setup_method(self):
        # Lightweight fake ConflictReport
        det = OntologyConflictDetector()
        existing_ont = {
            "classes": [_cls("http://ex.org/A", "A")],
            "properties": [], "constraints": [], "swrl_rules": [],
            "axioms": [], "expressions": [], "groups": [],
        }
        incoming = {
            "classes": [
                _cls("http://ex.org/B", "B"),              # new
                _cls("http://ex.org/A", "A_renamed"),       # uri_conflict
            ],
            "properties": [], "constraints": [], "swrl_rules": [],
            "axioms": [], "expressions": [], "groups": [],
        }
        self.report   = det.analyze(existing_ont, incoming)
        self.existing = list(existing_ont["classes"])

    def _apply(self, resolutions):
        from back.objects.ontology.Ontology import Ontology
        return Ontology._apply_resolutions("class", self.existing, self.report, resolutions)

    def test_skip_leaves_existing(self):
        result = self._apply({"http://ex.org/A": "skip"})
        uris = [r["uri"] for r in result]
        assert "http://ex.org/A" in uris
        # New item B is appended
        assert "http://ex.org/B" in uris
        # A_renamed should NOT be in the names since we skipped
        names = [r["name"] for r in result]
        assert "A_renamed" not in names

    def test_overwrite_replaces_existing(self):
        result = self._apply({"http://ex.org/A": "overwrite"})
        a_entry = next(r for r in result if r["uri"] == "http://ex.org/A")
        assert a_entry["name"] == "A_renamed"

    def test_rename_appends_with_new_name(self):
        result = self._apply({"http://ex.org/A": "rename:A_v2"})
        names = [r["name"] for r in result]
        assert "A_v2" in names
        # Original A still present
        assert "A" in names

    def test_new_item_always_appended(self):
        result = self._apply({"http://ex.org/A": "skip"})
        uris = [r["uri"] for r in result]
        assert "http://ex.org/B" in uris

    def test_empty_resolution_defaults_to_skip(self):
        result = self._apply({})
        names = [r["name"] for r in result]
        assert "A_renamed" not in names
        assert "A" in names


# ---------------------------------------------------------------------------
# Mixed scenario
# ---------------------------------------------------------------------------

class TestMixedScenario:
    def test_full_mix(self):
        det = OntologyConflictDetector()
        existing = {
            "classes": [_cls("http://ex.org/X", "X")],
            "properties": [_prop("http://ex.org/p1", "p1")],
            "constraints": [], "swrl_rules": [],
            "axioms": [], "expressions": [], "groups": [],
        }
        incoming = {
            "classes": [
                _cls("http://ex.org/X", "X"),          # duplicate
                _cls("http://ex.org/Y", "Y"),           # new
                _cls("http://ex.org/Z", "X"),           # name_conflict
            ],
            "properties": [
                _prop("http://ex.org/p1", "p1"),        # duplicate
                _prop("http://ex.org/p2", "p2"),        # new
            ],
            "constraints": [], "swrl_rules": [],
            "axioms": [], "expressions": [], "groups": [],
        }
        report = det.analyze(existing, incoming)
        d = report.to_dict()
        assert d["summary"]["new"]       == 2   # Y (class) + p2 (prop)
        new_names  = [i.name for i in report.new_items]
        conf_names = [i.name for i in report.conflicts]
        assert "y" in new_names
        assert "p2" in new_names
        assert "x" in conf_names     # name_conflict Z has name "X" lowercased
        assert d["summary"]["duplicates"] == 2  # X class + p1 prop
