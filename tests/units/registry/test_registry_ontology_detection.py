"""Unit tests for ``RegistryService`` helpers behind the ontology-only publish.

``version_document_has_ontology`` gates ``DRAFT -> IN-REVIEW`` (a version with
an ontology but no build is publishable) and feeds the ``has_graph`` flag; it
must read both version-document shapes and never raise on a malformed row.
``_version_sort_key`` picks the numeric-latest PUBLISHED version for
``has_graph`` and must tolerate junk.
"""

from __future__ import annotations

from back.objects.registry.RegistryService import RegistryService


# ----------------------------------------------------------------------
# version_document_has_ontology
# ----------------------------------------------------------------------


def test_top_level_ontology_with_classes_is_detected():
    data = {"ontology": {"classes": [{"uri": "urn:C"}]}}
    assert RegistryService.version_document_has_ontology(data) is True


def test_nested_version_ontology_is_detected():
    data = {"versions": {"2": {"ontology": {"classes": [{"uri": "urn:C"}]}}}}
    assert RegistryService.version_document_has_ontology(data, "2") is True


def test_nested_shape_needs_the_matching_version():
    data = {"versions": {"2": {"ontology": {"classes": [{"uri": "urn:C"}]}}}}
    # Without the version hint the nested shape is not inspected.
    assert RegistryService.version_document_has_ontology(data) is False
    # A different version has no ontology.
    assert RegistryService.version_document_has_ontology(data, "1") is False


def test_empty_or_missing_ontology_is_false():
    assert RegistryService.version_document_has_ontology({}) is False
    assert RegistryService.version_document_has_ontology({"ontology": {}}) is False
    assert (
        RegistryService.version_document_has_ontology({"ontology": {"classes": []}})
        is False
    )


def test_malformed_document_never_raises():
    for junk in (None, "nope", 42, [], {"ontology": "oops"}):
        assert RegistryService.version_document_has_ontology(junk) is False


# ----------------------------------------------------------------------
# _version_sort_key (drives numeric-latest PUBLISHED selection)
# ----------------------------------------------------------------------


def test_version_sort_key_orders_numerically_not_lexically():
    versions = ["1.9.0", "1.10.0", "1.2.0"]
    latest = max(versions, key=RegistryService._version_sort_key)
    assert latest == "1.10.0"


def test_version_sort_key_reads_a_version_dict():
    rows = [{"version": "2"}, {"version": "11"}, {"version": "3"}]
    latest = max(rows, key=RegistryService._version_sort_key)
    assert latest["version"] == "11"


def test_version_sort_key_tolerates_malformed_versions():
    assert RegistryService._version_sort_key("not.a.version") == [0]
    assert RegistryService._version_sort_key({"version": None}) == [0]
    assert RegistryService._version_sort_key({}) == [0]
