"""Tests for the CohortBuilder pipeline (Stages 1–6 + materialisation)."""

from typing import List, Dict
from unittest.mock import MagicMock

import pytest

from back.core.graph_analysis.CohortBuilder import CohortBuilder
from back.core.graph_analysis.models import (
    CohortCompat,
    CohortHop,
    CohortLink,
    CohortOutput,
    CohortRule,
    CohortUCTarget,
)
from back.core.triplestore.constants import RDF_TYPE


BASE_URI = "http://acme/"
PERSON = BASE_URI + "Person"
PROJECT = BASE_URI + "Project"
ASSIGNED_TO = BASE_URI + "assignedTo"
STATUS = BASE_URI + "status"
DEPARTMENT = BASE_URI + "department"


def _person_triples(person, project, status, department=None):
    out = [
        {"subject": person, "predicate": RDF_TYPE, "object": PERSON},
        {"subject": person, "predicate": ASSIGNED_TO, "object": project},
        {"subject": person, "predicate": STATUS, "object": status},
    ]
    if department is not None:
        out.append({"subject": person, "predicate": DEPARTMENT, "object": department})
    return out


def _build_test_graph() -> List[Dict[str, str]]:
    """Build a small consulting graph used by every scenario.

    Project P1: Alice (Exempt, R&D), Bob (Exempt, R&D), Carol (Non-Exempt, Sales)
    Project P2: Dave (Exempt, R&D), Eve (Exempt, R&D)
    Project P3: Bob (Exempt, R&D), Frank (Exempt, Sales)   # bridge to cohort 1
    """
    triples: List[Dict[str, str]] = []
    triples += [
        {"subject": "P1", "predicate": RDF_TYPE, "object": PROJECT},
        {"subject": "P2", "predicate": RDF_TYPE, "object": PROJECT},
        {"subject": "P3", "predicate": RDF_TYPE, "object": PROJECT},
    ]
    triples += _person_triples("Alice", "P1", "Exempt", "R&D")
    triples += _person_triples("Bob", "P1", "Exempt", "R&D")
    triples += _person_triples("Carol", "P1", "Non-Exempt", "Sales")
    triples += _person_triples("Dave", "P2", "Exempt", "R&D")
    triples += _person_triples("Eve", "P2", "Exempt", "R&D")
    triples += _person_triples("Frank", "P3", "Exempt", "Sales")
    triples += [
        {"subject": "Bob", "predicate": ASSIGNED_TO, "object": "P3"},  # bridge
    ]
    return triples


def _store_with(triples: List[Dict[str, str]]):
    s = MagicMock()
    s.query_triples.return_value = triples
    return s


def _builder(triples: List[Dict[str, str]]) -> CohortBuilder:
    return CohortBuilder(_store_with(triples), "graph", base_uri=BASE_URI)


class TestStageHelpers:
    def test_count_class_members(self):
        b = _builder(_build_test_graph())
        assert b.count_class_members(PERSON) == 6

    def test_count_link_edges(self):
        b = _builder(_build_test_graph())
        n = b.count_link_edges(
            PERSON, [CohortLink(shared_class=PROJECT, via=ASSIGNED_TO)]
        )
        # P1: Alice-Bob, Alice-Carol, Bob-Carol = 3
        # P2: Dave-Eve = 1
        # P3: Bob-Frank = 1
        assert n == 5

    def test_count_matching_nodes_value_equals(self):
        b = _builder(_build_test_graph())
        match, total = b.count_matching_nodes(
            PERSON,
            [
                CohortCompat(
                    type="value_equals", property=STATUS, value="Exempt"
                )
            ],
        )
        assert total == 6
        assert match == 5  # all but Carol

    def test_sample_property_values(self):
        b = _builder(_build_test_graph())
        vals = b.sample_property_values(PERSON, STATUS, limit=10)
        assert set(vals) == {"Exempt", "Non-Exempt"}


class TestPipeline:
    def _rule(self, **kwargs) -> CohortRule:
        defaults = dict(
            id="exempt-pool",
            label="Exempt pool",
            class_uri=PERSON,
            links=[CohortLink(shared_class=PROJECT, via=ASSIGNED_TO)],
            compatibility=[
                CohortCompat(type="same_value", property=STATUS),
                CohortCompat(type="value_equals", property=STATUS, value="Exempt"),
            ],
            group_type="connected",
            min_size=2,
        )
        defaults.update(kwargs)
        return CohortRule(**defaults)

    def test_consulting_scenario_connected(self):
        b = _builder(_build_test_graph())
        result = b.build(self._rule())

        # Exempt people only: {Alice, Bob, Dave, Eve, Frank} (Carol dropped).
        # P1 brings (Alice, Bob); P2 brings (Dave, Eve); P3 brings (Bob, Frank).
        # Bob connects P1 and P3 → component {Alice, Bob, Frank}.
        # P2 component → {Dave, Eve}.
        # 2 cohorts, sizes 3 and 2.
        assert result.stats.cohort_count == 2
        sizes = sorted([c.size for c in result.cohorts], reverse=True)
        assert sizes == [3, 2]
        all_members = {m for c in result.cohorts for m in c.members}
        assert "Carol" not in all_members
        assert {"Alice", "Bob", "Frank", "Dave", "Eve"} == all_members

    def test_min_size_filters_small_cohorts(self):
        b = _builder(_build_test_graph())
        rule = self._rule(min_size=3)
        result = b.build(rule)
        # Only the 3-member cohort survives.
        assert result.stats.cohort_count == 1
        assert result.cohorts[0].size == 3

    def test_strict_mode_finds_cliques_not_components(self):
        b = _builder(_build_test_graph())
        rule = self._rule(group_type="strict")
        result = b.build(rule)
        # P1 clique: {Alice, Bob}. P2 clique: {Dave, Eve}. P3 clique: {Bob, Frank}.
        # Bridge Bob does NOT promote {Alice, Bob, Frank} to a clique.
        sizes = sorted([c.size for c in result.cohorts], reverse=True)
        assert sizes == [2, 2, 2]

    def test_links_combine_all_intersection(self):
        b = _builder(_build_test_graph())
        rule = self._rule(
            links=[
                CohortLink(shared_class=PROJECT, via=ASSIGNED_TO),
                CohortLink(shared_class=PROJECT, via=ASSIGNED_TO),
            ],
            links_combine="all",
        )
        # Same link twice — intersection equals one link's edges, semantics preserved.
        result = b.build(rule)
        assert result.stats.cohort_count >= 1

    def test_no_links_means_complete_graph_on_survivors(self):
        b = _builder(_build_test_graph())
        rule = self._rule(links=[])
        result = b.build(rule)
        # All 5 Exempt members in one cohort.
        assert result.stats.cohort_count == 1
        assert result.cohorts[0].size == 5

    def test_content_hash_uri_stable_across_runs(self):
        b1 = _builder(_build_test_graph())
        b2 = _builder(_build_test_graph())
        ids1 = sorted([c.id for c in b1.build(self._rule()).cohorts])
        ids2 = sorted([c.id for c in b2.build(self._rule()).cohorts])
        assert ids1 == ids2

    def test_content_hash_changes_when_membership_changes(self):
        rule = self._rule()
        # First run.
        ids_before = sorted([c.id for c in _builder(_build_test_graph()).build(rule).cohorts])
        # Add a new Exempt person on P2 — the {Dave, Eve} cohort grows.
        triples = _build_test_graph() + _person_triples("Grace", "P2", "Exempt", "R&D")
        ids_after = sorted([c.id for c in _builder(triples).build(rule).cohorts])
        assert ids_before != ids_after

    def test_empty_graph_produces_empty_result(self):
        b = _builder([])
        result = b.build(self._rule())
        assert result.stats.cohort_count == 0
        assert result.cohorts == []

    def test_rule_validation_fails_fast(self):
        b = _builder(_build_test_graph())
        bad = CohortRule(id="", label="", class_uri="")
        with pytest.raises(ValueError, match="Invalid cohort rule"):
            b.build(bad)


COMPLIANCE = BASE_URI + "ComplianceType"
GOVERNED_BY = BASE_URI + "governedBy"


def _multi_hop_graph() -> List[Dict[str, str]]:
    """Person --assignedTo--> Project --governedBy--> ComplianceType.

    Two compliance types (CT-Exempt, CT-Standard) with three projects:
      P1, P2 → CT-Exempt; P3 → CT-Standard.
    Five Persons:
      Alice, Bob on P1; Carol on P2; Dave on P3; Eve on P3.

    Expected linkage by ComplianceType:
      CT-Exempt links {Alice, Bob, Carol}; CT-Standard links {Dave, Eve}.
    """
    triples: List[Dict[str, str]] = [
        {"subject": "P1", "predicate": RDF_TYPE, "object": PROJECT},
        {"subject": "P2", "predicate": RDF_TYPE, "object": PROJECT},
        {"subject": "P3", "predicate": RDF_TYPE, "object": PROJECT},
        {"subject": "CT-Exempt", "predicate": RDF_TYPE, "object": COMPLIANCE},
        {"subject": "CT-Standard", "predicate": RDF_TYPE, "object": COMPLIANCE},
        {"subject": "P1", "predicate": GOVERNED_BY, "object": "CT-Exempt"},
        {"subject": "P2", "predicate": GOVERNED_BY, "object": "CT-Exempt"},
        {"subject": "P3", "predicate": GOVERNED_BY, "object": "CT-Standard"},
    ]
    triples += _person_triples("Alice", "P1", "Exempt")
    triples += _person_triples("Bob", "P1", "Exempt")
    triples += _person_triples("Carol", "P2", "Non-Exempt")
    triples += _person_triples("Dave", "P3", "Exempt")
    triples += _person_triples("Eve", "P3", "Exempt")
    return triples


class TestMultiHopPath:
    def _two_hop_link(self) -> CohortLink:
        return CohortLink(
            path=[
                CohortHop(via=ASSIGNED_TO, target_class=PROJECT),
                CohortHop(via=GOVERNED_BY, target_class=COMPLIANCE),
            ]
        )

    def test_two_hop_groups_by_terminal_compliance(self):
        b = _builder(_multi_hop_graph())
        rule = CohortRule(
            id="compliance-pool",
            label="Compliance pool",
            class_uri=PERSON,
            links=[self._two_hop_link()],
            group_type="connected",
            min_size=2,
        )
        result = b.build(rule)
        sizes = sorted([c.size for c in result.cohorts], reverse=True)
        # CT-Exempt: {Alice, Bob, Carol}; CT-Standard: {Dave, Eve}.
        assert sizes == [3, 2]
        all_members = {m for c in result.cohorts for m in c.members}
        assert all_members == {"Alice", "Bob", "Carol", "Dave", "Eve"}

    def test_two_hop_count_link_edges(self):
        b = _builder(_multi_hop_graph())
        n = b.count_link_edges(PERSON, [self._two_hop_link()])
        # CT-Exempt clique of 3 → 3 edges; CT-Standard pair → 1 edge.
        assert n == 4

    def test_legacy_single_hop_still_works(self):
        """Regression: legacy ``shared_class`` + ``via`` rules keep working."""
        b = _builder(_build_test_graph())
        legacy = CohortLink(shared_class=PROJECT, via=ASSIGNED_TO)
        n = b.count_link_edges(PERSON, [legacy])
        assert n == 5  # same expectation as TestStageHelpers.test_count_link_edges

    def test_path_target_class_filters_intermediate_node(self):
        """A hop's ``target_class`` must restrict what counts as the next node."""
        b = _builder(_multi_hop_graph())
        rule = CohortRule(
            id="compliance-pool-strict",
            label="Compliance pool strict",
            class_uri=PERSON,
            # Wrong intermediate class → engine drops every walk after hop 1.
            links=[CohortLink(path=[
                CohortHop(via=ASSIGNED_TO, target_class=COMPLIANCE),
                CohortHop(via=GOVERNED_BY, target_class=COMPLIANCE),
            ])],
            group_type="connected",
            min_size=2,
        )
        result = b.build(rule)
        assert result.stats.cohort_count == 0

    def test_target_class_with_no_rdf_type_triples_is_lenient(self):
        """Regression: when the graph has no ``rdf:type`` triples for a
        hop's ``target_class``, the engine treats the type filter as a
        no-op (same lenient behaviour the pre-multi-hop engine had via
        ``... or None``).  Otherwise valid rules silently yield zero
        edges on data sources that don't materialise types.
        """
        # Six persons assigned to projects, but no rdf:type for projects.
        triples = []
        for p in ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"]:
            triples.append({"subject": p, "predicate": RDF_TYPE, "object": PERSON})
        for p, proj in [
            ("Alice", "P1"), ("Bob", "P1"), ("Carol", "P1"),
            ("Dave", "P2"), ("Eve", "P2"),
            ("Frank", "P3"), ("Bob", "P3"),
        ]:
            triples.append(
                {"subject": p, "predicate": ASSIGNED_TO, "object": proj}
            )
        b = _builder(triples)
        rule = CohortRule(
            id="lenient",
            label="Lenient pool",
            class_uri=PERSON,
            links=[CohortLink(path=[
                CohortHop(via=ASSIGNED_TO, target_class=PROJECT),
            ])],
            min_size=2,
        )
        result = b.build(rule)
        # P1: clique-of-3 (Alice, Bob, Carol) → 3 edges.
        # P2: pair (Dave, Eve) → 1 edge.
        # P3: pair (Bob, Frank) → 1 edge.
        # Bob bridges P1 and P3 → component {Alice, Bob, Carol, Frank}.
        # Plus {Dave, Eve}. Total 2 cohorts.
        assert result.stats.edge_count == 5
        assert result.stats.cohort_count == 2


COMPLIANCE_TYPE_ID = BASE_URI + "complianceTypeId"


def _multi_hop_graph_with_compliance_ids() -> List[Dict[str, str]]:
    """Same shape as :func:`_multi_hop_graph` but each ComplianceType also
    carries a ``complianceTypeId`` literal — exactly the scenario where a
    user wants to filter the *terminal* node by id rather than the source
    Person."""
    triples = _multi_hop_graph()
    triples += [
        {
            "subject": "CT-Exempt",
            "predicate": COMPLIANCE_TYPE_ID,
            "object": "Exempt",
        },
        {
            "subject": "CT-Standard",
            "predicate": COMPLIANCE_TYPE_ID,
            "object": "Standard",
        },
    ]
    return triples


class TestHopWhere:
    """Per-hop ``where`` filters apply at the *target* node of the hop."""

    def _path_with_terminal_where(self, value: str) -> CohortLink:
        return CohortLink(
            path=[
                CohortHop(via=ASSIGNED_TO, target_class=PROJECT),
                CohortHop(
                    via=GOVERNED_BY,
                    target_class=COMPLIANCE,
                    where=[
                        CohortCompat(
                            type="value_equals",
                            property=COMPLIANCE_TYPE_ID,
                            value=value,
                        )
                    ],
                ),
            ]
        )

    def test_terminal_where_keeps_matching_compliance(self):
        """Filter scopes to one terminal → only that compliance's clique."""
        b = _builder(_multi_hop_graph_with_compliance_ids())
        rule = CohortRule(
            id="exempt-only",
            label="Exempt-only compliance pool",
            class_uri=PERSON,
            links=[self._path_with_terminal_where("Exempt")],
            min_size=2,
        )
        result = b.build(rule)
        sizes = sorted([c.size for c in result.cohorts], reverse=True)
        assert sizes == [3]
        members = {m for c in result.cohorts for m in c.members}
        assert members == {"Alice", "Bob", "Carol"}

    def test_terminal_where_blocks_non_matching(self):
        b = _builder(_multi_hop_graph_with_compliance_ids())
        rule = CohortRule(
            id="standard-only",
            label="Standard-only compliance pool",
            class_uri=PERSON,
            links=[self._path_with_terminal_where("Standard")],
            min_size=2,
        )
        result = b.build(rule)
        sizes = sorted([c.size for c in result.cohorts], reverse=True)
        assert sizes == [2]
        members = {m for c in result.cohorts for m in c.members}
        assert members == {"Dave", "Eve"}

    def test_terminal_where_no_match_yields_zero_cohorts(self):
        b = _builder(_multi_hop_graph_with_compliance_ids())
        rule = CohortRule(
            id="ghost",
            label="Non-existent terminal value",
            class_uri=PERSON,
            links=[self._path_with_terminal_where("DoesNotExist")],
            min_size=2,
        )
        result = b.build(rule)
        assert result.stats.cohort_count == 0
        # Source class is *not* filtered, so survivors == class members.
        assert result.stats.survivor_count == 5
        assert result.stats.edge_count == 0

    def test_intermediate_where_filters_mid_path(self):
        """A ``where`` on an intermediate hop scopes which projects survive."""
        triples = _multi_hop_graph()
        # Tag P1 only with a "tier" attribute.
        TIER = BASE_URI + "tier"
        triples.append({"subject": "P1", "predicate": TIER, "object": "Gold"})
        triples.append({"subject": "P2", "predicate": TIER, "object": "Silver"})
        triples.append({"subject": "P3", "predicate": TIER, "object": "Silver"})
        b = _builder(triples)
        rule = CohortRule(
            id="gold-projects",
            label="Gold-tier compliance pool",
            class_uri=PERSON,
            links=[CohortLink(path=[
                CohortHop(
                    via=ASSIGNED_TO,
                    target_class=PROJECT,
                    where=[
                        CohortCompat(
                            type="value_equals",
                            property=TIER,
                            value="Gold",
                        )
                    ],
                ),
                CohortHop(via=GOVERNED_BY, target_class=COMPLIANCE),
            ])],
            min_size=2,
        )
        result = b.build(rule)
        # Only P1 survives intermediate filter; P1 → CT-Exempt → {Alice, Bob}.
        members = {m for c in result.cohorts for m in c.members}
        assert members == {"Alice", "Bob"}

    def test_value_in_at_terminal(self):
        b = _builder(_multi_hop_graph_with_compliance_ids())
        rule = CohortRule(
            id="multi",
            label="Multi-value terminal",
            class_uri=PERSON,
            links=[CohortLink(path=[
                CohortHop(via=ASSIGNED_TO, target_class=PROJECT),
                CohortHop(
                    via=GOVERNED_BY,
                    target_class=COMPLIANCE,
                    where=[
                        CohortCompat(
                            type="value_in",
                            property=COMPLIANCE_TYPE_ID,
                            values=["Exempt", "Standard"],
                        )
                    ],
                ),
            ])],
            min_size=2,
        )
        result = b.build(rule)
        sizes = sorted([c.size for c in result.cohorts], reverse=True)
        assert sizes == [3, 2]

    def test_missing_value_blocks_unless_allow_missing(self):
        """Hop where mirrors compatibility semantics on missing values."""
        # Same graph but only CT-Exempt carries an id.
        triples = _multi_hop_graph()
        triples.append(
            {
                "subject": "CT-Exempt",
                "predicate": COMPLIANCE_TYPE_ID,
                "object": "Exempt",
            }
        )

        # Strict: CT-Standard has no value → its branch is dropped.
        rule_strict = CohortRule(
            id="strict",
            label="Strict",
            class_uri=PERSON,
            links=[CohortLink(path=[
                CohortHop(via=ASSIGNED_TO, target_class=PROJECT),
                CohortHop(
                    via=GOVERNED_BY,
                    target_class=COMPLIANCE,
                    where=[
                        CohortCompat(
                            type="value_equals",
                            property=COMPLIANCE_TYPE_ID,
                            value="Exempt",
                        )
                    ],
                ),
            ])],
            min_size=2,
        )
        members_strict = {
            m for c in _builder(triples).build(rule_strict).cohorts for m in c.members
        }
        assert members_strict == {"Alice", "Bob", "Carol"}

        # Lenient: allow_missing=True → CT-Standard branch survives too.
        rule_lenient = CohortRule(
            id="lenient",
            label="Lenient",
            class_uri=PERSON,
            links=[CohortLink(path=[
                CohortHop(via=ASSIGNED_TO, target_class=PROJECT),
                CohortHop(
                    via=GOVERNED_BY,
                    target_class=COMPLIANCE,
                    where=[
                        CohortCompat(
                            type="value_equals",
                            property=COMPLIANCE_TYPE_ID,
                            value="Exempt",
                            allow_missing=True,
                        )
                    ],
                ),
            ])],
            min_size=2,
        )
        members_lenient = {
            m
            for c in _builder(triples).build(rule_lenient).cohorts
            for m in c.members
        }
        assert members_lenient == {"Alice", "Bob", "Carol", "Dave", "Eve"}

    def test_legacy_link_unaffected_by_hop_where(self):
        """Sanity: hop-where infra must not regress legacy 1-hop links."""
        b = _builder(_build_test_graph())
        legacy = CohortLink(shared_class=PROJECT, via=ASSIGNED_TO)
        n = b.count_link_edges(PERSON, [legacy])
        assert n == 5


class TestOntologyDataNamespaceDrift:
    """The codebase convention is that ontologies declare URIs with the
    base_uri's separator (typically ``…#name``) but data triples are
    loaded into the ``base_uri.rstrip("#") + "/"`` namespace by R2RML
    and the rest of the ingestion pipeline (see
    :class:`SPARQLRuleEngine._build_uri_map`). The cohort engine must
    rewrite property URIs the same way at lookup time, otherwise a
    rule built from the dropdown (which serves ontology URIs) finds
    zero edges on R2RML-loaded data.

    These tests reproduce the production bug observed on the
    ``Consulting`` domain — Persons typed ``…#Person`` but assigned to
    Projects via ``…/PhasProject`` — and assert the engine handles it.
    """

    BASE_HASH = "https://example.com/Consulting#"  # ontology base_uri
    DATA_NS = "https://example.com/Consulting/"   # what data triples use

    def _drifted_graph(self) -> List[Dict[str, str]]:
        """Person/Project graph where:
        - rdf:type uses the *ontology* namespace (with #)
        - object/datatype property predicates use the *data* namespace (with /)
        """
        return [
            {"subject": "Alice", "predicate": RDF_TYPE, "object": self.BASE_HASH + "Person"},
            {"subject": "Bob",   "predicate": RDF_TYPE, "object": self.BASE_HASH + "Person"},
            {"subject": "Carol", "predicate": RDF_TYPE, "object": self.BASE_HASH + "Person"},
            {"subject": "P1",    "predicate": RDF_TYPE, "object": self.BASE_HASH + "Project"},
            {"subject": "P2",    "predicate": RDF_TYPE, "object": self.BASE_HASH + "Project"},
            # Property predicates use the DATA namespace — the bug.
            {"subject": "Alice", "predicate": self.DATA_NS + "PhasProject", "object": "P1"},
            {"subject": "Bob",   "predicate": self.DATA_NS + "PhasProject", "object": "P1"},
            {"subject": "Carol", "predicate": self.DATA_NS + "PhasProject", "object": "P2"},
        ]

    def _builder_with_base_uri(self, triples) -> CohortBuilder:
        return CohortBuilder(
            store=_store_with(triples), graph_name="g", base_uri=self.BASE_HASH
        )

    def test_rule_via_uri_in_ontology_form_finds_data_form_edges(self):
        """Rule built from dropdown stores ``…#PhasProject`` (ontology
        form). Data triples have ``…/PhasProject``. Engine must
        translate at lookup time and produce non-zero edges."""
        b = self._builder_with_base_uri(self._drifted_graph())
        rule = CohortRule(
            id="cohort",
            label="Cohort",
            class_uri=self.BASE_HASH + "Person",
            links=[CohortLink(path=[
                CohortHop(
                    via=self.BASE_HASH + "PhasProject",   # # form
                    target_class=self.BASE_HASH + "Project",
                ),
            ])],
            min_size=2,
        )
        result = b.build(rule)
        # Alice + Bob share P1 → 1 edge → cohort {Alice, Bob}.
        assert result.stats.edge_count == 1
        assert result.stats.cohort_count == 1
        cohort_members = {m for c in result.cohorts for m in c.members}
        assert cohort_members == {"Alice", "Bob"}

    def test_compatibility_property_uri_normalised(self):
        """Compatibility filters carry property URIs in ontology form;
        data triples are in data form. Lookup must align."""
        triples = self._drifted_graph()
        # Add status literals on Persons with the data namespace.
        triples += [
            {"subject": "Alice", "predicate": self.DATA_NS + "status", "object": "Exempt"},
            {"subject": "Bob",   "predicate": self.DATA_NS + "status", "object": "Exempt"},
            {"subject": "Carol", "predicate": self.DATA_NS + "status", "object": "Non-Exempt"},
        ]
        b = self._builder_with_base_uri(triples)
        rule = CohortRule(
            id="exempt-only",
            label="Exempt only",
            class_uri=self.BASE_HASH + "Person",
            links=[CohortLink(path=[
                CohortHop(
                    via=self.BASE_HASH + "PhasProject",
                    target_class=self.BASE_HASH + "Project",
                ),
            ])],
            compatibility=[
                CohortCompat(
                    type="value_equals",
                    property=self.BASE_HASH + "status",   # # form
                    value="Exempt",
                ),
            ],
            min_size=2,
        )
        result = b.build(rule)
        # Compat filters out Carol → only Alice+Bob remain → 1 edge.
        assert result.stats.survivor_count == 2
        assert result.stats.edge_count == 1

    def test_hop_where_property_uri_normalised(self):
        """Per-hop ``where`` filter on a terminal node — property URI
        in ontology form must resolve against data-form predicates."""
        triples = self._drifted_graph()
        # Tag P1 only with tier=Gold (data namespace).
        triples += [
            {"subject": "P1", "predicate": self.DATA_NS + "tier", "object": "Gold"},
            {"subject": "P2", "predicate": self.DATA_NS + "tier", "object": "Silver"},
        ]
        b = self._builder_with_base_uri(triples)
        rule = CohortRule(
            id="gold",
            label="Gold-tier only",
            class_uri=self.BASE_HASH + "Person",
            links=[CohortLink(path=[
                CohortHop(
                    via=self.BASE_HASH + "PhasProject",
                    target_class=self.BASE_HASH + "Project",
                    where=[
                        CohortCompat(
                            type="value_equals",
                            property=self.BASE_HASH + "tier",   # # form
                            value="Gold",
                        )
                    ],
                ),
            ])],
            min_size=2,
        )
        result = b.build(rule)
        # P1 (Gold) keeps Alice + Bob; P2 (Silver) drops Carol.
        members = {m for c in result.cohorts for m in c.members}
        assert members == {"Alice", "Bob"}

    def test_path_trace_reflects_normalisation(self):
        """The trace must show non-zero ``raw`` after normalisation —
        otherwise the "Trace path" diagnostic would still mislead."""
        b = self._builder_with_base_uri(self._drifted_graph())
        out = b.trace_paths(
            self.BASE_HASH + "Person",
            [CohortLink(path=[
                CohortHop(
                    via=self.BASE_HASH + "PhasProject",
                    target_class=self.BASE_HASH + "Project",
                ),
            ])],
        )
        h0 = out["links"][0]["hops"][0]
        assert h0["in_frontier"] == 3       # Alice, Bob, Carol
        assert h0["neighbours_raw"] == 3    # 3 PhasProject triples
        assert h0["out_frontier"] == 2      # P1, P2
        assert out["links"][0]["edge_count"] == 1

    def test_no_base_uri_means_no_normalisation(self):
        """When base_uri isn't configured, the engine must not silently
        rewrite URIs — falls back to exact matching (legacy behaviour)."""
        b = CohortBuilder(
            store=_store_with(self._drifted_graph()), graph_name="g"
        )  # no base_uri
        rule = CohortRule(
            id="cohort",
            label="Cohort",
            class_uri=self.BASE_HASH + "Person",
            links=[CohortLink(path=[
                CohortHop(
                    via=self.BASE_HASH + "PhasProject",
                    target_class=self.BASE_HASH + "Project",
                ),
            ])],
        )
        result = b.build(rule)
        # Without base_uri, the rewrite is skipped → exact-match miss.
        assert result.stats.edge_count == 0

    # --- data loaded with ontology-namespace (# form) predicates -----
    #
    # Reproduces the "ElectricitySuspended / hasClaim returns nothing"
    # production bug: data was not loaded via R2RML but inserted with
    # ontology-namespace predicates (``…#hasClaim`` instead of
    # ``…/hasClaim``).  _normalized_links already converts the *rule*
    # predicate to data form; _outgoing_edge_index must normalise the
    # *triple* predicate to the same form or the lookup silently misses.

    def _graph_with_ontology_form_predicates(self) -> List[Dict[str, str]]:
        """Person --#PhasProject--> Project triples where the predicate
        is in the ontology namespace (``#``), not the data namespace
        (``/``).  This happens when data is inserted outside the R2RML
        pipeline, e.g. direct graph insert or W3C OWL round-trip.
        """
        return [
            {"subject": "Alice", "predicate": RDF_TYPE,
             "object": self.BASE_HASH + "Person"},
            {"subject": "Bob",   "predicate": RDF_TYPE,
             "object": self.BASE_HASH + "Person"},
            {"subject": "Carol", "predicate": RDF_TYPE,
             "object": self.BASE_HASH + "Person"},
            {"subject": "P1", "predicate": RDF_TYPE,
             "object": self.BASE_HASH + "Project"},
            {"subject": "P2", "predicate": RDF_TYPE,
             "object": self.BASE_HASH + "Project"},
            # Predicate in ONTOLOGY (# form) — the new bug scenario.
            {"subject": "Alice", "predicate": self.BASE_HASH + "PhasProject",
             "object": "P1"},
            {"subject": "Bob",   "predicate": self.BASE_HASH + "PhasProject",
             "object": "P1"},
            {"subject": "Carol", "predicate": self.BASE_HASH + "PhasProject",
             "object": "P2"},
        ]

    def test_via_from_foreign_namespace_resolved_by_local_name(self):
        """Regression for the ElectricitySuspended / hasClaim production bug.

        The domain's base_uri is ``https://databricks-ontology.com/Cust360Auto#``
        but ALL object property URIs in the ontology use a shared/default
        namespace ``https://ontobricks.com/ontology#``.  The data triples use
        the domain's data namespace as expected.

        ``_to_data_uri`` cannot bridge two completely different namespaces, so
        ``_resolve_predicate`` must fall back to local-name matching against
        the triples actually in the graph.
        """
        domain_base = "https://databricks-ontology.com/Cust360Auto#"
        domain_data = "https://databricks-ontology.com/Cust360Auto/"
        prop_ns = "https://ontobricks.com/ontology#"   # foreign namespace
        triples = [
            {"subject": "Alice", "predicate": RDF_TYPE,
             "object": domain_base + "Customer"},
            {"subject": "Bob",   "predicate": RDF_TYPE,
             "object": domain_base + "Customer"},
            {"subject": "Carol", "predicate": RDF_TYPE,
             "object": domain_base + "Customer"},
            {"subject": "Claim1", "predicate": RDF_TYPE,
             "object": domain_base + "Claim"},
            {"subject": "Claim1", "predicate": RDF_TYPE,
             "object": domain_base + "Claim"},
            {"subject": "Claim2", "predicate": RDF_TYPE,
             "object": domain_base + "Claim"},
            # Data triples use the domain data namespace.
            {"subject": "Alice", "predicate": domain_data + "hasclaim",
             "object": "Claim1"},
            {"subject": "Bob",   "predicate": domain_data + "hasclaim",
             "object": "Claim1"},
            {"subject": "Carol", "predicate": domain_data + "hasclaim",
             "object": "Claim2"},
        ]
        b = CohortBuilder(
            store=_store_with(triples), graph_name="g", base_uri=domain_base
        )
        rule = CohortRule(
            id="electricity-suspended",
            label="Electricity suspended",
            class_uri=domain_base + "Customer",
            links=[CohortLink(path=[
                CohortHop(
                    via=prop_ns + "hasclaim",     # foreign namespace — the bug
                    target_class=domain_base + "Claim",
                ),
            ])],
            min_size=2,
        )
        result = b.build(rule)
        assert result.stats.edge_count == 1, (
            "Expected 1 edge (Alice↔Bob via Claim1). "
            "neighbours_raw=0 means _resolve_predicate failed to match "
            "the foreign-namespace 'hasclaim' to the domain data predicate."
        )
        assert result.stats.cohort_count == 1
        cohort_members = {m for c in result.cohorts for m in c.members}
        assert cohort_members == {"Alice", "Bob"}

    def test_data_with_ontology_form_predicate_is_indexed_correctly(self):
        """Regression for 'hasClaim returns nothing': data triples carry
        the ontology-namespace (``#``) predicate form instead of the
        data-namespace (``/``) form.  The engine must normalise triple
        predicates when building the outgoing-edge index so the lookup
        (which uses the data form after ``_normalized_links``) succeeds."""
        b = self._builder_with_base_uri(
            self._graph_with_ontology_form_predicates()
        )
        rule = CohortRule(
            id="cohort",
            label="Cohort",
            class_uri=self.BASE_HASH + "Person",
            links=[CohortLink(path=[
                CohortHop(
                    via=self.BASE_HASH + "PhasProject",
                    target_class=self.BASE_HASH + "Project",
                ),
            ])],
            min_size=2,
        )
        result = b.build(rule)
        # Alice + Bob share P1 → cohort {Alice, Bob}; Carol is alone on P2.
        assert result.stats.edge_count == 1, (
            "Expected 1 edge — Alice↔Bob via P1. "
            "neighbours_raw=0 means _outgoing_edge_index failed to "
            "normalise the ontology-form predicate."
        )
        assert result.stats.cohort_count == 1
        cohort_members = {m for c in result.cohorts for m in c.members}
        assert cohort_members == {"Alice", "Bob"}

    def test_trace_shows_nonzero_raw_for_ontology_form_predicate(self):
        """trace_paths must also show neighbours_raw > 0 for the same
        scenario — the 'no neighbours found via this predicate' message
        must NOT be shown when the data simply uses ontology-form URIs."""
        b = self._builder_with_base_uri(
            self._graph_with_ontology_form_predicates()
        )
        out = b.trace_paths(
            self.BASE_HASH + "Person",
            [CohortLink(path=[
                CohortHop(
                    via=self.BASE_HASH + "PhasProject",
                    target_class=self.BASE_HASH + "Project",
                ),
            ])],
        )
        h0 = out["links"][0]["hops"][0]
        assert h0["in_frontier"] == 3, "all 3 persons should be in frontier"
        assert h0["neighbours_raw"] == 3, (
            "must see all 3 ontology-form PhasProject triples after "
            "normalisation — failing here is the root cause of the "
            "ElectricitySuspended / hasClaim diagnosis bug"
        )
        assert h0["out_frontier"] == 2   # P1, P2

    # --- class-URI drift on rdf:type triples -------------------------
    #
    # Reproduces the production "explain says not an instance" bug:
    # the loader emits rdf:type triples with the class URI in the
    # **data** namespace (``…/Person``) but the rule references the
    # **ontology** namespace (``…#Person``). The membership check
    # must match across both forms.

    def _drifted_graph_data_form_types(self) -> List[Dict[str, str]]:
        """Same shape as :meth:`_drifted_graph` but rdf:type objects
        ALSO use the data namespace — mirrors what the user reported
        (``…/Consulting/Person/9`` typed as ``…/Consulting/Person``).
        """
        return [
            {"subject": self.DATA_NS + "Person/Alice",
             "predicate": RDF_TYPE, "object": self.DATA_NS + "Person"},
            {"subject": self.DATA_NS + "Person/Bob",
             "predicate": RDF_TYPE, "object": self.DATA_NS + "Person"},
            {"subject": self.DATA_NS + "Person/Carol",
             "predicate": RDF_TYPE, "object": self.DATA_NS + "Person"},
            {"subject": self.DATA_NS + "Project/P1",
             "predicate": RDF_TYPE, "object": self.DATA_NS + "Project"},
            {"subject": self.DATA_NS + "Project/P2",
             "predicate": RDF_TYPE, "object": self.DATA_NS + "Project"},
            {"subject": self.DATA_NS + "Person/Alice",
             "predicate": self.DATA_NS + "PhasProject",
             "object": self.DATA_NS + "Project/P1"},
            {"subject": self.DATA_NS + "Person/Bob",
             "predicate": self.DATA_NS + "PhasProject",
             "object": self.DATA_NS + "Project/P1"},
            {"subject": self.DATA_NS + "Person/Carol",
             "predicate": self.DATA_NS + "PhasProject",
             "object": self.DATA_NS + "Project/P2"},
        ]

    def test_members_of_class_matches_data_form_rdf_type(self):
        """rdf:type uses ``…/Person`` (data form), rule uses
        ``…#Person`` (ontology form). Must still resolve."""
        b = self._builder_with_base_uri(self._drifted_graph_data_form_types())
        members = b._members_of_class(
            b._load_triples(None, allow_overflow=True),
            self.BASE_HASH + "Person",   # # form
        )
        assert members == {
            self.DATA_NS + "Person/Alice",
            self.DATA_NS + "Person/Bob",
            self.DATA_NS + "Person/Carol",
        }

    def test_members_of_class_matches_mixed_rdf_type(self):
        """Mixed graph: some rdf:type triples in ontology form, some
        in data form. Both forms must be picked up."""
        triples = self._drifted_graph()  # # form for rdf:type
        triples += [
            {"subject": "Dave",
             "predicate": RDF_TYPE,
             "object": self.DATA_NS + "Person"},   # / form
        ]
        b = self._builder_with_base_uri(triples)
        members = b._members_of_class(
            b._load_triples(None, allow_overflow=True),
            self.BASE_HASH + "Person",
        )
        assert members == {"Alice", "Bob", "Carol", "Dave"}

    def test_explain_membership_resolves_data_form_target(self):
        """End-to-end reproducer of the user's bug: target URI in
        data form, rule class_uri in ontology form, rdf:type triples
        in data form. Explain must report ``in_class=True`` (no
        ``not an instance`` reason)."""
        b = self._builder_with_base_uri(self._drifted_graph_data_form_types())
        rule = CohortRule(
            id="cohort",
            label="Cohort",
            class_uri=self.BASE_HASH + "Person",
            links=[CohortLink(path=[
                CohortHop(
                    via=self.BASE_HASH + "PhasProject",
                    target_class=self.BASE_HASH + "Project",
                ),
            ])],
        )
        out = b.explain_membership(
            rule, self.DATA_NS + "Person/Alice"
        )
        assert out["in_class"] is True
        assert "reason" not in out  # no failure message

    def test_explain_diagnoses_wrong_type(self):
        """Target IS in the graph and HAS a type, just not the rule's
        class. The new diagnostic must call that out (and quote the
        actual type) so the user can fix the rule."""
        triples = self._drifted_graph()
        # Bob is a Project not a Person — shouldn't pass class check.
        triples += [
            {"subject": "Misfit",
             "predicate": RDF_TYPE,
             "object": self.BASE_HASH + "Project"},
        ]
        b = self._builder_with_base_uri(triples)
        rule = CohortRule(
            id="cohort",
            label="Cohort",
            class_uri=self.BASE_HASH + "Person",
        )
        out = b.explain_membership(rule, "Misfit")
        assert out["in_class"] is False
        assert "Project" in out["reason"]
        assert "typed as" in out["reason"]

    def test_explain_diagnoses_unknown_uri(self):
        """Target URI doesn't appear in any triple — the diagnostic
        must say so (so the user knows it's a typo / wrong domain)."""
        b = self._builder_with_base_uri(self._drifted_graph())
        rule = CohortRule(
            id="cohort",
            label="Cohort",
            class_uri=self.BASE_HASH + "Person",
        )
        out = b.explain_membership(rule, self.DATA_NS + "Person/Ghost")
        assert out["in_class"] is False
        assert "not found" in out["reason"]

    # --- Bug 1: data triples in ontology form (#) ------------------------
    #
    # When data is inserted outside R2RML (direct insert, W3C round-trip,
    # manual load) the triple predicates may stay in ontology form
    # (``…#hasClaim``) instead of the data form (``…/hasClaim``).
    # The old ``_outgoing_edge_index`` was a @staticmethod that stored raw
    # predicates, so the lookup keyed on ``/hasClaim`` found nothing.
    # After the fix, every predicate is normalised via ``_resolve_predicate``
    # before indexing.

    def _graph_with_ontology_form_predicates(self) -> List[Dict[str, str]]:
        """Graph where data triples carry predicates in ontology (``#``) form
        — simulates direct inserts or W3C OWL round-trips."""
        return [
            {"subject": "Alice", "predicate": RDF_TYPE, "object": self.BASE_HASH + "Person"},
            {"subject": "Bob",   "predicate": RDF_TYPE, "object": self.BASE_HASH + "Person"},
            {"subject": "P1",    "predicate": RDF_TYPE, "object": self.BASE_HASH + "Project"},
            # Predicates in ONTOLOGY form (#) — not the data form (/).
            {"subject": "Alice", "predicate": self.BASE_HASH + "PhasProject", "object": "P1"},
            {"subject": "Bob",   "predicate": self.BASE_HASH + "PhasProject", "object": "P1"},
        ]

    def test_data_with_ontology_form_predicate_is_indexed_correctly(self):
        """``_outgoing_edge_index`` must normalise triple predicates from
        ontology form (``#``) to data form (``/``) so the path walker's
        lookup (``/PhasProject``) finds the triples that were stored under
        ``#PhasProject``."""
        b = self._builder_with_base_uri(self._graph_with_ontology_form_predicates())
        rule = CohortRule(
            id="cohort",
            label="Cohort",
            class_uri=self.BASE_HASH + "Person",
            links=[CohortLink(path=[
                CohortHop(
                    via=self.BASE_HASH + "PhasProject",   # ontology form
                    target_class=self.BASE_HASH + "Project",
                ),
            ])],
            min_size=2,
        )
        result = b.build(rule)
        # Alice and Bob both reach P1 → 1 edge → 1 cohort
        assert result.stats.edge_count == 1
        assert result.stats.cohort_count == 1
        members = {m for c in result.cohorts for m in c.members}
        assert members == {"Alice", "Bob"}

    def test_trace_shows_nonzero_raw_for_ontology_form_predicate(self):
        """``trace_paths`` must report ``neighbours_raw > 0`` when data
        predicates are in ontology form — before the fix it returned 0,
        misleading users into thinking the predicate URI was wrong."""
        b = self._builder_with_base_uri(self._graph_with_ontology_form_predicates())
        out = b.trace_paths(
            self.BASE_HASH + "Person",
            [CohortLink(path=[
                CohortHop(
                    via=self.BASE_HASH + "PhasProject",
                    target_class=self.BASE_HASH + "Project",
                ),
            ])],
        )
        h0 = out["links"][0]["hops"][0]
        assert h0["neighbours_raw"] > 0, (
            "Trace must show non-zero raw neighbours — got 0, "
            "which would falsely trigger the 'no neighbours found' diagnostic"
        )
        assert h0["out_frontier"] > 0

    # --- Bug 2: cross-namespace (foreign) predicate ----------------------
    #
    # Even after Bug 1, rules whose properties live in a *completely
    # different* namespace (e.g. ``ontobricks.com/ontology#hasclaim``
    # in a domain whose base is ``databricks-ontology.com/Cust360Auto#``)
    # were still broken: ``_to_data_uri`` can only bridge ``#`` ↔ ``/``
    # within the SAME base namespace. The fix adds ``_predicate_alias_map``
    # + ``_resolve_predicate`` for a local-name fallback.

    def test_via_from_foreign_namespace_resolved_by_local_name(self):
        """Exact replica of the Cust360Auto production scenario:
        - domain base:        ``databricks-ontology.com/Cust360Auto#``
        - property in rule:   ``ontobricks.com/ontology#hasclaim``  (foreign)
        - data triple pred:   ``databricks-ontology.com/Cust360Auto/hasclaim``
        ``_to_data_uri`` cannot bridge these; the local-name alias fallback
        in ``_resolve_predicate`` must produce the correct result."""
        FOREIGN_NS = "https://ontobricks.com/ontology#"
        DOMAIN_BASE = "https://databricks-ontology.com/Cust360Auto#"
        DOMAIN_DATA = "https://databricks-ontology.com/Cust360Auto/"
        triples = [
            {"subject": "C1",  "predicate": RDF_TYPE, "object": DOMAIN_BASE + "Customer"},
            {"subject": "C2",  "predicate": RDF_TYPE, "object": DOMAIN_BASE + "Customer"},
            {"subject": "Cl1", "predicate": RDF_TYPE, "object": DOMAIN_BASE + "Claim"},
            # Data triples use the DOMAIN's data namespace (not the foreign one).
            {"subject": "C1", "predicate": DOMAIN_DATA + "hasclaim", "object": "Cl1"},
            {"subject": "C2", "predicate": DOMAIN_DATA + "hasclaim", "object": "Cl1"},
        ]
        b = CohortBuilder(
            store=_store_with(triples), graph_name="g", base_uri=DOMAIN_BASE
        )
        rule = CohortRule(
            id="shared-claim",
            label="SharedClaim",
            class_uri=DOMAIN_BASE + "Customer",
            links=[CohortLink(path=[
                CohortHop(
                    via=FOREIGN_NS + "hasclaim",   # foreign namespace
                    target_class=DOMAIN_BASE + "Claim",
                ),
            ])],
            min_size=2,
        )
        result = b.build(rule)
        # C1 and C2 both reach Cl1 → 1 edge → 1 cohort of size 2
        assert result.stats.edge_count == 1
        assert result.stats.cohort_count == 1
        members = {m for c in result.cohorts for m in c.members}
        assert members == {"C1", "C2"}


class TestTracePaths:
    """Per-hop path-trace diagnostic powering the Preview tab's
    *Trace path* button. Verifies that the counters pinpoint **which**
    hop empties the walk under each failure mode."""

    def _two_hop_link(self) -> CohortLink:
        return CohortLink(
            path=[
                CohortHop(via=ASSIGNED_TO, target_class=PROJECT),
                CohortHop(via=GOVERNED_BY, target_class=COMPLIANCE),
            ]
        )

    def test_healthy_path_reports_full_frontiers(self):
        b = _builder(_multi_hop_graph())
        out = b.trace_paths(PERSON, [self._two_hop_link()])
        assert out["class_member_count"] == 5
        assert out["survivor_count"] == 5
        assert len(out["links"]) == 1
        link = out["links"][0]
        assert link["edge_count"] == 4  # CT-Exempt clique 3 + CT-Standard pair 1
        h0, h1 = link["hops"]
        assert h0["in_frontier"] == 5
        assert h0["dropped_type"] == 0
        assert h0["dropped_where"] == 0
        assert h0["out_frontier"] == 3  # P1, P2, P3
        assert h1["in_frontier"] == 3
        assert h1["out_frontier"] == 2  # CT-Exempt, CT-Standard

    def test_compatibility_filtering_shrinks_starting_frontier(self):
        """Stage 3a survivors flow into the trace's starting frontier."""
        b = _builder(_multi_hop_graph())
        # Drop everyone but Alice & Bob (via Person.status="Exempt", and
        # the multi_hop_graph has 4 Exempt persons: Alice, Bob, Dave, Eve).
        compat = [
            CohortCompat(type="value_equals", property=STATUS, value="Exempt"),
        ]
        out = b.trace_paths(PERSON, [self._two_hop_link()], compat)
        assert out["survivor_count"] == 4  # Alice, Bob, Dave, Eve
        link = out["links"][0]
        assert link["starting_frontier"] == 4
        assert link["hops"][0]["in_frontier"] == 4

    def test_wrong_predicate_uri_pinpoints_first_hop(self):
        b = _builder(_multi_hop_graph())
        rule = [CohortLink(path=[
            CohortHop(via=BASE_URI + "doesNotExist", target_class=PROJECT),
            CohortHop(via=GOVERNED_BY, target_class=COMPLIANCE),
        ])]
        out = b.trace_paths(PERSON, rule)
        link = out["links"][0]
        h0, h1 = link["hops"]
        assert h0["in_frontier"] == 5
        assert h0["neighbours_raw"] == 0
        assert h0["out_frontier"] == 0
        # Subsequent hop is reported with all-zeros so the UI can
        # still render one row per declared hop.
        assert h1["in_frontier"] == 0
        assert h1["out_frontier"] == 0
        assert link["edge_count"] == 0

    def test_target_class_mismatch_pinpoints_offending_hop(self):
        """All neighbours rejected by ``target_class`` are reported in
        ``dropped_type`` on the offending hop."""
        b = _builder(_multi_hop_graph())
        rule = [CohortLink(path=[
            CohortHop(via=ASSIGNED_TO, target_class=PROJECT),
            CohortHop(via=GOVERNED_BY, target_class=PERSON),  # wrong target
        ])]
        out = b.trace_paths(PERSON, rule)
        h0, h1 = out["links"][0]["hops"]
        assert h0["out_frontier"] == 3
        assert h1["in_frontier"] == 3
        # CT-Exempt and CT-Standard reachable, but neither is a Person.
        assert h1["dropped_type"] >= 2
        assert h1["out_frontier"] == 0
        assert out["links"][0]["edge_count"] == 0

    def test_hop_where_failure_attributes_to_dropped_where(self):
        """A hop where filter rejecting all neighbours is reported in
        ``dropped_where``, not ``dropped_type`` — that's the user's
        original 'still-zero' scenario, made debuggable."""
        b = _builder(_multi_hop_graph_with_compliance_ids())
        rule = [CohortLink(path=[
            CohortHop(via=ASSIGNED_TO, target_class=PROJECT),
            CohortHop(
                via=GOVERNED_BY,
                target_class=COMPLIANCE,
                where=[
                    CohortCompat(
                        type="value_equals",
                        property=COMPLIANCE_TYPE_ID,
                        value="DoesNotExist",
                    )
                ],
            ),
        ])]
        out = b.trace_paths(PERSON, rule)
        h0, h1 = out["links"][0]["hops"]
        assert h1["where_count"] == 1
        assert h1["neighbours_raw"] >= 2
        assert h1["dropped_where"] == h1["neighbours_raw"]
        assert h1["dropped_type"] == 0
        assert h1["out_frontier"] == 0
        assert out["links"][0]["edge_count"] == 0

    def test_legacy_single_hop_link_traced(self):
        """Legacy 1-hop rules get a single-hop trace with no padding."""
        b = _builder(_build_test_graph())
        out = b.trace_paths(
            PERSON, [CohortLink(shared_class=PROJECT, via=ASSIGNED_TO)]
        )
        link = out["links"][0]
        assert len(link["hops"]) == 1
        assert link["edge_count"] == 5
        assert link["hops"][0]["out_frontier"] >= 1


class TestExplain:
    def test_in_class_in_cohort(self):
        rule = CohortRule(
            id="exempt-pool",
            label="Exempt pool",
            class_uri=PERSON,
            links=[CohortLink(shared_class=PROJECT, via=ASSIGNED_TO)],
            compatibility=[
                CohortCompat(type="value_equals", property=STATUS, value="Exempt")
            ],
        )
        b = _builder(_build_test_graph())
        out = b.explain_membership(rule, "Alice")
        assert out["in_class"] is True
        assert out["compatible"] is True
        assert out["in_cohort"] is not None

    def test_failing_constraint_reported(self):
        rule = CohortRule(
            id="exempt-pool",
            label="Exempt pool",
            class_uri=PERSON,
            links=[CohortLink(shared_class=PROJECT, via=ASSIGNED_TO)],
            compatibility=[
                CohortCompat(type="value_equals", property=STATUS, value="Exempt")
            ],
        )
        b = _builder(_build_test_graph())
        out = b.explain_membership(rule, "Carol")
        assert out["in_class"] is True
        assert out["compatible"] is False
        assert out["failing_constraints"]
        assert out["failing_constraints"][0]["property"] == STATUS

    def test_not_in_class(self):
        rule = CohortRule(
            id="exempt-pool",
            label="Exempt pool",
            class_uri=PERSON,
            links=[],
        )
        b = _builder(_build_test_graph())
        out = b.explain_membership(rule, "P1")
        assert out["in_class"] is False


class TestMaterialisation:
    def test_materialize_to_graph_inserts_expected_triples(self):
        triples = _build_test_graph()
        store = _store_with(triples)
        store.insert_triples = MagicMock(return_value=10)

        rule = CohortRule(
            id="exempt-pool",
            label="Exempt pool",
            class_uri=PERSON,
            links=[CohortLink(shared_class=PROJECT, via=ASSIGNED_TO)],
            compatibility=[
                CohortCompat(type="value_equals", property=STATUS, value="Exempt")
            ],
        )
        b = CohortBuilder(store, "graph", base_uri=BASE_URI)
        result = b.build(rule)
        b.materialize_to_graph(rule, result)

        # First call: delete_cohort_triples on the store.
        # Second call: insert_triples.
        assert store.insert_triples.called
        inserted = store.insert_triples.call_args.args[1]
        # 4 cohort-entity triples per cohort + 1 :inCohort<RuleId> per member.
        cohort_count = result.stats.cohort_count
        member_count = sum(c.size for c in result.cohorts)
        assert len(inserted) == 4 * cohort_count + member_count

        # Membership predicate is rule-scoped (`<base>/inCohort<rule.id>`)
        # so the same graph can host multiple cohort rules without
        # collisions on the predicate column.
        expected_pred = f"{BASE_URI}inCohort{rule.id}"
        membership = [t for t in inserted if t["predicate"] == expected_pred]
        assert len(membership) == member_count
        # And the historic single-predicate form should NOT appear in the
        # output anymore -- guards against accidental regressions.
        legacy_pred = f"{BASE_URI}inCohort"
        assert all(t["predicate"] != legacy_pred for t in inserted)

        # The delete pass uses the same rule-scoped predicate so a
        # re-materialise only wipes this rule's membership triples.
        delete_args = store.delete_cohort_triples.call_args
        assert delete_args is not None
        assert delete_args.args[2] == expected_pred

    def test_materialize_to_uc_chunks_inserts_and_deletes_partition(self):
        triples = _build_test_graph()
        rule = CohortRule(
            id="exempt-pool",
            label="Exempt pool",
            class_uri=PERSON,
            links=[CohortLink(shared_class=PROJECT, via=ASSIGNED_TO)],
            compatibility=[
                CohortCompat(type="value_equals", property=STATUS, value="Exempt")
            ],
            output=CohortOutput(
                graph=True,
                uc_table=CohortUCTarget(
                    catalog="c", schema="s", table_name="cohorts_t"
                ),
            ),
        )
        client = MagicMock()
        b = CohortBuilder(_store_with(triples), "graph", base_uri=BASE_URI)
        result = b.build(rule)
        n = b.materialize_to_uc(rule, result, client, rule.output.uc_table)

        # Expect: CREATE, DELETE, INSERT  (3 statements minimum).
        executed = [c.args[0] for c in client.execute_statement.call_args_list]
        assert any("CREATE TABLE IF NOT EXISTS" in s for s in executed)
        assert any("DELETE FROM" in s for s in executed)
        assert any("INSERT INTO" in s for s in executed)
        # One row per (cohort, member).
        assert n == sum(c.size for c in result.cohorts)
