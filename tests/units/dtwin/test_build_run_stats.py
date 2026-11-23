"""Ontology / mapping stats and step timings recorded with a build run."""

from __future__ import annotations

from types import SimpleNamespace

from back.objects.digitaltwin.build import BuildRunStats


class TestCollectDomainStats:
    """Ontology + mapping stat block recorded with a build run."""

    def test_counts_ontology_and_mapping(self):
        ontology = {
            "classes": [{"uri": "C1"}, {"uri": "C2"}],
            "properties": [
                {"type": "ObjectProperty"},
                {"type": "ObjectProperty"},
                {"type": "DatatypeProperty"},
            ],
            "constraints": [{"x": 1}],
        }
        assignment = {
            "entities": [{"excluded": False}, {"excluded": True}],
            "relationships": [{"excluded": False}],
        }
        stats = BuildRunStats.domain_stats(
            ontology, assignment, if_then_rules=[{"r": 1}], axioms=[], shacl_shapes=[{}]
        )

        assert stats["ontology"] == {
            "classes": 2,
            "properties": 3,
            "object_properties": 2,
            "attributes": 1,
            "constraints": 1,
            "if_then_rules": 1,
            "axioms": 0,
            "shacl_shapes": 1,
        }
        assert stats["mapping"]["entity_mappings"] == 2
        assert stats["mapping"]["excluded_entities"] == 1
        assert stats["mapping"]["active_entity_mappings"] == 1
        assert stats["mapping"]["relationship_mappings"] == 1
        assert stats["mapping"]["active_relationship_mappings"] == 1

    def test_empty_inputs_yield_zeros(self):
        stats = BuildRunStats.domain_stats(None, None)
        assert stats["ontology"]["classes"] == 0
        assert stats["mapping"]["entity_mappings"] == 0

    def test_legacy_assignment_keys(self):
        assignment = {
            "data_source_mappings": [{"excluded": False}],
            "relationship_mappings": [{"excluded": False}, {"excluded": False}],
        }
        stats = BuildRunStats.domain_stats({}, assignment)
        assert stats["mapping"]["entity_mappings"] == 1
        assert stats["mapping"]["relationship_mappings"] == 2


class TestStepTimesFromTask:
    """phase_times mirrors the UI build log (step description -> duration)."""

    def test_computes_per_step_durations(self):
        steps = [
            SimpleNamespace(
                name="prepare",
                description="Generating SQL",
                status="completed",
                started_at="2026-06-03T08:00:00+00:00",
                completed_at="2026-06-03T08:00:02+00:00",
            ),
            SimpleNamespace(
                name="graph",
                description="Populating graph",
                status="completed",
                started_at="2026-06-03T08:00:02+00:00",
                completed_at="2026-06-03T08:00:07.5+00:00",
            ),
        ]
        task = SimpleNamespace(steps=steps)
        out = BuildRunStats.step_times(task)
        assert out == {"Generating SQL": 2.0, "Populating graph": 5.5}

    def test_skips_steps_without_start(self):
        steps = [
            SimpleNamespace(
                name="pending",
                description="Not started",
                status="pending",
                started_at=None,
                completed_at=None,
            ),
        ]
        out = BuildRunStats.step_times(SimpleNamespace(steps=steps))
        assert out == {}

    def test_no_steps_returns_empty(self):
        assert BuildRunStats.step_times(SimpleNamespace(steps=[])) == {}
        assert BuildRunStats.step_times(None) == {}
