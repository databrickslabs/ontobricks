"""Ontology / mapping counts and step timings recorded with every build run."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, Optional


class BuildRunStats:
    """Pure helpers that shape the ``stats`` and ``phase_times`` of a build-run trace."""

    @staticmethod
    def domain_stats(
        ontology: Optional[Dict[str, Any]],
        assignment: Optional[Dict[str, Any]],
        *,
        constraints=None,
        if_then_rules=None,
        axioms=None,
        shacl_shapes=None,
    ) -> Dict[str, Any]:
        """Ontology + mapping statistics recorded with a build run.

        Mirrors the counts shown in the domain Cockpit so the build trace
        carries the same ontology/mapping picture that was live at build
        time. Pure and defensive: never raises; missing data yields zeros.
        """
        try:
            ont = ontology or {}
            classes = ont.get("classes", []) or []
            properties = ont.get("properties", []) or []
            obj_props = [p for p in properties if p.get("type") == "ObjectProperty"]
            attr_props = [p for p in properties if p.get("type") != "ObjectProperty"]

            asg = assignment or {}
            entities = asg.get("entities", asg.get("data_source_mappings", [])) or []
            relationships = (
                asg.get("relationships", asg.get("relationship_mappings", [])) or []
            )
            excluded_ent = [m for m in entities if m.get("excluded")]
            excluded_rel = [m for m in relationships if m.get("excluded")]

            # Default constraints to the ontology-embedded list when not passed.
            cons = (
                constraints if constraints is not None else ont.get("constraints", [])
            )

            return {
                "ontology": {
                    "classes": len(classes),
                    "properties": len(properties),
                    "object_properties": len(obj_props),
                    "attributes": len(attr_props),
                    "constraints": len(cons or []),
                    "if_then_rules": len(if_then_rules or []),
                    "axioms": len(axioms or []),
                    "shacl_shapes": len(shacl_shapes or []),
                },
                "mapping": {
                    "entity_mappings": len(entities),
                    "relationship_mappings": len(relationships),
                    "excluded_entities": len(excluded_ent),
                    "excluded_relationships": len(excluded_rel),
                    "active_entity_mappings": len(entities) - len(excluded_ent),
                    "active_relationship_mappings": (
                        len(relationships) - len(excluded_rel)
                    ),
                },
            }
        except Exception:  # noqa: BLE001
            return {}

    @staticmethod
    def _parse_iso(ts: str) -> Optional[datetime]:
        """Lenient ISO-8601 parse that tolerates 1-2 digit fractional seconds.

        Python 3.9's ``datetime.fromisoformat`` only accepts fractional
        seconds with exactly 3 or 6 digits; timestamps like
        ``...07.5+00:00`` would otherwise raise. We pad the fraction to 6
        digits before parsing.
        """
        if not ts:
            return None
        try:
            return datetime.fromisoformat(ts)
        except (ValueError, TypeError):
            padded = re.sub(
                r"\.(\d{1,6})",
                lambda m: "." + (m.group(1) + "000000")[:6],
                ts,
                count=1,
            )
            try:
                return datetime.fromisoformat(padded)
            except (ValueError, TypeError):
                return None

    @staticmethod
    def step_times(task) -> Dict[str, float]:
        """Per-step wall-clock durations (seconds) keyed by step description.

        Mirrors exactly the "steps + duration" the build UI renders from the
        TaskManager step list, so the recorded ``phase_times`` matches what
        the user saw during the build. Pure and defensive.
        """
        out: Dict[str, float] = {}
        try:
            steps = getattr(task, "steps", None) or []
            for step in steps:
                started = getattr(step, "started_at", None)
                if not started:
                    continue
                completed = getattr(step, "completed_at", None) or started
                t0 = BuildRunStats._parse_iso(started)
                t1 = BuildRunStats._parse_iso(completed)
                if t0 is None or t1 is None:
                    continue
                label = (
                    getattr(step, "description", None)
                    or getattr(step, "name", None)
                    or "step"
                )
                out[str(label)] = round(max(0.0, (t1 - t0).total_seconds()), 3)
        except Exception:  # noqa: BLE001
            return {}
        return out
