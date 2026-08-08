"""Deterministic domain-complexity assessment for engine selection.

The supervisor must decide, for a given domain, whether to run the heavyweight
PGE loop (planner → generator → evaluator → critic, multi-attempt, multi-replan)
or the lightweight single-agent engine. That decision is **deterministic** — it
is computed here from the source metadata + the generated ontology, never left
to an LLM's discretion — and then *exposed* to the Agent Bricks Supervisor as a
tool it calls before routing (see ``agent_supervisor/mas.py``).

Why deterministic: a Multi-Agent Supervisor routes semantically over agent
descriptions, which is fine for "which specialist answers this question" but
unreliable for "is this domain complex enough to warrant the expensive engine".
We keep the hard threshold in code, register it as a Unity Catalog function
(``uc_function.sql``), and let the supervisor's natural-language instructions act
on its structured recommendation.

The signals that make a domain *hard to map* — and therefore worth the PGE loop:

* **Many source tables** — more SQL surface to plan and validate.
* **Cross-source reconciliation** — the same real-world entity realised by
  several tables (one per source system / region / tenant) whose keys and column
  names disagree. This is exactly what the PGE planner + semantic critic exist
  for; the simple engine has no notion of it.
* **A large ontology** — many classes / object properties means many mapping
  items, each a chance for a dangling endpoint the evaluator must catch.
* **Schema heterogeneity** — divergent naming conventions across tables (e.g.
  ``MOTHER_NHS_NO`` vs ``mother_nhs_number``) signal multi-source feeds that need
  normalization the PGE engine performs.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List

from agents.pge_eval.normalize import (
    normalize_metadata,
    normalize_name,
    normalize_ontology,
)
from back.core.logging import get_logger

logger = get_logger(__name__)

# --- Tunable weights (sum to 1.0) and the decision threshold ----------------
# Kept as module constants so the threshold is auditable and adjustable without
# touching logic. The UC function mirrors these values.
WEIGHT_TABLES = 0.20
WEIGHT_CLASSES = 0.20
WEIGHT_RELATIONSHIPS = 0.15
WEIGHT_CROSS_SOURCE = 0.30
WEIGHT_HETEROGENEITY = 0.15

# Saturation points — the count at which a signal contributes its full weight.
SATURATE_TABLES = 5
SATURATE_CLASSES = 12
SATURATE_RELATIONSHIPS = 10

# A domain scoring at/above this is routed to the PGE engine.
COMPLEXITY_THRESHOLD = 0.45

# An id-like column appearing in 2+ tables is the strongest cheap signal of
# cross-source reconciliation (the same entity keyed across feeds).
_ID_COLUMN_RE = re.compile(r"(^|_)(id|no|number|key|code|nhs|mrn|uuid)$")


@dataclass
class ComplexityReport:
    """Structured, JSON-serialisable complexity verdict for one domain."""

    score: float
    tier: str  # "simple" | "complex"
    recommended_engine: str  # "simple" | "pge"
    signals: Dict[str, float] = field(default_factory=dict)
    rationale: str = ""

    def to_dict(self) -> dict:
        return {
            "score": round(self.score, 4),
            "tier": self.tier,
            "recommended_engine": self.recommended_engine,
            "signals": self.signals,
            "rationale": self.rationale,
        }


class ComplexityAssessor:
    """Score a domain's mapping complexity and recommend an engine.

    Stateless; use the module-level :func:`assess` for the common path.
    """

    @staticmethod
    def assess(metadata: dict, ontology: dict) -> ComplexityReport:
        """Return a :class:`ComplexityReport` for *metadata* + *ontology*.

        Both arguments accept the same shapes the rest of the pipeline uses
        (agent or registry shape); parsing is delegated to ``pge_eval.normalize``
        so this stays consistent with the evaluator.
        """
        tables = normalize_metadata(metadata or {})
        onto = normalize_ontology(ontology or {})

        n_tables = len(tables)
        n_columns = sum(len(t.get("columns") or []) for t in tables)
        n_classes = len(onto.classes)
        n_relationships = len(onto.object_properties)

        cross_source = ComplexityAssessor._cross_source_score(tables)
        heterogeneity = ComplexityAssessor._heterogeneity_score(tables)

        s_tables = min(n_tables / SATURATE_TABLES, 1.0)
        s_classes = min(n_classes / SATURATE_CLASSES, 1.0)
        s_rels = min(n_relationships / SATURATE_RELATIONSHIPS, 1.0)

        score = (
            WEIGHT_TABLES * s_tables
            + WEIGHT_CLASSES * s_classes
            + WEIGHT_RELATIONSHIPS * s_rels
            + WEIGHT_CROSS_SOURCE * cross_source
            + WEIGHT_HETEROGENEITY * heterogeneity
        )

        is_complex = score >= COMPLEXITY_THRESHOLD
        tier = "complex" if is_complex else "simple"
        engine = "pge" if is_complex else "simple"

        signals = {
            "n_tables": n_tables,
            "n_columns": n_columns,
            "n_classes": n_classes,
            "n_relationships": n_relationships,
            "cross_source": round(cross_source, 4),
            "heterogeneity": round(heterogeneity, 4),
        }
        rationale = ComplexityAssessor._rationale(tier, signals, score)
        logger.info(
            "Complexity assessment — score=%.3f tier=%s engine=%s signals=%s",
            score,
            tier,
            engine,
            signals,
        )
        return ComplexityReport(
            score=score,
            tier=tier,
            recommended_engine=engine,
            signals=signals,
            rationale=rationale,
        )

    @staticmethod
    def _cross_source_score(tables: List[dict]) -> float:
        """Fraction-style [0,1] signal that the same entity spans several tables.

        Strongest evidence is an id-like column shared across multiple tables
        (the join key reconciling feeds). We also treat a high table-to-shared-
        key ratio as cross-source. Returns 0.0 for a single table.
        """
        if len(tables) < 2:
            return 0.0

        # Detect id-likeness on the raw (lowercased) name so the ``_id``/``_no``
        # suffix boundary survives, but GROUP by the normalized name so the same
        # key written differently across feeds (MOTHER_NHS_NO vs mother_nhs_no)
        # counts as one shared key.
        id_col_tables: Dict[str, int] = {}
        for t in tables:
            seen_in_table = set()
            for col in t.get("columns") or []:
                raw = (col or "").lower()
                key = normalize_name(col)
                if _ID_COLUMN_RE.search(raw) and key and key not in seen_in_table:
                    id_col_tables[key] = id_col_tables.get(key, 0) + 1
                    seen_in_table.add(key)

        shared_keys = [k for k, n in id_col_tables.items() if n >= 2]
        if not shared_keys:
            return 0.0

        # How widely is the most-shared key spread across tables?
        max_spread = max(id_col_tables[k] for k in shared_keys)
        spread_ratio = max_spread / len(tables)
        # Presence of a shared key is itself meaningful; spread scales it up.
        return min(0.5 + 0.5 * spread_ratio, 1.0)

    @staticmethod
    def _heterogeneity_score(tables: List[dict]) -> float:
        """[0,1] signal of divergent column-naming conventions across tables.

        Mixed UPPER/lower/camel/snake conventions across feeds is a hallmark of
        multi-source data needing the PGE engine's normalization. Returns the
        fraction of distinct conventions observed beyond the first.
        """
        if len(tables) < 2:
            return 0.0

        conventions = set()
        for t in tables:
            for col in t.get("columns") or []:
                conventions.add(_naming_convention(col))
        conventions.discard("other")
        if not conventions:
            return 0.0
        # 1 convention → homogeneous (0.0); each extra convention adds signal.
        return min((len(conventions) - 1) / 3.0, 1.0)

    @staticmethod
    def _rationale(tier: str, signals: Dict[str, float], score: float) -> str:
        drivers = []
        if signals["cross_source"] > 0:
            drivers.append("a shared key across multiple tables (cross-source reconciliation)")
        if signals["n_tables"] >= SATURATE_TABLES:
            drivers.append(f"{signals['n_tables']} source tables")
        if signals["n_classes"] >= SATURATE_CLASSES:
            drivers.append(f"{signals['n_classes']} ontology classes")
        if signals["heterogeneity"] > 0:
            drivers.append("heterogeneous column-naming across feeds")
        driver_text = "; ".join(drivers) if drivers else "a small, single-source schema"
        return (
            f"Score {score:.2f} ({tier}). Drivers: {driver_text}. "
            f"Recommended engine: {'PGE loop' if tier == 'complex' else 'simple single-agent'}."
        )


def _naming_convention(name: str) -> str:
    """Classify a raw column name's casing convention."""
    if not name:
        return "other"
    if "_" in name:
        return "upper_snake" if name.isupper() else "snake"
    if name.isupper():
        return "upper"
    if name[0].islower() and any(c.isupper() for c in name):
        return "camel"
    if name.islower():
        return "lower"
    return "other"


def assess(metadata: dict, ontology: dict) -> ComplexityReport:
    """Module-level convenience wrapper over :meth:`ComplexityAssessor.assess`."""
    return ComplexityAssessor.assess(metadata, ontology)
