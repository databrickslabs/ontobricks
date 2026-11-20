"""Deterministic check of whether a version can support a question."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.objects.ontology.evolution.CoverageState import CoverageState as S
from back.objects.ontology.evolution.Intent import Intent
from back.objects.ontology.evolution.JoinCandidateFinder import JoinCandidateFinder
from back.objects.ontology.evolution.OntologyIndex import Edge, OntologyIndex
from back.objects.ontology.evolution.SourceAccessDenied import SourceAccessDenied
from back.objects.ontology.evolution.SourceProbe import SourceProbe
from back.objects.ontology.evolution.SourceUnavailable import SourceUnavailable

SUPPORTED_FILTER_OPS = ("eq",)
MAX_DANGLING = JoinCandidateFinder.MAX_DANGLING


class CoverageService:
    """Turn an Intent into obligations, each with a state and evidence.

    A report is *ready* only when every required obligation is RESOLVED;
    states are never averaged. Readiness means the preconditions for an
    answer hold on the inspected sources, not that an answer is correct.
    """

    def __init__(self, index: OntologyIndex, probe: SourceProbe):
        self._index = index
        self._probe = probe
        self._finder = JoinCandidateFinder(index, probe)
        self._obligations: List[Dict[str, Any]] = []
        self._evidence: List[Dict[str, Any]] = []
        self._entities: Dict[str, Optional[Dict[str, str]]] = {}

    # -- recording ------------------------------------------------------

    def _ob(self, key: str, kind: str, state: S, reason: str = "", **refs) -> None:
        if any(o["key"] == key for o in self._obligations):
            return
        self._obligations.append(
            {
                "key": key,
                "kind": kind,
                "state": state.value,
                "reason": reason,
                "refs": refs,
            }
        )

    def _ev(
        self, kind: str, source_ref: str, outcome: str, scope: str, **detail
    ) -> str:
        ev_id = f"ev{len(self._evidence) + 1}"
        self._evidence.append(
            {
                "id": ev_id,
                "type": kind,
                "source_ref": source_ref,
                "outcome": outcome,
                "scope": scope,
                **detail,
            }
        )
        return ev_id

    def _guard(self, fn, table: str, *args):
        """Run a probe call; record INACCESSIBLE / SOURCE_UNAVAILABLE instead of raising."""
        try:
            return fn(*args)
        except SourceAccessDenied:
            self._ob(
                f"source:{table.split('.')[-1]}",
                "source",
                S.INACCESSIBLE,
                "access_denied",
            )
        except SourceUnavailable:
            self._ob(
                f"source:{table.split('.')[-1]}",
                "source",
                S.SOURCE_UNAVAILABLE,
                "unavailable",
            )
        return None

    # -- assessment -----------------------------------------------------

    def assess(self, intent: Intent) -> Dict[str, Any]:
        gap: Optional[Dict[str, Any]] = None
        join: Dict[str, Any] = {}
        output = self._output_class(intent)
        self._unsupported(intent)
        if output:
            for flt in intent.filters:
                if flt.cls not in self._index.classes:
                    self._ob(
                        f"class:{flt.cls}",
                        "class",
                        S.MISSING_SEMANTICS,
                        "class_not_in_ontology",
                    )
                    gap = gap or {"kind": "class", "name": flt.cls}
                    continue
                self._filter(flt)
                if flt.cls != output:
                    found = self._path(output, flt.cls, intent)
                    gap = gap or found.get("gap")
                    join = join or found.get("join") or {}
        report = self._report(gap, join)
        return report

    def _output_class(self, intent: Intent) -> Optional[str]:
        if intent.output_class:
            if intent.output_class in self._index.classes:
                self._entity(intent.output_class)
                return intent.output_class
            self._ob(
                f"class:{intent.output_class}",
                "class",
                S.MISSING_SEMANTICS,
                "class_not_in_ontology",
            )
            return None
        filter_classes = {f.cls for f in intent.filters}
        for mention in intent.mentions:
            hits = [
                c
                for c in self._index.resolve_mention(mention)
                if c not in filter_classes
            ]
            if len(hits) > 1:
                self._ob(
                    f"mention:{mention}",
                    "entity",
                    S.AMBIGUOUS,
                    "several_classes",
                    candidates=hits,
                )
                return None
            if len(hits) == 1:
                self._entity(hits[0])
                return hits[0]
        self._ob("output", "entity", S.MISSING_SEMANTICS, "no_output_class")
        return None

    def _unsupported(self, intent: Intent) -> None:
        if intent.aggregate:
            fn = str(intent.aggregate.get("fn") or "aggregate")
            self._ob(
                f"aggregate:{fn}",
                "metric",
                S.UNSUPPORTED,
                "aggregation_not_supported_v1",
            )
        for flt in intent.filters:
            if flt.op not in SUPPORTED_FILTER_OPS:
                symbol = {"gt": ">", "lt": "<", "ge": ">=", "le": "<=", "ne": "!="}.get(
                    flt.op, flt.op
                )
                key = f"filter:{flt.cls}.{flt.attribute}{symbol}{flt.value}"
                self._ob(key, "filter", S.UNSUPPORTED, "filter_op_not_supported_v1")
        for sel in intent.select:
            cls, _, attr = sel.partition(".")
            if not self._index.attribute(cls, attr):
                self._ob(
                    f"attribute:{sel}",
                    "property",
                    S.MISSING_SEMANTICS,
                    "attribute_not_in_ontology",
                )

    def _entity(self, cls: str) -> Optional[Dict[str, str]]:
        if cls not in self._entities:
            self._entities[cls] = self._check_entity(cls)
        return self._entities[cls]

    def _check_entity(self, cls: str) -> Optional[Dict[str, str]]:
        entity = self._index.entity(cls)
        if not entity or not entity["table"] or not entity["id_column"]:
            self._ob(
                f"entity:{cls}", "mapping", S.MISSING_SEMANTICS, "class_not_mapped"
            )
            return None
        if self._guard(self._probe.columns, entity["table"], entity["table"]) is None:
            return None
        unique = self._guard(
            self._probe.key_is_unique,
            entity["table"],
            entity["table"],
            entity["id_column"],
        )
        if unique is False:
            ev = self._ev(
                "mapping_profile",
                entity["table"],
                "fail",
                "full_table",
                check="id_unique",
            )
            self._ob(
                f"entity:{cls}",
                "mapping",
                S.UNVERIFIED,
                "identity_not_unique",
                evidence=[ev],
            )
            return None
        return entity

    def _filter(self, flt) -> None:
        if flt.op not in SUPPORTED_FILTER_OPS:
            return
        if flt.attribute != "id" and not self._index.attribute(flt.cls, flt.attribute):
            self._ob(
                f"attribute:{flt.cls}.{flt.attribute}",
                "property",
                S.MISSING_SEMANTICS,
                "attribute_not_in_ontology",
            )
            return
        column = self._index.attribute_column(flt.cls, flt.attribute)
        entity = self._entity(flt.cls)
        key = f"filter:{flt.cls}.{flt.attribute}={flt.value}"
        if not column:
            self._ob(key, "filter", S.MISSING_SEMANTICS, "attribute_not_mapped")
            return
        if not entity:
            return
        count = self._guard(
            self._probe.count_value, entity["table"], entity["table"], column, flt.value
        )
        if count is None:
            return
        ev = self._ev(
            "mapping_profile",
            f"{entity['table']}.{column}",
            "pass",
            "full_table",
            matches=count,
        )
        # A name (the class's label column) should pick out one entity; other
        # attributes legitimately select several (e.g. a customer's orders).
        if count > 1 and column == entity.get("label_column") and flt.attribute != "id":
            self._ob(
                key,
                "filter",
                S.AMBIGUOUS,
                "value_matches_several_entities",
                evidence=[ev],
                matches=count,
            )
            return
        self._ob(
            key, "filter", S.RESOLVED, "value_checked", evidence=[ev], matches=count
        )

    def _path(self, start: str, goal: str, intent: Intent) -> Dict[str, Any]:
        paths = self._index.paths(start, goal, intent.max_edges)
        if intent.path_hint:
            paths = [
                p for p in paths if set(intent.path_hint) <= {e[0] for e in p}
            ] or paths
        if paths:
            shortest = [p for p in paths if len(p) == len(paths[0])]
            if len(shortest) > 1:
                self._ob(
                    f"path:{start}->{goal}",
                    "relationship",
                    S.AMBIGUOUS,
                    "several_paths",
                    candidates=[[e[0] for e in p] for p in shortest],
                )
                return {}
            return self._check_edges(shortest[0])
        if self._index.paths(start, goal, max_edges=10):
            self._ob(
                f"path:{start}->{goal}",
                "relationship",
                S.UNSUPPORTED,
                "path_exceeds_budget",
            )
            return {}
        return self._missing_link(start, goal, intent)

    def _check_edges(self, path: List[Edge]) -> Dict[str, Any]:
        for name, a, b in path:
            mapping = self._index.relationship_mapping(name)
            rel = self._index.relationships[name]
            if not mapping:
                self._ob(
                    f"mapping:{name}",
                    "mapping",
                    S.MISSING_SEMANTICS,
                    "relationship_not_mapped",
                )
                return self._repair_gap(name, rel)
            src, tgt = self._entity(rel["domain"]), self._entity(rel["range"])
            if not src or not tgt:
                return {}
            profile = self._guard(
                self._probe.mapping_profile,
                src["table"],
                mapping.get("sql_query") or "",
                mapping.get("source_id_column") or "",
                mapping.get("target_id_column") or "",
                src,
                tgt,
            )
            if profile is None:
                return {}
            rows = max(int(profile["rows"]), 1)
            bad = max(profile["dangling_source"], profile["dangling_target"]) / rows
            ev = self._ev(
                "mapping_profile",
                name,
                "fail" if bad > MAX_DANGLING else "pass",
                "full_table",
                **profile,
            )
            if profile["rows"] == 0 or bad > MAX_DANGLING:
                self._ob(
                    f"mapping:{name}",
                    "mapping",
                    S.MISSING_SEMANTICS,
                    "mapping_dangling",
                    evidence=[ev],
                )
                return self._repair_gap(name, rel)
            self._ob(
                f"relationship:{name}",
                "relationship",
                S.RESOLVED,
                "mapping_checked",
                evidence=[ev],
            )
        return {}

    def _repair_gap(self, name: str, rel: Dict[str, Any]) -> Dict[str, Any]:
        cands = self._guarded_candidates(rel["domain"], rel["range"])
        chosen, ambiguous = JoinCandidateFinder.choose(
            [c for c in cands if c["source_class"] == rel["domain"]]
        )
        gap = {"kind": "mapping", "property": name}
        return {"gap": gap, "join": self._join_entry(chosen, cands, ambiguous)}

    def _missing_link(self, start: str, goal: str, intent: Intent) -> Dict[str, Any]:
        left, right = self._index.component(start), self._index.component(goal)
        by_pair: Dict[tuple, List[Dict[str, Any]]] = {}
        for a in left:
            for b in right:
                cands = self._guarded_candidates(a, b)
                if cands:
                    by_pair[(a, b)] = cands
        if len(by_pair) != 1:
            key = f"relationship:{start}->{goal}"
            reason = "no_join_evidence" if not by_pair else "several_class_pairs"
            state = S.MISSING_SEMANTICS if not by_pair else S.AMBIGUOUS
            self._ob(
                key,
                "relationship",
                state,
                reason,
                candidates=[list(p) for p in by_pair],
            )
            return (
                {"gap": {"kind": "relationship", "source": start, "target": goal}}
                if not by_pair
                else {}
            )
        (a, b), cands = next(iter(by_pair.items()))
        chosen, ambiguous = JoinCandidateFinder.choose(cands)
        src, tgt = (
            (chosen["source_class"], chosen["target_class"])
            if chosen
            else (cands[0]["source_class"], cands[0]["target_class"])
        )
        key = f"relationship:{src}->{tgt}"
        if ambiguous:
            self._ob(
                key,
                "relationship",
                S.AMBIGUOUS,
                "several_joins",
                candidates=[_join_label(c) for c in cands],
            )
        else:
            self._ob(
                key, "relationship", S.MISSING_SEMANTICS, "relationship_not_in_ontology"
            )
        if _path_len_with(self._index, start, goal, src, tgt) > intent.max_edges:
            self._ob(
                f"path:{start}->{goal}",
                "relationship",
                S.UNSUPPORTED,
                "path_exceeds_budget",
            )
            return {}
        for cls in (src, tgt):
            self._entity(cls)
        return {
            "gap": {"kind": "relationship", "source": src, "target": tgt},
            "join": self._join_entry(chosen, cands, ambiguous),
        }

    def _guarded_candidates(self, a: str, b: str) -> List[Dict[str, Any]]:
        tables = ",".join(
            t
            for t in (
                (self._index.entity(a) or {}).get("table", ""),
                (self._index.entity(b) or {}).get("table", ""),
            )
            if t
        )
        return self._guard(self._finder.candidates, tables, a, b) or []

    def _join_entry(self, chosen, cands, ambiguous) -> Dict[str, Any]:
        for cand in cands:
            cand["evidence_ids"] = [
                self._ev(
                    kind,
                    _join_label(cand),
                    "pass" if cand["data_valid"] else "fail",
                    "full_table" if kind != "declared_fk" else "catalog_declaration",
                    **({"profile": cand["profile"]} if kind != "declared_fk" else {}),
                )
                for kind in cand["evidence"]
            ]
        return {"chosen": chosen, "ambiguous": ambiguous, "candidates": cands}

    # -- result ---------------------------------------------------------

    def _report(self, gap, join) -> Dict[str, Any]:
        states = {o["key"]: o["state"] for o in self._obligations}
        values = set(states.values())
        ready = bool(states) and values == {S.RESOLVED.value} and not gap
        if S.UNSUPPORTED.value in values or S.INACCESSIBLE.value in values:
            hint = "BLOCKED"
        elif S.SOURCE_UNAVAILABLE.value in values:
            hint = "INCONCLUSIVE"
        elif ready:
            hint = "NO_CHANGE"
        elif S.AMBIGUOUS.value in values:
            hint = "BLOCKED"
        elif gap and (join or {}).get("chosen"):
            hint = "PROPOSE"
        else:
            hint = "ABSTAIN"
        return {
            "schema_version": 1,
            "ready": ready,
            "decision_hint": hint,
            "obligations": self._obligations,
            "states": states,
            "gap": gap,
            "join": join or None,
            "evidence": self._evidence,
        }


def _join_label(cand: Dict[str, Any]) -> str:
    src = cand["source_table"].split(".")[-1]
    tgt = cand["target_table"].split(".")[-1]
    return f"{src}.{cand['source_column']}={tgt}.{cand['target_column']}"


def _path_len_with(index: OntologyIndex, start: str, goal: str, a: str, b: str) -> int:
    """Edges on the shortest start→goal path once an a–b edge exists."""

    def dist(x, y):
        if x == y:
            return 0
        paths = index.paths(x, y, max_edges=10, limit=1)
        return len(paths[0]) if paths else 99

    return min(dist(start, a) + 1 + dist(b, goal), dist(start, b) + 1 + dist(a, goal))
