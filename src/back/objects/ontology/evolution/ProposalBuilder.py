"""Rule-based proposal: turn a coverage gap and its chosen join into typed ops."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from back.objects.ontology.evolution.OntologyIndex import OntologyIndex
from back.objects.ontology.evolution.WarehouseSourceProbe import quote_column, split_fqn


class ProposalBuilder:
    """Build at most one alternative with at most two Evolve operations.

    Mapping SQL is assembled here from validated identifiers only; nothing a
    user or a model typed reaches it as free text.
    """

    SCHEMA_VERSION = 1
    MAX_ALTERNATIVES = 3
    NAME = re.compile(r"^[a-z][A-Za-z0-9]{1,63}$")
    MAX_LABEL = 80
    MAX_DESCRIPTION = 400

    def __init__(self, index: OntologyIndex):
        self._index = index

    def build(self, coverage: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        gap = coverage.get("gap") or {}
        chosen = ((coverage.get("join") or {}).get("chosen")) or None
        if coverage.get("decision_hint") != "PROPOSE" or not chosen:
            return None
        if gap.get("kind") == "relationship":
            name = self._relationship_name(
                chosen["source_class"], chosen["target_class"]
            )
            ops = [
                self._add_relationship(name, chosen),
                self._set_mapping(name, chosen),
            ]
        elif gap.get("kind") == "mapping":
            ops = [self._set_mapping(gap["property"], chosen)]
        else:
            return None
        return {
            "schema_version": self.SCHEMA_VERSION,
            "source": "rule",
            "selected": 0,
            "alternatives": [
                {
                    "operations": ops,
                    "rationale": _rationale(gap, chosen),
                    "evidence_ids": list(chosen.get("evidence_ids") or []),
                }
            ],
        }

    def from_suggestion(
        self, coverage: Dict[str, Any], suggestion: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Turn agent alternatives into typed ops, or None when coverage allows no proposal.

        An alternative only chooses one of the join candidates coverage
        already profiled and, for a new relationship, its name, label and
        description. SQL is still assembled here from the candidate's
        validated identifiers; nothing the model wrote reaches it.
        Unusable alternatives are listed in ``rejected`` with a reason.
        """
        gap = coverage.get("gap") or {}
        join = coverage.get("join") or {}
        if coverage.get("decision_hint") != "PROPOSE" or not join.get("chosen"):
            return None
        cands = list(join.get("candidates") or [])
        alternatives, rejected = [], []
        raw = suggestion.get("alternatives") if isinstance(suggestion, dict) else None
        for i, alt in enumerate(raw if isinstance(raw, list) else []):
            if len(alternatives) >= self.MAX_ALTERNATIVES:
                rejected.append({"index": i, "reason": "over_alternative_limit"})
                continue
            built, reason = self._alternative(gap, join, cands, alt)
            if built:
                alternatives.append(built)
            else:
                rejected.append({"index": i, "reason": reason})
        return {
            "schema_version": self.SCHEMA_VERSION,
            "source": "agent",
            "selected": 0,
            "alternatives": alternatives,
            "rejected": rejected,
        }

    def _alternative(self, gap, join, cands, alt):
        if not isinstance(alt, dict):
            return None, "not_an_object"
        try:
            index = int(alt.get("candidate"))
        except (TypeError, ValueError):
            return None, "candidate_not_an_index"
        if not 0 <= index < len(cands):
            return None, "unknown_candidate"
        cand = cands[index]
        if not cand.get("data_valid") and cand != join.get("chosen"):
            return None, "candidate_failed_data_checks"
        rationale = str(alt.get("rationale") or "").strip()
        if not rationale:
            return None, "missing_rationale"
        if gap.get("kind") == "mapping":
            rel = self._index.relationships.get(gap["property"]) or {}
            if cand["source_class"] != rel.get("domain"):
                return None, "candidate_direction_differs_from_relationship"
            ops = [self._set_mapping(gap["property"], cand)]
        elif gap.get("kind") == "relationship":
            if {cand["source_class"], cand["target_class"]} != {
                gap["source"],
                gap["target"],
            }:
                return None, "candidate_outside_gap"
            name = str(alt.get("name") or "").strip()
            if not self.NAME.match(name):
                return None, "invalid_relationship_name"
            if name in self._taken():
                return None, "relationship_name_taken"
            op = self._add_relationship(name, cand)
            label = str(alt.get("label") or "").strip()
            description = str(alt.get("description") or "").strip()
            if label:
                op["params"]["label"] = label[: self.MAX_LABEL]
            if description:
                op["params"]["description"] = description[: self.MAX_DESCRIPTION]
            ops = [op, self._set_mapping(name, cand)]
        else:
            return None, "no_supported_gap"
        return {
            "operations": ops,
            "rationale": rationale[: self.MAX_DESCRIPTION],
            "evidence_ids": list(cand.get("evidence_ids") or []),
            "candidate": index,
            "join": _join_text(cand),
            "citations": list(alt.get("citations") or []),
        }, ""

    def _taken(self) -> set:
        taken = set(self._index.relationships) | set(self._index.classes)
        for attr_holder in self._index.classes.values():
            taken.update(a.get("name") for a in attr_holder.get("dataProperties") or [])
        return taken

    def _relationship_name(self, source: str, target: str) -> str:
        base = "has" + target
        taken = self._taken()
        name, n = base, 2
        while name in taken:
            name, n = f"{base}{n}", n + 1
        return name

    def _add_relationship(self, name: str, chosen: Dict[str, Any]) -> Dict[str, Any]:
        src, tgt = chosen["source_class"], chosen["target_class"]
        return {
            "op": "add_relationship",
            "target": {"name": name, "kind": "relationship"},
            "params": {
                "uri": f"{self._index.base_uri}{name}",
                "domain": src,
                "range": tgt,
                "label": f"has {_words(tgt)}",
                "description": f"Each {_words(src)} refers to one {_words(tgt)}.",
            },
        }

    @staticmethod
    def _set_mapping(name: str, chosen: Dict[str, Any]) -> Dict[str, Any]:
        sid = chosen["source_id_column"]
        tid = chosen["target_id_column"]
        col, tcol = chosen["source_column"], chosen["target_column"]
        src, tgt = chosen["source_table"], chosen["target_table"]
        for ident in (sid, tid, col, tcol):
            quote_column(ident)
        src, tgt = ".".join(split_fqn(src)), ".".join(split_fqn(tgt))
        if tcol == tid:
            sql = f"SELECT {sid}, {col} FROM {src}"
            target_id_column = col
        else:
            sql = (
                f"SELECT s.{sid} AS {sid}, t.{tid} AS {tid} "
                f"FROM {src} s JOIN {tgt} t ON s.{col} = t.{tcol}"
            )
            target_id_column = tid
        return {
            "op": "set_relationship_mapping",
            "target": {"name": name, "kind": "relationship"},
            "params": {
                "sql_query": sql,
                "source_id_column": sid,
                "target_id_column": target_id_column,
                "source_table": src.split(".")[-1],
                "target_table": tgt.split(".")[-1],
            },
        }


def _words(name: str) -> str:
    out: List[str] = []
    for ch in name:
        if ch.isupper() and out:
            out.append(" ")
        out.append(ch.lower())
    return "".join(out)


def _join_text(cand: Dict[str, Any]) -> str:
    return (
        f"{cand['source_table'].split('.')[-1]}.{cand['source_column']} = "
        f"{cand['target_table'].split('.')[-1]}.{cand['target_column']}"
    )


def _rationale(gap: Dict[str, Any], chosen: Dict[str, Any]) -> str:
    join = _join_text(chosen)
    evidence = ", ".join(chosen.get("evidence") or [])
    if gap.get("kind") == "mapping":
        return f"Remap {gap['property']} on {join} ({evidence})."
    return (
        f"Link {chosen['source_class']} to {chosen['target_class']} on {join} ({evidence}). "
        "The join exists in the data; its business meaning still needs review."
    )
