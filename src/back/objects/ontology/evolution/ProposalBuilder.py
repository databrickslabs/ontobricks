"""Rule-based proposal: turn a coverage gap and its chosen join into typed ops."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.objects.ontology.evolution.OntologyIndex import OntologyIndex
from back.objects.ontology.evolution.WarehouseSourceProbe import quote_column, split_fqn


class ProposalBuilder:
    """Build at most one alternative with at most two Evolve operations.

    Mapping SQL is assembled here from validated identifiers only; nothing a
    user or a model typed reaches it as free text.
    """

    SCHEMA_VERSION = 1

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

    def _relationship_name(self, source: str, target: str) -> str:
        base = "has" + target
        taken = set(self._index.relationships) | set(self._index.classes)
        for attr_holder in self._index.classes.values():
            taken.update(a.get("name") for a in attr_holder.get("dataProperties") or [])
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


def _rationale(gap: Dict[str, Any], chosen: Dict[str, Any]) -> str:
    join = (
        f"{chosen['source_table'].split('.')[-1]}.{chosen['source_column']} = "
        f"{chosen['target_table'].split('.')[-1]}.{chosen['target_column']}"
    )
    evidence = ", ".join(chosen.get("evidence") or [])
    if gap.get("kind") == "mapping":
        return f"Remap {gap['property']} on {join} ({evidence})."
    return (
        f"Link {chosen['source_class']} to {chosen['target_class']} on {join} ({evidence}). "
        "The join exists in the data; its business meaning still needs review."
    )
