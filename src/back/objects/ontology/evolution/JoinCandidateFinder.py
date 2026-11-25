"""Find and rank source joins that could back a missing relationship."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from back.objects.ontology.evolution.OntologyIndex import OntologyIndex
from back.objects.ontology.evolution.SourceProbe import SourceProbe

_REF_SUFFIXES = ("_ref", "_code", "_key", "_number", "_sku")
_STRING_TYPES = ("STRING", "VARCHAR", "CHAR")
_MAX_OVERLAP_PROBES = 15


def key_stem(column: str) -> str:
    """Key column name without a per-table prefix: ``l_suppkey`` / ``s_suppkey`` → ``suppkey``.

    Warehouse schemas often prefix every column with a table code (TPC-H,
    many ERPs), so the foreign key and the key it references never share
    a name. Stems shorter than four characters never match.
    """
    name = str(column or "").lower()
    head, sep, tail = name.partition("_")
    stem = tail if sep and len(head) <= 3 and tail else name
    return stem if len(stem) >= 4 else ""


class JoinCandidateFinder:
    """Rank candidate joins between two mapped classes, with their evidence.

    Evidence, strongest first: a declared foreign key, a join whose data
    checks pass on the current source tables, a column named after the
    target's id. Two equally strong candidates are reported as ambiguous
    rather than picked by name. A join that runs is not proof of meaning;
    the business meaning still needs review.
    """

    MAX_DANGLING = 0.05

    def __init__(self, index: OntologyIndex, probe: SourceProbe):
        self._index = index
        self._probe = probe
        self._columns: Dict[str, Dict[str, str]] = {}
        self._fks: Dict[str, List[Dict[str, str]]] = {}

    def _cols(self, table: str) -> Dict[str, str]:
        if table not in self._columns:
            self._columns[table] = self._probe.columns(table)
        return self._columns[table]

    def _foreign_keys(self, table: str) -> List[Dict[str, str]]:
        if table not in self._fks:
            self._fks[table] = self._probe.foreign_keys(table)
        return self._fks[table]

    def candidates(self, a: str, b: str) -> List[Dict[str, Any]]:
        """Candidates in both directions between classes *a* and *b*."""
        return self._directed(a, b) + self._directed(b, a)

    def _directed(self, src: str, tgt: str) -> List[Dict[str, Any]]:
        es, et = self._index.entity(src), self._index.entity(tgt)
        if not es or not et or not es["table"] or not et["table"]:
            return []
        src_cols, tgt_cols = self._cols(es["table"]), self._cols(et["table"])
        found: Dict[Tuple[str, str], Dict[str, Any]] = {}

        def add(column: str, target_column: str, evidence: str) -> None:
            key = (column, target_column)
            if key not in found:
                found[key] = {
                    "source_class": src,
                    "target_class": tgt,
                    "source_table": es["table"],
                    "source_id_column": es["id_column"],
                    "source_column": column,
                    "target_table": et["table"],
                    "target_column": target_column,
                    "target_id_column": et["id_column"],
                    "evidence": [],
                }
            if evidence not in found[key]["evidence"]:
                found[key]["evidence"].append(evidence)

        for fk in self._foreign_keys(es["table"]):
            if fk["ref_table"] == et["table"] and fk["column"] in src_cols:
                add(fk["column"], fk["ref_column"], "declared_fk")
        if et["id_column"] in src_cols and et["id_column"] != es["id_column"]:
            add(et["id_column"], et["id_column"], "name_match")
        for column in src_cols:
            if column not in (es["id_column"], et["id_column"]) and key_stem(
                column
            ) == key_stem(et["id_column"]):
                add(column, et["id_column"], "key_name_match")
        self._value_overlap_candidates(src_cols, tgt_cols, es, et, add)

        target_ids_unique = self._probe.key_is_unique(et["table"], et["id_column"])
        out = []
        for cand in found.values():
            cand["profile"] = self._probe.join_profile(
                cand["source_table"],
                cand["source_column"],
                cand["target_table"],
                cand["target_column"],
            )
            cand["reasons"] = self._problems(cand["profile"], target_ids_unique)
            cand["data_valid"] = not cand["reasons"]
            if "value_overlap" in cand["evidence"] and not cand["data_valid"]:
                continue
            out.append(cand)
        return out

    def _value_overlap_candidates(self, src_cols, tgt_cols, es, et, add) -> None:
        refs = [
            c
            for c in src_cols
            if c.lower().endswith(_REF_SUFFIXES) and _is_string(src_cols[c])
        ]
        targets = [
            c for c in tgt_cols if c != et["id_column"] and _is_string(tgt_cols[c])
        ]
        probes = 0
        for column in refs:
            for target_column in targets:
                if probes >= _MAX_OVERLAP_PROBES:
                    return
                probes += 1
                profile = self._probe.join_profile(
                    es["table"], column, et["table"], target_column
                )
                if self._problems(profile, True) == []:
                    add(column, target_column, "value_overlap")

    def _problems(self, profile: Dict[str, Any], target_ids_unique: bool) -> List[str]:
        rows, matched = int(profile["source_rows"]), int(profile["matched"])
        problems = []
        if rows == 0:
            problems.append("no_edges")
        elif (rows - matched) / rows > self.MAX_DANGLING:
            problems.append("unmatched_rows_pct")
        if not profile["target_column_unique"]:
            problems.append("target_column_not_unique")
        if not target_ids_unique:
            problems.append("target_identity_not_unique")
        return problems

    @staticmethod
    def choose(
        candidates: List[Dict[str, Any]],
    ) -> Tuple[Optional[Dict[str, Any]], bool]:
        """``(chosen, ambiguous)`` under the evidence order described on the class."""
        declared = [c for c in candidates if "declared_fk" in c["evidence"]]
        if len(declared) == 1:
            return declared[0], False
        if len(declared) > 1:
            return None, True
        valid = [c for c in candidates if c["data_valid"]]
        if len(valid) == 1:
            return valid[0], False
        if len(valid) > 1:
            return None, True
        named = [c for c in candidates if "name_match" in c["evidence"]]
        return (named[0], False) if len(named) == 1 else (None, len(named) > 1)


def _is_string(data_type: str) -> bool:
    return str(data_type or "").upper().startswith(_STRING_TYPES)
