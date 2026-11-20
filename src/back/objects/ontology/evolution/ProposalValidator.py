"""Validation ladder for an Evolve proposal against its candidate design."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.objects.ontology.autofix_ops import EVOLVE_OPS
from back.objects.ontology.evolution.OntologyIndex import OntologyIndex
from back.objects.ontology.evolution.SourceAccessDenied import SourceAccessDenied
from back.objects.ontology.evolution.SourceProbe import SourceProbe
from back.objects.ontology.evolution.SourceUnavailable import SourceUnavailable

MAX_OPS = 2
MAX_DANGLING = 0.05


class ProposalValidator:
    """Check a proposal step by step; any blocking failure stops the run.

    Steps: schema, ontology (the ops applied), mapping columns, identity and
    data on the current sources, row multiplication. Business meaning is
    never machine-approved: it is always reported as needing review.
    """

    def __init__(self, probe: SourceProbe):
        self._probe = probe

    def validate(
        self,
        operations: List[Dict[str, Any]],
        candidate: Optional[Dict[str, Any]],
        apply_error: str = "",
        join: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        checks: List[Dict[str, Any]] = []

        def check(name: str, ok: bool, reason: str = "") -> bool:
            checks.append(
                {
                    "name": name,
                    "status": "pass" if ok else "fail",
                    "reason": "" if ok else reason,
                }
            )
            return ok

        names = [op.get("op") for op in operations]
        if check(
            "schema",
            0 < len(operations) <= MAX_OPS and set(names) <= EVOLVE_OPS,
            "operations_outside_allowlist_or_budget",
        ):
            if check(
                "ontology",
                candidate is not None and not apply_error,
                apply_error or "not_applied",
            ):
                for op in operations:
                    if op.get("op") == "set_relationship_mapping":
                        self._mapping_checks(
                            op,
                            OntologyIndex(
                                candidate["ontology"], candidate["assignment"]
                            ),
                            check,
                        )
        if join is not None:
            check(
                "row_multiplication",
                bool((join.get("profile") or {}).get("target_column_unique")),
                "target_column_not_unique",
            )
        checks.append(
            {"name": "business_meaning", "status": "needs_review", "reason": ""}
        )
        failures = [c["reason"] for c in checks if c["status"] == "fail"]
        return {"passed": not failures, "checks": checks, "failures": failures}

    def _mapping_checks(self, op: Dict[str, Any], index: OntologyIndex, check) -> None:
        name = (op.get("target") or {}).get("name") or ""
        rel = index.relationships.get(name) or {}
        params = op.get("params") or {}
        src, tgt = index.entity(rel.get("domain", "")), index.entity(
            rel.get("range", "")
        )
        if not check(
            "mapping_endpoints",
            bool(src and tgt and src["table"] and tgt["table"]),
            "endpoint_class_not_mapped",
        ):
            return
        try:
            src_cols = self._probe.columns(src["table"])
            profile = self._probe.mapping_profile(
                params["sql_query"],
                params["source_id_column"],
                params["target_id_column"],
                src,
                tgt,
            )
            src_unique = self._probe.key_is_unique(src["table"], src["id_column"])
            tgt_unique = self._probe.key_is_unique(tgt["table"], tgt["id_column"])
        except SourceAccessDenied:
            check("source_access", False, "source_access_denied")
            return
        except SourceUnavailable:
            check("source_access", False, "source_unavailable")
            return
        check(
            "mapping_columns",
            params["source_id_column"] in src_cols,
            "source_id_column_missing",
        )
        rows = int(profile["rows"])
        check("data_edges", rows > 0, "no_edges")
        if rows:
            check(
                "dangling_source",
                profile["dangling_source"] / rows <= MAX_DANGLING,
                "dangling_source_pct",
            )
            check(
                "dangling_target",
                profile["dangling_target"] / rows <= MAX_DANGLING,
                "dangling_target_pct",
            )
        check("source_identity", src_unique, "source_identity_not_unique")
        check("target_identity", tgt_unique, "target_identity_not_unique")
