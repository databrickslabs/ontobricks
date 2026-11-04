"""Map a sandbox or Explorer union name to the overlay-key companion.

Explorer overlays live triples per ``(subject, predicate)``. Remaining sandbox
rows cannot represent an emptied predicate (last-value ``remove``), so the
companion ``__smp_<id>`` stores every pair the sandbox has owned. Append-only:
seed, add, set, and inferences insert keys; remove does not delete them.
"""

from __future__ import annotations

from typing import Any, Iterable, Set, Tuple

_SIM = "__sim_"
_SMU = "__smu_"
_SMP = "__smp_"

OverlayPred = Tuple[str, str]


def overlay_relation(relation: str) -> str:
    """Physical ``__smp_`` name for a ``__sim_`` sandbox or ``__smu_`` view."""
    name = relation or ""
    leaf = name.split(".")[-1]
    if _SMP in leaf:
        return name
    if _SMU in name:
        return name.replace(_SMU, _SMP, 1)
    if _SIM in name:
        return name.replace(_SIM, _SMP, 1)
    return ""


def overlay_preds_from_rows(rows: Iterable[Any]) -> Set[OverlayPred]:
    """``(subject, predicate)`` pairs from overlay-key or sandbox triples."""
    preds: Set[OverlayPred] = set()
    for row in rows:
        subject = (row or {}).get("subject") or ""
        predicate = (row or {}).get("predicate") or ""
        if subject and predicate:
            preds.add((subject, predicate))
    return preds


def load_overlay_preds(store: Any, graph_name: str, sandbox_rows: Iterable[Any]) -> Set[OverlayPred]:
    """Union remaining sandbox pairs with persisted overlay keys when present."""
    preds = overlay_preds_from_rows(sandbox_rows)
    relation = overlay_relation(graph_name)
    if not relation:
        return preds
    query = getattr(store, "query_triples", None)
    if not callable(query):
        return preds
    try:
        extra = query(relation) or []
    except Exception:  # noqa: BLE001
        return preds
    preds.update(overlay_preds_from_rows(extra))
    return preds
