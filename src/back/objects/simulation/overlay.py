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
_SMB = "__smb_"
_SMO = "__smo_"
_FAMILY = (_SIM, _SMU, _SMP, _SMB, _SMO)

OverlayPred = Tuple[str, str]


def _swap_marker(relation: str, marker: str) -> str:
    name = relation or ""
    leaf = name.split(".")[-1]
    if marker in leaf:
        return name
    for current in _FAMILY:
        if current in name:
            return name.replace(current, marker, 1)
    return ""


def overlay_relation(relation: str) -> str:
    """Physical ``__smp_`` name for a ``__sim_`` sandbox or ``__smu_`` view."""
    return _swap_marker(relation, _SMP)


def before_relation(relation: str) -> str:
    """Physical ``__smb_`` before-picture companion of a sandbox."""
    return _swap_marker(relation, _SMB)


def origin_relation(relation: str) -> str:
    """Physical ``__smo_`` origin companion of a sandbox."""
    return _swap_marker(relation, _SMO)


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
