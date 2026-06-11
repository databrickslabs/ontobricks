"""Tier-3 self-baseline storage (§3.4).

Each scored run is persisted under ``logs/goals/``.  The baseline for the
next run is the pipeline's own *most recent accepted* (GREEN) scorecard —
never a domain answer key.  This is how "did it get worse" is detected
without gold labels.
"""

from __future__ import annotations

import glob
import json
import os
from typing import Any, Dict, List, Optional

DEFAULT_BASELINE_DIR = "logs/goals"
_PREFIX = "scorecard_"


def _sort_key(card: Dict[str, Any]) -> Any:
    return (card.get("timestamp") or "", card.get("run_id") or "")


def save_scorecard(
    scorecard: Dict[str, Any], baseline_dir: str = DEFAULT_BASELINE_DIR
) -> str:
    """Persist *scorecard* and return the path written."""
    os.makedirs(baseline_dir, exist_ok=True)
    run_id = scorecard.get("run_id") or "run"
    safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in str(run_id))
    path = os.path.join(baseline_dir, f"{_PREFIX}{safe}.json")
    with open(path, "w") as f:
        json.dump(scorecard, f, indent=2, default=str)
    return path


def _load_all(baseline_dir: str) -> List[Dict[str, Any]]:
    cards: List[Dict[str, Any]] = []
    for p in glob.glob(os.path.join(baseline_dir, f"{_PREFIX}*.json")):
        try:
            with open(p) as f:
                cards.append(json.load(f))
        except (OSError, json.JSONDecodeError):
            continue
    return cards


def load_baseline(
    baseline_dir: str = DEFAULT_BASELINE_DIR,
    *,
    exclude_run_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Return the most recent accepted (GREEN) scorecard, or ``None``.

    A RED run never becomes a baseline — otherwise a regression would
    silently reset the bar.  ``exclude_run_id`` drops the current run so a
    scorecard never baselines against itself.
    """
    cards = [
        c
        for c in _load_all(baseline_dir)
        if c.get("verdict") == "GREEN"
        and c.get("run_id") != exclude_run_id
    ]
    if not cards:
        return None
    cards.sort(key=_sort_key)
    return cards[-1]
