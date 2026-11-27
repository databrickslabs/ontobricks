"""Backend-neutral contract of the ``_overrides`` layer.

One row of an overrides relation is a triple plus the operation applied to the
mapped graph:

* ``op = 'add'``  - the triple is added to the graph;
* ``op = 'del'``  - the triple is hidden from the graph. ``origin='user'`` for a
  manual hide; ``origin='reasoning'`` for a removal rule (recomputed on Build).

``origin`` records who wrote the row. Rows written by reasoning, cohorts and
communities are *derived*: Build and purge remove them. Rows written by a user
survive both.

The effective graph is ``(mapped UNION add rows) MINUS del rows`` on the key
``(subject, predicate, object)``. A triple never has both an ``add`` and a
``del`` row: the stores resolve that at write time, so readers need neither
ordering nor timestamps.
"""

from __future__ import annotations

import uuid
from typing import Dict, Iterable, List

from back.core.errors import ValidationError

OP_ADD = "add"
OP_DEL = "del"
OPS = (OP_ADD, OP_DEL)

ORIGIN_USER = "user"
ORIGIN_REASONING = "reasoning"
ORIGIN_COHORT = "cohort"
ORIGIN_COMMUNITY = "community"
ORIGINS = (ORIGIN_USER, ORIGIN_REASONING, ORIGIN_COHORT, ORIGIN_COMMUNITY)
DERIVED_ORIGINS = (ORIGIN_REASONING, ORIGIN_COHORT, ORIGIN_COMMUNITY)

Triple = Dict[str, str]


def validate_op(op: str) -> str:
    """Return *op* when it is a known operation, else raise ``ValidationError``."""
    if op not in OPS:
        raise ValidationError(f"Invalid override op {op!r}; expected one of {OPS}")
    return op


def validate_origin(origin: str) -> str:
    """Return *origin* when it is a known origin, else raise ``ValidationError``."""
    if origin not in ORIGINS:
        raise ValidationError(
            f"Invalid override origin {origin!r}; expected one of {ORIGINS}"
        )
    return origin


def validate_derived_origin(origin: str) -> str:
    """Return *origin* when a derived writer may use it (never ``user``)."""
    validate_origin(origin)
    if origin not in DERIVED_ORIGINS:
        raise ValidationError(
            f"Origin {origin!r} is not a derived origin; use add_triples() for user rows"
        )
    return origin


HIDE_ORIGINS = (ORIGIN_USER, ORIGIN_REASONING)


def validate_hide_origin(origin: str) -> str:
    """Return *origin* when a ``del`` row may carry it, else raise ``ValidationError``.

    A manual hide is ``user``; a removal rule is ``reasoning`` (recomputed on Build).
    """
    if origin not in HIDE_ORIGINS:
        raise ValidationError(
            f"Invalid hide origin {origin!r}; expected one of {HIDE_ORIGINS}"
        )
    return origin


def new_change_id() -> str:
    """Identifier linking the ``del`` and ``add`` rows of one modification."""
    return uuid.uuid4().hex


def triple_key(triple: Triple) -> tuple[str, str, str]:
    """Normalised ``(subject, predicate, object)`` key of *triple*."""
    return (
        str(triple.get("subject", "") or ""),
        str(triple.get("predicate", "") or ""),
        str(triple.get("object", "") or ""),
    )


def unique_triples(triples: Iterable[Triple]) -> List[Triple]:
    """Drop duplicate triples, keeping first-seen order."""
    seen: set[tuple[str, str, str]] = set()
    out: List[Triple] = []
    for triple in triples:
        key = triple_key(triple)
        if key in seen:
            continue
        seen.add(key)
        out.append({"subject": key[0], "predicate": key[1], "object": key[2]})
    return out
