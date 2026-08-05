"""Lock management — the single-editor DRAFT edit-lock orchestrator.

Groups the lock-specific application logic (the :class:`EditLockService`
renew-only lease orchestrator) into one subpackage. The lock's *persistence*
lives with the Lakebase store (``registry/store/lakebase/store.py``, keyed by
``domain_edit_locks``) and its HTTP surface with the routers — only the
session-aware orchestration is owned here.
"""

from back.objects.registry.lockmgt.EditLockService import (
    EditLockService,
    MODE_EDIT,
    MODE_VIEW,
    MODE_NONE,
)

__all__ = [
    "EditLockService",
    "MODE_EDIT",
    "MODE_VIEW",
    "MODE_NONE",
]
