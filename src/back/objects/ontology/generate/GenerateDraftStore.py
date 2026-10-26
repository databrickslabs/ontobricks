"""Persistence of the durable Generate draft through a DomainSession."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Optional

from back.core.logging import get_logger
from back.objects.ontology.generate.DraftRevisionConflict import DraftRevisionConflict
from back.objects.ontology.generate.DraftValidationError import DraftValidationError
from back.objects.ontology.generate.GenerateDraft import GenerateDraft

if TYPE_CHECKING:
    from back.objects.session.DomainSession import DomainSession

logger = get_logger(__name__)


class GenerateDraftStore:
    """Persists/loads the durable Generate draft through a ``DomainSession``.

    Thin persistence boundary: converts between the typed
    :class:`GenerateDraft` and the raw dict ``DomainSession`` stores under
    ``data["generate_draft"]``, and enforces optimistic-concurrency revision
    checks on every write so a stale read-modify-write (e.g. from a reloaded
    tab) cannot silently clobber a newer draft. Callers still own calling
    the session's own ``save()`` (already done here for convenience, mirroring
    every other ``DomainSession`` mutation helper).
    """

    def __init__(self, session: "DomainSession") -> None:
        self._session = session

    def load(self) -> Optional[GenerateDraft]:
        """Return the persisted draft, or ``None`` if none exists.

        ``GenerateDraft.from_dict`` already self-heals unknown/missing
        per-field enum values (the ordinary schema-migration path), but a
        handful of invariants can only be checked once the whole draft is
        assembled (unique entity ids, unique normalized labels, a required
        ``id``/``canonical_label`` on every entity). A persisted draft that
        still fails one of *those* is corrupted beyond safe per-field
        repair — rather than raise and take down whatever screen is trying
        to resume, treat it exactly like "no draft": force re-detection.
        """
        raw = self._session.data.get("generate_draft")
        if not raw:
            return None
        try:
            return GenerateDraft.from_dict(raw)
        except DraftValidationError:
            logger.warning(
                "Discarding structurally invalid persisted Generate draft "
                "(forcing re-detection).",
                exc_info=True,
            )
            return None

    def save(self, draft: GenerateDraft) -> GenerateDraft:
        """Persist *draft*, enforcing optimistic concurrency.

        ``draft.draft_revision`` must equal the currently-persisted
        revision (``0`` when no draft is persisted yet — i.e. this is the
        first save). On success the persisted revision is bumped by 1 and
        the bumped copy is both stored and returned.
        """
        current = self.load()
        current_revision = current.draft_revision if current else 0
        if draft.draft_revision != current_revision:
            raise DraftRevisionConflict(
                f"Draft revision conflict: submitted revision "
                f"{draft.draft_revision!r} does not match current revision "
                f"{current_revision!r}. Reload the draft and retry."
            )
        saved = replace(draft, draft_revision=current_revision + 1)
        self._session.data["generate_draft"] = saved.to_dict()
        self._session.save()
        return saved

    def reset(self) -> None:
        """Discard the persisted draft (explicit discard, or re-detection)."""
        self._session.data["generate_draft"] = None
        self._session.save()
