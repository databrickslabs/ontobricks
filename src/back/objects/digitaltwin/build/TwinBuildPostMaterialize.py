"""Build-pipeline steps that sit around the ``…_data`` relation.

Extracted from :class:`~back.objects.digitaltwin.build._build_pipeline._BuildPipeline`
so the pipeline class stays inside its size budget. Every method takes the
running pipeline (*pipe*) and reads its state by attribute; the pipeline keeps
one-line hooks that delegate here.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from back.core.logging import get_logger

logger = get_logger(__name__)


class TwinBuildPostMaterialize:
    """Registry stamping of a finished build (more steps follow in later commits)."""

    @staticmethod
    def persist_last_build_to_registry(pipe: Any) -> None:
        """Stamp ``last_build`` on the registry row without rewriting JSONB.

        The session/UI build path stamps domain.last_build before the build
        thread starts; the API path does not. The timestamp must reach the
        ``domain_versions.last_build`` column so ReviewService.submit() can
        unblock the Submit-for-Review gate.

        A full ``write_version`` here is unsafe: the worker's live session
        can have empty ``ontology.classes`` (request-scoped DomainSession
        after Generate/complete, or a snapshot that only carried mappings).
        Rewriting the document would wipe a previously saved ontology while
        keeping mappings — SHACL suggest then returns nothing.

        Best-effort: a failure is logged but never propagates.
        """
        try:
            from back.objects.registry.RegistryService import RegistryService
            from back.objects.session import sanitize_domain_folder

            folder = getattr(pipe.domain, "uc_domain_folder", "") or (
                sanitize_domain_folder(pipe.domain_name)
            )
            version = (
                getattr(pipe.domain_snap, "current_version", None)
                or getattr(pipe.domain, "current_version", None)
                or ""
            )
            if not folder or not version:
                logger.warning(
                    "[DT-BUILD %s] _persist_last_build_to_registry: "
                    "cannot resolve folder=%r version=%r — skipping",
                    pipe.task_id,
                    folder,
                    version,
                )
                return

            # API build path never stamps last_build before starting; do it now.
            if not getattr(pipe.domain, "last_build", None):
                pipe.domain.last_build = datetime.now(timezone.utc).isoformat()

            svc = RegistryService.from_context(pipe.domain, pipe.settings)
            ok, msg = svc._store.stamp_last_build(
                folder, str(version), pipe.domain.last_build
            )
            if ok:
                logger.info(
                    "[DT-BUILD %s] stamped last_build=%s in registry "
                    "(folder=%s version=%s)",
                    pipe.task_id,
                    pipe.domain.last_build,
                    folder,
                    version,
                )
            else:
                logger.error(
                    "[DT-BUILD %s] stamp_last_build failed when persisting "
                    "last_build: %s",
                    pipe.task_id,
                    msg,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[DT-BUILD %s] could not persist last_build to registry "
                "(non-fatal — Submit for Review gate may remain blocked): %s",
                pipe.task_id,
                exc,
            )
