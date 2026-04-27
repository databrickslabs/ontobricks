"""One-shot migration helper: copy Volume registry data into Lakebase.

The migration walks every JSON-shaped artefact persisted by
:class:`VolumeRegistryStore` and re-writes it through
:class:`LakebaseRegistryStore`. Binary artefacts (``documents/`` and
``*.lbug.tar.gz``) are *not* moved — they always live on the Unity
Catalog Volume.

The operation is idempotent at the Lakebase side because:

- ``write_version`` upserts on ``(registry_id, domain_name, version)``.
- ``save_domain_permissions`` replaces the row for the domain.
- ``save_schedules`` deletes-then-inserts the schedule set.
- ``save_global_config`` last-write-wins-merges the JSONB blob.
- ``append_schedule_history`` keeps the most recent ``max_entries``.

Intended call site: ``POST /settings/registry/migrate-to-lakebase``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from back.core.logging import get_logger

from .base import RegistryStore

logger = get_logger(__name__)


@dataclass
class MigrationReport:
    """Aggregated result of a Volume → Lakebase copy run."""

    domains: int = 0
    versions: int = 0
    permission_sets: int = 0
    schedules: int = 0
    history_entries: int = 0
    global_config: bool = False
    errors: List[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.errors is None:
            self.errors = []

    def as_dict(self) -> Dict[str, Any]:
        return {
            "domains": self.domains,
            "versions": self.versions,
            "permission_sets": self.permission_sets,
            "schedules": self.schedules,
            "history_entries": self.history_entries,
            "global_config": self.global_config,
            "errors": list(self.errors),
        }

    @property
    def ok(self) -> bool:
        return not self.errors


def migrate_volume_to_lakebase(
    src: RegistryStore,
    dst: RegistryStore,
    *,
    initialize_dst: bool = True,
) -> MigrationReport:
    """Copy every JSON-shaped artefact from *src* to *dst*.

    Parameters
    ----------
    src:
        Source store (typically :class:`VolumeRegistryStore`).
    dst:
        Destination store (typically :class:`LakebaseRegistryStore`).
    initialize_dst:
        When ``True`` (default), call ``dst.initialize`` before
        copying so the schema/tables exist.
    """
    report = MigrationReport()

    if initialize_dst:
        ok, msg = dst.initialize()
        if not ok:
            report.errors.append(f"initialize destination: {msg}")
            return report

    # Global config first — schedules embedded in it are handled below
    # via the dedicated schedules path.
    try:
        cfg = src.load_global_config()
        if cfg:
            ok, msg = dst.save_global_config(cfg)
            if ok:
                report.global_config = True
            else:
                report.errors.append(f"save_global_config: {msg}")
    except Exception as exc:  # noqa: BLE001
        report.errors.append(f"load_global_config: {exc}")

    # Schedules
    try:
        scheds = src.load_schedules()
        if scheds:
            ok, msg = dst.save_schedules(scheds)
            if ok:
                report.schedules = len(scheds)
            else:
                report.errors.append(f"save_schedules: {msg}")
    except Exception as exc:  # noqa: BLE001
        report.errors.append(f"load_schedules: {exc}")

    # Domains + versions + per-domain permissions + history
    try:
        ok, folders, msg = src.list_domain_folders()
        if not ok:
            report.errors.append(f"list_domain_folders: {msg}")
            return report
    except Exception as exc:  # noqa: BLE001
        report.errors.append(f"list_domain_folders: {exc}")
        return report

    for folder in folders:
        report.domains += 1
        _copy_domain(src, dst, folder, report)

    return report


def _copy_domain(
    src: RegistryStore,
    dst: RegistryStore,
    folder: str,
    report: MigrationReport,
) -> None:
    # Versions
    try:
        ok, versions, msg = src.list_versions(folder)
        if not ok:
            report.errors.append(f"list_versions[{folder}]: {msg}")
            versions = []
    except Exception as exc:  # noqa: BLE001
        report.errors.append(f"list_versions[{folder}]: {exc}")
        versions = []

    for version in versions:
        try:
            ok, data, msg = src.read_version(folder, version)
            if not ok:
                report.errors.append(
                    f"read_version[{folder}/{version}]: {msg}"
                )
                continue
            ok, msg = dst.write_version(folder, version, data)
            if ok:
                report.versions += 1
            else:
                report.errors.append(
                    f"write_version[{folder}/{version}]: {msg}"
                )
        except Exception as exc:  # noqa: BLE001
            report.errors.append(
                f"copy version[{folder}/{version}]: {exc}"
            )

    # Domain permissions
    try:
        perms = src.load_domain_permissions(folder)
        if perms and perms.get("permissions"):
            ok, msg = dst.save_domain_permissions(folder, perms)
            if ok:
                report.permission_sets += 1
            else:
                report.errors.append(
                    f"save_domain_permissions[{folder}]: {msg}"
                )
    except Exception as exc:  # noqa: BLE001
        report.errors.append(f"load_domain_permissions[{folder}]: {exc}")

    # Schedule history
    try:
        history = src.load_schedule_history(folder)
        for entry in history:
            try:
                dst.append_schedule_history(folder, entry)
                report.history_entries += 1
            except Exception as exc:  # noqa: BLE001
                report.errors.append(
                    f"append_schedule_history[{folder}]: {exc}"
                )
                break
    except Exception as exc:  # noqa: BLE001
        report.errors.append(f"load_schedule_history[{folder}]: {exc}")


def summarize(report: MigrationReport) -> Tuple[bool, str]:
    """Build a single-line summary suitable for the API response.

    On failure the first error is appended so the admin UI can show
    what went wrong without forcing the user to dig into the
    structured ``report`` payload (the most common case is the
    Lakebase ``initialize`` step failing — connection refused, missing
    database, missing extension permission, etc.).
    """
    summary = (
        f"domains={report.domains}, versions={report.versions}, "
        f"permission_sets={report.permission_sets}, "
        f"schedules={report.schedules}, history={report.history_entries}, "
        f"global_config={'yes' if report.global_config else 'no'}"
    )
    if not report.ok:
        first = report.errors[0] if report.errors else "unknown error"
        more = (
            f" (+{len(report.errors) - 1} more)"
            if len(report.errors) > 1
            else ""
        )
        return False, f"{summary}; errors={len(report.errors)} — {first}{more}"
    return True, summary
