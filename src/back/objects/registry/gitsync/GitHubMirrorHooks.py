"""Store mixin that queues a GitHub mirror update after each registry write."""

from __future__ import annotations

from typing import Any, List, Tuple

from back.objects.registry.gitsync.GitHubMirror import github_mirror


class GitHubMirrorHooks:
    """Mix in *before* the concrete store bases so these overrides run first.

    Every write still goes to the next class in the MRO; the mirror is only
    notified on success and never raises, so a GitHub outage cannot fail a
    registry write. Notifications are no-ops unless Settings → GitHub is on.
    """

    def write_version(self, folder: str, version: str, data: Any) -> Tuple[bool, str]:
        ok, msg = super().write_version(folder, version, data)  # type: ignore[misc]
        if ok:
            github_mirror.notify_write(self, folder, version)
        return ok, msg

    def update_version_status(
        self, folder: str, version: str, status: str
    ) -> Tuple[bool, str]:
        ok, msg = super().update_version_status(folder, version, status)  # type: ignore[misc]
        if ok:
            github_mirror.notify_write(self, folder, version)
        return ok, msg

    def delete_version(self, folder: str, version: str) -> Tuple[bool, str]:
        ok, msg = super().delete_version(folder, version)  # type: ignore[misc]
        if ok:
            github_mirror.notify_delete_version(self, folder, version)
        return ok, msg

    def delete_domain(self, folder: str) -> List[str]:
        # GitHub domain delete is opt-in from the registry UI confirm dialog.
        return super().delete_domain(folder)  # type: ignore[misc]
