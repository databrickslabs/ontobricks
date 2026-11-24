"""Lakebase Autoscaling project and endpoint discovery."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Dict, List

from databricks.sdk.errors import NotFound, PermissionDenied

from back.core.logging import get_logger

logger = get_logger(__name__)

#: Errors that mean "this principal can't see that project/branch": skip it.
#: Anything else (throttling, 5xx) propagates so the caller sees the real cause.
_UNREADABLE = (NotFound, PermissionDenied)


class LakebaseProjectService:
    """Discover Lakebase projects and endpoints through the Postgres API."""

    _PROJECTS_PATH = "/api/2.0/postgres/projects"
    _PAGE_SIZE = 100

    @staticmethod
    def list_projects(api: Any) -> List[Dict[str, Any]]:
        """Return every Lakebase project visible to the current principal."""
        projects: List[Dict[str, Any]] = []
        page_token = ""

        while True:
            query: Dict[str, Any] = {"page_size": LakebaseProjectService._PAGE_SIZE}
            if page_token:
                query["page_token"] = page_token

            response = (
                api.do(
                    "GET",
                    LakebaseProjectService._PROJECTS_PATH,
                    query=query,
                )
                or {}
            )
            projects.extend(response.get("projects") or [])
            page_token = response.get("next_page_token") or ""
            if not page_token:
                return projects

    @staticmethod
    def find_endpoint(
        api: Any, host: str, known_branches: Iterable[str]
    ) -> Dict[str, Any] | None:
        """Return the endpoint serving *host*, or ``None``.

        Tries *known_branches* (``projects/<p>/branches/<b>``, consumed
        lazily) first. Only then walks every project, which costs one call
        per project and branch on a large workspace.
        """
        for branch_path in known_branches:
            endpoint = LakebaseProjectService.branch_endpoint(api, branch_path, host)
            if endpoint:
                return endpoint
        for project in LakebaseProjectService.list_projects(api):
            for branch_path in LakebaseProjectService._branch_paths(api, project.get("name") or ""):
                endpoint = LakebaseProjectService.branch_endpoint(api, branch_path, host)
                if endpoint:
                    return endpoint
        return None

    @staticmethod
    def branch_endpoint(api: Any, branch_path: str, host: str) -> Dict[str, Any] | None:
        """Return the endpoint of *branch_path* serving *host*, or ``None``.

        Matches ``status.hosts.host`` or ``read_only_host``, ignoring case.
        An empty *host* takes the first endpoint that has a primary host.
        An unreadable branch is no match.
        """
        try:
            endpoints = (
                api.do("GET", f"/api/2.0/postgres/{branch_path}/endpoints") or {}
            ).get("endpoints") or []
        except _UNREADABLE as exc:
            logger.info("Skipping unreadable Lakebase branch %s: %s", branch_path, exc)
            return None
        host = host.strip().lower()
        for endpoint in endpoints:
            hosts = (endpoint.get("status") or {}).get("hosts") or {}
            primary = (hosts.get("host") or "").strip().lower()
            read_only = (hosts.get("read_only_host") or "").strip().lower()
            if (not host and primary) or (host and host in (primary, read_only)):
                return endpoint
        return None

    @staticmethod
    def _branch_paths(api: Any, project_path: str) -> List[str]:
        """Return the branch paths of *project_path*; none if it is unreadable."""
        if not project_path:
            return []
        try:
            branches = (
                api.do("GET", f"/api/2.0/postgres/{project_path}/branches") or {}
            ).get("branches") or []
        except _UNREADABLE as exc:
            logger.info("Skipping unreadable Lakebase project %s: %s", project_path, exc)
            return []
        return [b["name"] for b in branches if b.get("name")]
