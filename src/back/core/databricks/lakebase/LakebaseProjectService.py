"""Lakebase Autoscaling project discovery."""

from __future__ import annotations

from typing import Any, Dict, List


class LakebaseProjectService:
    """List Lakebase projects through the paginated Postgres API."""

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
