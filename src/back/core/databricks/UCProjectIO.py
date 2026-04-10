"""Unity Catalog I/O for OntoBricks project JSON files (stateless, credential-based)."""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from back.core.databricks import VolumeFileService
from back.core.errors import InfrastructureError, NotFoundError, ValidationError
from back.core.logging import get_logger

logger = get_logger(__name__)


class UCProjectIO:
    """Stateless project I/O through Unity Catalog Volumes."""

    @staticmethod
    def list_projects(
        catalog: str,
        schema: str,
        volume: str,
        host: Optional[str] = None,
        token: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """List available project files in a Unity Catalog volume.

        Raises:
            ValidationError: Missing credentials.
            InfrastructureError: UC communication failure.
        """
        if not host or not token:
            raise ValidationError("Databricks credentials required")

        try:
            uc_service = VolumeFileService(host=host, token=token)
            ok, files, msg = uc_service.list_files(catalog, schema, volume)
            if not ok:
                raise InfrastructureError(msg or "Failed to list files in UC volume")

            return [
                {
                    "name": f.get("name", "").replace(".json", ""),
                    "path": f.get("path", ""),
                    "size": f.get("size", 0),
                    "modified": f.get("modification_time", ""),
                }
                for f in files
                if f.get("name", "").endswith(".json")
            ]
        except (ValidationError, InfrastructureError):
            raise
        except Exception as e:
            logger.exception("Failed to list projects from UC: %s", e)
            raise InfrastructureError("Failed to list projects", detail=str(e))

    @staticmethod
    def load_project(
        project_path: str,
        host: Optional[str] = None,
        token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Load a project file from Unity Catalog.

        Returns:
            The parsed project data dict.

        Raises:
            ValidationError: Missing credentials or invalid JSON.
            NotFoundError: File could not be read.
            InfrastructureError: UC communication failure.
        """
        if not host or not token:
            raise ValidationError("Databricks credentials required")

        try:
            uc_service = VolumeFileService(host=host, token=token)
            success, content, read_msg = uc_service.read_file(project_path)
            if not success:
                raise NotFoundError(read_msg or f"Could not read file: {project_path}")

            return json.loads(content)

        except (ValidationError, NotFoundError):
            raise
        except json.JSONDecodeError as e:
            logger.exception("Invalid JSON loading project: %s", e)
            raise ValidationError("Invalid JSON in project file", detail=str(e))
        except Exception as e:
            logger.exception("Failed to load project from UC: %s", e)
            raise InfrastructureError("Failed to load project", detail=str(e))
