"""
Registry Service for OntoBricks.

Centralises all project-registry management (config resolution, path
construction, project CRUD, version management) behind a single
``RegistryService`` class and a lightweight ``RegistryCfg`` dataclass.

Usage in a route handler::

    from back.objects.registry import RegistryCfg, RegistryService

    cfg  = RegistryCfg.from_project(project, settings)
    svc  = RegistryService.from_context(project, settings)
    ok, names, msg = svc.list_projects()
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from back.core.logging import get_logger
from back.core.databricks import VolumeFileService

logger = get_logger(__name__)

_DEFAULT_VOLUME = "OntoBricksRegistry"
_REGISTRY_MARKER = ".registry"


# ------------------------------------------------------------------
# RegistryCfg — lightweight value object
# ------------------------------------------------------------------

@dataclass(frozen=True)
class RegistryCfg:
    """Immutable registry location triplet (catalog, schema, volume)."""

    catalog: str
    schema: str
    volume: str

    # -- constructors ------------------------------------------------

    @classmethod
    def from_volume_path(cls, path: str) -> RegistryCfg:
        """Parse ``/Volumes/<catalog>/<schema>/<volume>`` into a RegistryCfg."""
        parts = path.strip("/").split("/")
        if len(parts) >= 4 and parts[0].lower() == "volumes":
            return cls(catalog=parts[1], schema=parts[2], volume=parts[3])
        logger.warning("Cannot parse volume path '%s'; expected /Volumes/<c>/<s>/<v>", path)
        return cls(catalog="", schema="", volume="")

    @classmethod
    def from_project(cls, project, settings) -> RegistryCfg:
        """Build from a *ProjectSession* and *Settings* with env-var fallbacks.

        When the app is deployed with a Volume resource the injected path
        (``settings.registry_volume_path``) takes highest priority so that
        admin-level Databricks App resource configuration always wins.
        """
        vol_path = getattr(settings, "registry_volume_path", "")
        if vol_path:
            return cls.from_volume_path(vol_path)

        reg = project.settings.get("registry", {})
        return cls(
            catalog=reg.get("catalog") or settings.registry_catalog,
            schema=reg.get("schema") or settings.registry_schema,
            volume=reg.get("volume") or settings.registry_volume or _DEFAULT_VOLUME,
        )

    @classmethod
    def from_session(cls, session_mgr, settings) -> RegistryCfg:
        """Build from *SessionManager* and *Settings*."""
        from back.objects.session.project_session import get_project
        return cls.from_project(get_project(session_mgr), settings)

    @classmethod
    def from_dict(cls, d: Dict[str, str]) -> RegistryCfg:
        """Build from a plain dict (e.g. an existing ``registry_cfg``)."""
        return cls(
            catalog=d.get("catalog", ""),
            schema=d.get("schema", ""),
            volume=d.get("volume", "") or _DEFAULT_VOLUME,
        )

    # -- helpers -----------------------------------------------------

    @property
    def is_configured(self) -> bool:
        return bool(self.catalog and self.schema and self.volume)

    def as_dict(self) -> Dict[str, str]:
        """Dict representation for backward compatibility with legacy callers."""
        return {"catalog": self.catalog, "schema": self.schema, "volume": self.volume}


# ------------------------------------------------------------------
# RegistryService — all I/O operations
# ------------------------------------------------------------------

class RegistryService:
    """Encapsulates every UC-Volume registry operation."""

    def __init__(self, cfg: RegistryCfg, uc: VolumeFileService):
        self._cfg = cfg
        self._uc = uc

    # -- factory -----------------------------------------------------

    @classmethod
    def from_context(cls, project, settings) -> RegistryService:
        """One-call factory: resolve config + build VolumeFileService."""
        from back.core.helpers import get_databricks_host_and_token

        cfg = RegistryCfg.from_project(project, settings)
        host, token = get_databricks_host_and_token(project, settings)
        uc = VolumeFileService(host=host, token=token)
        return cls(cfg, uc)

    # -- properties --------------------------------------------------

    @property
    def cfg(self) -> RegistryCfg:
        return self._cfg

    @property
    def uc(self) -> VolumeFileService:
        """Expose the underlying VolumeFileService for callers that need it."""
        return self._uc

    # -- path builders -----------------------------------------------

    def volume_root(self) -> str:
        c = self._cfg
        return f"/Volumes/{c.catalog}/{c.schema}/{c.volume}"

    def projects_path(self) -> str:
        return f"{self.volume_root()}/projects"

    def project_path(self, folder: str) -> str:
        return f"{self.projects_path()}/{folder}"

    def version_file_path(self, folder: str, version: str) -> str:
        return f"{self.project_path(folder)}/v{version}.json"

    def marker_path(self) -> str:
        return f"{self.volume_root()}/{_REGISTRY_MARKER}"

    def config_file_path(self) -> str:
        return f"{self.volume_root()}/.global_config.json"

    def permissions_file_path(self) -> str:
        return f"{self.volume_root()}/.permissions.json"

    def history_file_path(self, folder: str) -> str:
        return f"{self.project_path(folder)}/.schedule_history.json"

    # -- registry lifecycle ------------------------------------------

    def is_initialized(self) -> bool:
        """Check whether the ``.registry`` marker exists."""
        ok, _, _ = self._uc.read_file(self.marker_path())
        return ok

    def initialize(self, client) -> Tuple[bool, str]:
        """Create the registry volume (if missing) and write the marker.

        *client* must be a ``DatabricksClient`` that supports
        ``list_volumes`` / ``create_volume``.
        """
        c = self._cfg
        volumes = client.list_volumes(c.catalog, c.schema)
        if c.volume not in volumes:
            ok = client.create_volume(c.catalog, c.schema, c.volume)
            if not ok:
                return False, f"Failed to create volume {c.volume}"

        self._uc.write_file(
            self.marker_path(), "OntoBricks Project Registry", overwrite=True,
        )
        logger.info("Registry initialized at %s.%s.%s", c.catalog, c.schema, c.volume)
        return True, f"Registry initialized: {c.catalog}.{c.schema}.{c.volume}"

    # -- project CRUD ------------------------------------------------

    def list_projects(self) -> Tuple[bool, List[str], str]:
        """Return sorted project folder names (hidden dirs excluded)."""
        ok, items, msg = self._uc.list_directory(self.projects_path(), dirs_only=True)
        if not ok:
            return False, [], msg
        names = sorted(i["name"] for i in items if not i["name"].startswith("."))
        return True, names, ""

    def list_project_details(self) -> Tuple[bool, List[Dict[str, Any]], str]:
        """List projects with description and version list.

        For each project folder the latest version file is opened to
        extract the description from ``info.description``.
        """
        ok, items, msg = self._uc.list_directory(self.projects_path(), dirs_only=True)
        if not ok:
            return False, [], msg

        result: List[Dict[str, Any]] = []
        for item in sorted(items, key=lambda i: i["name"]):
            name = item["name"]
            if name.startswith("."):
                continue

            description = ""
            version_list: List[str] = []
            try:
                ver_ok, ver_items, _ = self._uc.list_directory(
                    f"{self.projects_path()}/{name}", extensions=[".json"],
                )
                if ver_ok and ver_items:
                    version_list = sorted(
                        [
                            f["name"][1:-5]
                            for f in ver_items
                            if f["name"].startswith("v") and f["name"].endswith(".json")
                        ],
                        key=lambda v: [int(x) for x in v.split(".")],
                        reverse=True,
                    )
                    if version_list:
                        latest_file = f"v{version_list[0]}.json"
                        f_ok, content, _ = self._uc.read_file(
                            f"{self.projects_path()}/{name}/{latest_file}",
                        )
                        if f_ok and content:
                            description = json.loads(content).get("info", {}).get("description", "")
            except Exception:
                logger.debug("Could not read description for project %s", name)

            result.append({
                "name": name,
                "description": description,
                "versions": version_list,
            })

        return True, result, ""

    def list_mcp_projects(self, require_ontology: bool = False) -> Tuple[bool, List[Dict[str, str]], str]:
        """List projects that have an MCP-enabled version.

        Returns ``(ok, projects, message)`` where each project is
        ``{"name": ..., "description": ...}``.  When *require_ontology* is
        ``True`` only projects whose MCP version has a non-empty ``classes``
        list are included.
        """
        ok, items, msg = self._uc.list_directory(self.projects_path(), dirs_only=True)
        if not ok:
            return False, [], msg

        result: List[Dict[str, str]] = []
        for item in sorted(items, key=lambda i: i["name"]):
            name = item["name"]
            if name.startswith("."):
                continue
            try:
                mcp_ver, mcp_data = self.find_mcp_version(name)
                if not mcp_ver:
                    continue
                info = mcp_data.get("info", {})
                if require_ontology:
                    ver_data = mcp_data.get("versions", {}).get(mcp_ver, {})
                    ont = ver_data.get("ontology", mcp_data.get("ontology", {}))
                    if not ont.get("classes"):
                        continue
                result.append({"name": name, "description": info.get("description", "")})
            except Exception:
                logger.debug("Could not inspect project %s", name)
        return True, result, ""

    def delete_project(self, folder: str) -> List[str]:
        """Delete a project directory and all its contents."""
        return self.recursive_delete(self.project_path(folder))

    def recursive_delete(self, dir_path: str) -> List[str]:
        """Recursively delete all files and then empty directories."""
        dir_path = dir_path.rstrip("/")
        errors: List[str] = []

        logger.info("recursive_delete: listing %s", dir_path)
        ok, items, msg = self._uc.list_directory(dir_path)
        if not ok:
            logger.warning("recursive_delete: cannot list %s: %s", dir_path, msg)
            errors.append(f"Cannot list {dir_path}: {msg}")
            return errors

        logger.info("recursive_delete: found %d items in %s", len(items), dir_path)
        for item in items:
            item_path = item["path"].rstrip("/")
            if item.get("is_directory", False):
                logger.info("recursive_delete: descending into %s", item_path)
                errors.extend(self.recursive_delete(item_path))
            else:
                logger.info("recursive_delete: deleting file %s", item_path)
                d_ok, d_msg = self._uc.delete_file(item_path)
                if d_ok:
                    logger.info("recursive_delete: deleted %s", item_path)
                else:
                    errors.append(d_msg)
                    logger.warning("recursive_delete: FAILED %s: %s", item_path, d_msg)

        d_ok, d_msg = self._uc.delete_directory(dir_path)
        if d_ok:
            logger.info("recursive_delete: removed directory %s", dir_path)
        else:
            errors.append(d_msg)
            logger.warning("recursive_delete: could not remove directory %s: %s",
                           dir_path, d_msg)

        return errors

    # -- version management ------------------------------------------

    def list_versions(self, folder: str) -> Tuple[bool, List[str], str]:
        """Return version strings (e.g. ``['2', '1']``) for a project folder."""
        ok, items, msg = self._uc.list_directory(
            self.project_path(folder), extensions=[".json"],
        )
        if not ok:
            return False, [], msg
        versions = [
            f["name"][1:-5]
            for f in items
            if f["name"].startswith("v") and f["name"].endswith(".json")
        ]
        return True, versions, ""

    def list_versions_sorted(self, folder: str, *, reverse: bool = True) -> List[str]:
        """Convenience: sorted version list (empty on failure)."""
        ok, versions, _ = self.list_versions(folder)
        if not ok:
            return []
        versions.sort(key=lambda v: [int(x) for x in v.split(".")], reverse=reverse)
        return versions

    def get_latest_version(self, folder: str) -> Optional[str]:
        """Return the highest version string, or ``None``."""
        vs = self.list_versions_sorted(folder)
        return vs[0] if vs else None

    def read_version(self, folder: str, version: str) -> Tuple[bool, dict, str]:
        """Read and JSON-parse a version file."""
        path = self.version_file_path(folder, version)
        ok, content, msg = self._uc.read_file(path)
        if not ok:
            return False, {}, msg
        try:
            return True, json.loads(content), ""
        except json.JSONDecodeError as exc:
            return False, {}, f"Invalid JSON: {exc}"

    def write_version(self, folder: str, version: str, data: str) -> Tuple[bool, str]:
        """Write a version file (``data`` should be a JSON string)."""
        path = self.version_file_path(folder, version)
        return self._uc.write_file(path, data)

    def delete_version(self, folder: str, version: str) -> Tuple[bool, str]:
        """Delete a single version file."""
        path = self.version_file_path(folder, version)
        return self._uc.delete_file(path)

    # -- load project from registry (stateless) ----------------------

    def load_latest_project_data(self, folder: str) -> Tuple[bool, dict, str, str]:
        """Load the latest version for *folder*.

        Returns ``(ok, data_dict, version_str, error_msg)``.
        """
        latest = self.get_latest_version(folder)
        if not latest:
            return False, {}, "", f'No versions found for project "{folder}"'
        ok, data, msg = self.read_version(folder, latest)
        if not ok:
            return False, {}, latest, msg
        return True, data, latest, ""

    def find_mcp_version(self, folder: str) -> Tuple[Optional[str], dict]:
        """Find the version with ``mcp_enabled=True`` for *folder*.

        Returns ``(version_str, data_dict)`` or ``(None, {})`` when no
        version has the flag set.
        """
        for ver in self.list_versions_sorted(folder):
            ok, data, _ = self.read_version(folder, ver)
            if not ok:
                continue
            if data.get('info', {}).get('mcp_enabled'):
                return ver, data
        return None, {}

    def load_mcp_project_data(self, folder: str) -> Tuple[bool, dict, str, str]:
        """Load the MCP-enabled version for *folder*.

        Falls back to the latest version when no version has
        ``mcp_enabled`` set.

        Returns ``(ok, data_dict, version_str, error_msg)``.
        """
        ver, data = self.find_mcp_version(folder)
        if ver:
            return True, data, ver, ""
        return self.load_latest_project_data(folder)
