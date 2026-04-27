"""JSON-on-Volume implementation of :class:`RegistryStore`.

This is the original storage layout: every piece of registry-shaped
data is a JSON file underneath ``/Volumes/<catalog>/<schema>/<volume>``
on Unity Catalog.

Layout (unchanged)::

    /Volumes/<c>/<s>/<v>/.registry                # marker
    /Volumes/<c>/<s>/<v>/.global_config.json      # warehouse_id, schedules, …
    /Volumes/<c>/<s>/<v>/domains/<folder>/V1/V1.json
    /Volumes/<c>/<s>/<v>/domains/<folder>/V1/documents/…
    /Volumes/<c>/<s>/<v>/domains/<folder>/.domain_permissions.json
    /Volumes/<c>/<s>/<v>/domains/<folder>/.schedule_history.json

Binary artifacts (``documents/`` and ``*.lbug.tar.gz``) are *not* the
store's responsibility — :class:`back.objects.registry.RegistryService`
manages them directly via :class:`VolumeFileService` because the same
layout is used regardless of which store backs the registry.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from back.core.databricks import VolumeFileService
from back.core.logging import get_logger
from back.objects.registry.registry_cache import (
    invalidate_registry_cache,
    registry_cache_key,
)

from ..base import DomainSummary, RegistryStore, ScheduleHistoryEntry

logger = get_logger(__name__)

_REGISTRY_MARKER = ".registry"
_DOMAINS_FOLDER = "domains"
_LEGACY_DOMAINS_FOLDER = "projects"
_GLOBAL_CONFIG_FILENAME = ".global_config.json"
_DOMAIN_PERMISSIONS_FILENAME = ".domain_permissions.json"
_SCHEDULE_HISTORY_FILENAME = ".schedule_history.json"
_SCHEDULES_KEY = "schedules"


class VolumeRegistryStore(RegistryStore):
    """JSON-files-on-UC-Volume store. Default backend."""

    def __init__(self, *, registry_cfg, host: str = "", token: str = ""):
        self._cfg = registry_cfg
        self._uc = VolumeFileService(host=host, token=token)
        self._resolved_domains_folder: Optional[str] = None

    # ------------------------------------------------------------------
    # Path builders (also used by RegistryService for binary I/O)
    # ------------------------------------------------------------------

    @property
    def cfg(self):
        return self._cfg

    @property
    def uc(self) -> VolumeFileService:
        return self._uc

    def volume_root(self) -> str:
        c = self._cfg
        return f"/Volumes/{c.catalog}/{c.schema}/{c.volume}"

    def _resolve_domains_folder(self) -> str:
        if self._resolved_domains_folder is not None:
            return self._resolved_domains_folder
        root = self.volume_root()
        ok, _, _ = self._uc.list_directory(f"{root}/{_DOMAINS_FOLDER}", dirs_only=True)
        if ok:
            self._resolved_domains_folder = _DOMAINS_FOLDER
            return _DOMAINS_FOLDER
        ok_legacy, _, _ = self._uc.list_directory(
            f"{root}/{_LEGACY_DOMAINS_FOLDER}", dirs_only=True
        )
        if ok_legacy:
            logger.info("Using legacy '%s/' folder", _LEGACY_DOMAINS_FOLDER)
            self._resolved_domains_folder = _LEGACY_DOMAINS_FOLDER
            return _LEGACY_DOMAINS_FOLDER
        self._resolved_domains_folder = _DOMAINS_FOLDER
        return _DOMAINS_FOLDER

    def domains_path(self) -> str:
        return f"{self.volume_root()}/{self._resolve_domains_folder()}"

    def domain_path(self, folder: str) -> str:
        return f"{self.domains_path()}/{folder}"

    def version_path(self, folder: str, version: str) -> str:
        return f"{self.domain_path(folder)}/V{version}"

    def version_file_path(self, folder: str, version: str) -> str:
        return f"{self.version_path(folder, version)}/V{version}.json"

    def marker_path(self) -> str:
        return f"{self.volume_root()}/{_REGISTRY_MARKER}"

    def global_config_path(self) -> str:
        return f"{self.volume_root()}/{_GLOBAL_CONFIG_FILENAME}"

    def domain_permissions_path(self, folder: str) -> str:
        return f"{self.domain_path(folder)}/{_DOMAIN_PERMISSIONS_FILENAME}"

    def history_path(self, folder: str) -> str:
        return f"{self.domain_path(folder)}/{_SCHEDULE_HISTORY_FILENAME}"

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    def backend(self) -> str:
        return "volume"

    @property
    def cache_key(self) -> str:
        c = self._cfg
        # Prefix with the backend tag so a switch at runtime (volume <-> lakebase)
        # invalidates the registry-level TTL cache automatically.
        return f"volume:{registry_cache_key(c.catalog, c.schema, c.volume)}"

    def is_initialized(self) -> bool:
        ok, _, _ = self._uc.read_file(self.marker_path())
        return ok

    def initialize(self, *, client: Any = None) -> Tuple[bool, str]:
        c = self._cfg
        if client is not None:
            volumes = client.list_volumes(c.catalog, c.schema)
            if c.volume not in volumes:
                if not client.create_volume(c.catalog, c.schema, c.volume):
                    return False, f"Failed to create volume {c.volume}"
        ok, msg = self._uc.write_file(
            self.marker_path(), "OntoBricks Domain Registry", overwrite=True
        )
        if not ok:
            return False, msg
        logger.info("Registry initialised at %s.%s.%s", c.catalog, c.schema, c.volume)
        return True, f"Registry initialized: {c.catalog}.{c.schema}.{c.volume}"

    # ------------------------------------------------------------------
    # Domain listings
    # ------------------------------------------------------------------

    def list_domain_folders(self) -> Tuple[bool, List[str], str]:
        ok, items, msg = self._uc.list_directory(self.domains_path(), dirs_only=True)
        if not ok:
            return False, [], msg
        names = sorted(i["name"] for i in items if not i["name"].startswith("."))
        return True, names, ""

    def list_domains_with_metadata(self) -> Tuple[bool, List[DomainSummary], str]:
        ok, items, msg = self._uc.list_directory(self.domains_path(), dirs_only=True)
        if not ok:
            return False, [], msg

        result: List[DomainSummary] = []
        for item in sorted(items, key=lambda i: i["name"]):
            name = item["name"]
            if name.startswith("."):
                continue

            description = ""
            base_uri = ""
            version_objects: List[Dict[str, Any]] = []
            try:
                versions = self._list_versions_sorted(name)
                for idx, ver in enumerate(versions):
                    active = False
                    last_update = ""
                    last_build = ""
                    f_ok, content, _ = self._uc.read_file(
                        self.version_file_path(name, ver)
                    )
                    if f_ok and content:
                        doc = json.loads(content)
                        info = doc.get("info", {})
                        active = bool(info.get("mcp_enabled"))
                        last_update = info.get("last_update", "")
                        last_build = info.get("last_build", "")
                        if idx == 0:
                            description = info.get("description", "")
                            base_uri = self._latest_ontology(doc).get("base_uri", "")
                    version_objects.append(
                        {
                            "version": ver,
                            "active": active,
                            "last_update": last_update,
                            "last_build": last_build,
                        }
                    )
            except Exception:
                logger.debug("Could not read details for domain %s", name)

            result.append(
                {
                    "name": name,
                    "base_uri": base_uri,
                    "description": description,
                    "versions": version_objects,
                }
            )

        return True, result, ""

    def domain_exists(self, folder: str) -> bool:
        ok, names, _ = self.list_domain_folders()
        return ok and folder in names

    def delete_domain(self, folder: str) -> List[str]:
        errors = self._recursive_delete(self.domain_path(folder))
        invalidate_registry_cache(self.cache_key)
        return errors

    # ------------------------------------------------------------------
    # Versions
    # ------------------------------------------------------------------

    def list_versions(self, folder: str) -> Tuple[bool, List[str], str]:
        ok, items, msg = self._uc.list_directory(
            self.domain_path(folder), dirs_only=True
        )
        if not ok:
            return False, [], msg
        versions = [
            d["name"][1:]
            for d in items
            if d["name"].startswith("V") and d["name"][1:].replace(".", "").isdigit()
        ]
        return True, versions, ""

    def read_version(
        self, folder: str, version: str
    ) -> Tuple[bool, Dict[str, Any], str]:
        ok, content, msg = self._uc.read_file(self.version_file_path(folder, version))
        if not ok:
            return False, {}, msg
        try:
            return True, json.loads(content), ""
        except json.JSONDecodeError as exc:
            return False, {}, f"Invalid JSON: {exc}"

    def write_version(
        self, folder: str, version: str, data: Dict[str, Any]
    ) -> Tuple[bool, str]:
        path = self.version_file_path(folder, version)
        return self._uc.write_file(path, json.dumps(data, indent=2), overwrite=True)

    def delete_version(self, folder: str, version: str) -> Tuple[bool, str]:
        errors = self._recursive_delete(self.version_path(folder, version))
        if errors:
            return False, "; ".join(errors)
        invalidate_registry_cache(self.cache_key)
        return True, ""

    # ------------------------------------------------------------------
    # Permissions
    # ------------------------------------------------------------------

    def load_domain_permissions(self, folder: str) -> Dict[str, Any]:
        path = self.domain_permissions_path(folder)
        try:
            ok, content, _ = self._uc.read_file(path)
            if ok and content:
                return json.loads(content)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Could not load domain permissions for %s: %s", folder, exc)
        return {"version": 1, "permissions": []}

    def save_domain_permissions(
        self, folder: str, data: Dict[str, Any]
    ) -> Tuple[bool, str]:
        path = self.domain_permissions_path(folder)
        return self._uc.write_file(path, json.dumps(data, indent=2), overwrite=True)

    # ------------------------------------------------------------------
    # Schedules + history
    # ------------------------------------------------------------------

    def load_schedules(self) -> Dict[str, Dict[str, Any]]:
        cfg = self.load_global_config()
        return dict(cfg.get(_SCHEDULES_KEY) or {})

    def save_schedules(
        self, schedules: Dict[str, Dict[str, Any]]
    ) -> Tuple[bool, str]:
        return self.save_global_config({_SCHEDULES_KEY: schedules})

    def load_schedule_history(self, folder: str) -> List[ScheduleHistoryEntry]:
        path = self.history_path(folder)
        try:
            ok, content, _ = self._uc.read_file(path)
            if ok and content:
                return json.loads(content)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Could not load history for %s: %s", folder, exc)
        return []

    def append_schedule_history(
        self, folder: str, entry: ScheduleHistoryEntry, *, max_entries: int = 50
    ) -> None:
        entries = self.load_schedule_history(folder)
        entries.append(dict(entry))
        if len(entries) > max_entries:
            entries = entries[-max_entries:]
        path = self.history_path(folder)
        try:
            self._uc.write_file(path, json.dumps(entries, indent=2), overwrite=True)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not save history for '%s': %s", folder, exc)

    # ------------------------------------------------------------------
    # Global config
    # ------------------------------------------------------------------

    def load_global_config(self) -> Dict[str, Any]:
        path = self.global_config_path()
        try:
            ok, content, _ = self._uc.read_file(path)
            if ok and content:
                data = json.loads(content)
                data.pop("schedule_history", None)
                for sched in (data.get(_SCHEDULES_KEY) or {}).values():
                    sched.pop("registry_cfg", None)
                return data
        except Exception as exc:  # noqa: BLE001
            logger.debug("Could not load global config: %s", exc)
        return {}

    def save_global_config(self, updates: Dict[str, Any]) -> Tuple[bool, str]:
        data = self.load_global_config()
        data["version"] = data.get("version", 1)
        data.pop("schedule_history", None)
        data.update(updates)
        for sched in (data.get(_SCHEDULES_KEY) or {}).values():
            sched.pop("registry_cfg", None)
        path = self.global_config_path()
        return self._uc.write_file(path, json.dumps(data, indent=2), overwrite=True)

    # ------------------------------------------------------------------
    # Misc
    # ------------------------------------------------------------------

    def domain_folder_id(self, folder: str) -> Optional[str]:
        return folder if self.domain_exists(folder) else None

    def describe(self) -> Dict[str, Any]:
        c = self._cfg
        return {
            "backend": self.backend,
            "cache_key": self.cache_key,
            "catalog": c.catalog,
            "schema": c.schema,
            "volume": c.volume,
            "volume_path": self.volume_root() if c.volume else "",
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _list_versions_sorted(self, folder: str) -> List[str]:
        ok, versions, _ = self.list_versions(folder)
        if not ok:
            return []
        versions.sort(key=lambda v: [int(x) for x in v.split(".")], reverse=True)
        return versions

    @staticmethod
    def _latest_ontology(doc: Dict[str, Any]) -> Dict[str, Any]:
        versions = doc.get("versions")
        if versions:
            keys = sorted(versions.keys(), reverse=True)
            if keys:
                return versions[keys[0]].get("ontology", {})
        return doc.get("ontology", {})

    def _recursive_delete(self, dir_path: str) -> List[str]:
        dir_path = dir_path.rstrip("/")
        errors: List[str] = []
        ok, items, msg = self._uc.list_directory(dir_path)
        if not ok:
            errors.append(f"Cannot list {dir_path}: {msg}")
            return errors
        for item in items:
            item_path = item["path"].rstrip("/")
            if item.get("is_directory", False):
                errors.extend(self._recursive_delete(item_path))
            else:
                d_ok, d_msg = self._uc.delete_file(item_path)
                if not d_ok:
                    errors.append(d_msg)
        d_ok, d_msg = self._uc.delete_directory(dir_path)
        if not d_ok:
            errors.append(d_msg)
        return errors
