"""
Registry Service for OntoBricks.

Centralises all domain-registry management (config resolution, path
construction, domain CRUD, version management) behind a single
``RegistryService`` class and a lightweight ``RegistryCfg`` dataclass.

New registries store domain folders under ``/domains/``.  For backward
compatibility, if the new folder does not exist but the legacy
``/projects/`` folder does, the service transparently falls back to it.

Usage in a route handler::

    from back.objects.registry import RegistryCfg, RegistryService

    cfg  = RegistryCfg.from_domain(domain, settings)
    svc  = RegistryService.from_context(domain, settings)
    ok, names, msg = svc.list_domains()
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from back.core.logging import get_logger
from back.core.databricks import VolumeFileService
from back.objects.registry.registry_cache import (
    registry_cache_key,
    get_cached_registry_details,
    set_cached_registry_details,
    get_cached_registry_names,
    set_cached_registry_names,
    invalidate_registry_cache,
)

logger = get_logger(__name__)

_SCHEME_RE = re.compile(r"^https?:/[^/]")
_DEFAULT_VOLUME = "OntoBricksRegistry"
_REGISTRY_MARKER = ".registry"
_DOMAINS_FOLDER = "domains"
_LEGACY_DOMAINS_FOLDER = "projects"


# ------------------------------------------------------------------
# RegistryCfg — lightweight value object
# ------------------------------------------------------------------


@dataclass(frozen=True)
class RegistryCfg:
    """Immutable registry location triplet (catalog, schema, volume).

    ``backend`` selects the data-store implementation:
    ``"volume"`` (default) for JSON-on-UC-Volume, ``"lakebase"`` for
    Postgres-on-Lakebase. Binary artifacts live on the Volume in both
    cases, so the catalog/schema/volume triplet stays meaningful even
    when ``backend == "lakebase"``.

    ``lakebase_schema`` is the Postgres schema for registry tables. It
    is ignored when ``backend == "volume"``.
    """

    catalog: str
    schema: str
    volume: str
    backend: str = "volume"
    lakebase_schema: str = "ontobricks_registry"

    # -- constructors ------------------------------------------------

    @classmethod
    def from_volume_path(
        cls,
        path: str,
        *,
        backend: str = "volume",
        lakebase_schema: str = "ontobricks_registry",
    ) -> RegistryCfg:
        """Parse ``/Volumes/<catalog>/<schema>/<volume>`` into a RegistryCfg."""
        parts = path.strip("/").split("/")
        if len(parts) >= 4 and parts[0].lower() == "volumes":
            return cls(
                catalog=parts[1],
                schema=parts[2],
                volume=parts[3],
                backend=backend,
                lakebase_schema=lakebase_schema,
            )
        logger.warning(
            "Cannot parse volume path '%s'; expected /Volumes/<c>/<s>/<v>", path
        )
        return cls(
            catalog="",
            schema="",
            volume="",
            backend=backend,
            lakebase_schema=lakebase_schema,
        )

    @classmethod
    def from_domain(cls, domain, settings) -> RegistryCfg:
        """Build from a *DomainSession* and *Settings* with env-var fallbacks.

        When the app is deployed with a Volume resource the injected path
        (``settings.registry_volume_path``) takes highest priority so that
        admin-level Databricks App resource configuration always wins.

        ``backend`` and ``lakebase_schema`` come from the *Settings*
        (env-level) and are then overridden by the global config when
        the registry is reachable — see :class:`RegistryService`.
        """
        backend = (getattr(settings, "registry_backend", "volume") or "volume").lower()
        lb_schema = (
            getattr(settings, "lakebase_schema", "ontobricks_registry")
            or "ontobricks_registry"
        )

        vol_path = getattr(settings, "registry_volume_path", "")
        if vol_path:
            return cls.from_volume_path(
                vol_path, backend=backend, lakebase_schema=lb_schema
            )

        reg = domain.settings.get("registry", {})
        return cls(
            catalog=reg.get("catalog") or settings.registry_catalog,
            schema=reg.get("schema") or settings.registry_schema,
            volume=reg.get("volume") or settings.registry_volume or _DEFAULT_VOLUME,
            backend=(reg.get("backend") or backend).lower(),
            lakebase_schema=reg.get("lakebase_schema") or lb_schema,
        )

    @classmethod
    def from_session(cls, session_mgr, settings) -> RegistryCfg:
        """Build from *SessionManager* and *Settings*."""
        from back.objects.session.DomainSession import get_domain

        return cls.from_domain(get_domain(session_mgr), settings)

    @classmethod
    def from_dict(cls, d: Dict[str, str]) -> RegistryCfg:
        """Build from a plain dict (e.g. an existing ``registry_cfg``)."""
        return cls(
            catalog=d.get("catalog", ""),
            schema=d.get("schema", ""),
            volume=d.get("volume", "") or _DEFAULT_VOLUME,
            backend=(d.get("backend") or "volume").lower(),
            lakebase_schema=d.get("lakebase_schema") or "ontobricks_registry",
        )

    # -- helpers -----------------------------------------------------

    @property
    def is_configured(self) -> bool:
        if self.backend == "lakebase":
            return bool(self.catalog and self.schema and self.volume)
        return bool(self.catalog and self.schema and self.volume)

    def as_dict(self) -> Dict[str, str]:
        """Dict representation for backward compatibility with legacy callers."""
        return {
            "catalog": self.catalog,
            "schema": self.schema,
            "volume": self.volume,
            "backend": self.backend,
            "lakebase_schema": self.lakebase_schema,
        }


# ------------------------------------------------------------------
# RegistryService — all I/O operations
# ------------------------------------------------------------------


class RegistryService:
    """Encapsulates every registry operation regardless of backend.

    All JSON-shaped data (domains, versions, permissions, schedules,
    global config) is routed through a :class:`RegistryStore` instance
    (Volume or Lakebase). Binary artifacts (``documents/`` and
    ``*.lbug.tar.gz``) stay on the Unity Catalog Volume in both backends
    and are managed via :attr:`uc`.
    """

    def __init__(
        self,
        cfg: RegistryCfg,
        uc: VolumeFileService,
        store=None,
    ):
        self._cfg = cfg
        self._uc = uc
        self._resolved_domains_folder: Optional[str] = None
        self._store = store or self._build_store(cfg, uc)

    # -- factory -----------------------------------------------------

    @classmethod
    def from_context(cls, domain, settings) -> RegistryService:
        """One-call factory: resolve config + build VolumeFileService + store."""
        from back.core.helpers import get_databricks_host_and_token

        cfg = RegistryCfg.from_domain(domain, settings)
        host, token = get_databricks_host_and_token(domain, settings)
        uc = VolumeFileService(host=host, token=token)
        return cls(cfg, uc)

    @staticmethod
    def _build_store(cfg: RegistryCfg, uc: VolumeFileService):
        """Build the right :class:`RegistryStore` for *cfg.backend*.

        For the Volume backend we bypass the factory and instantiate
        :class:`VolumeRegistryStore` manually so we can reuse the
        already-built :class:`VolumeFileService` without duplicating
        host/token credentials.
        """
        from back.objects.registry.store import (
            RegistryFactory,
            VolumeRegistryStore,
        )

        if (cfg.backend or "volume").lower() == "lakebase":
            return RegistryFactory.lakebase(
                registry_cfg=cfg,
                schema=cfg.lakebase_schema,
            )
        store = VolumeRegistryStore.__new__(VolumeRegistryStore)
        store._cfg = cfg
        store._uc = uc
        store._resolved_domains_folder = None
        return store

    # -- properties --------------------------------------------------

    @property
    def cfg(self) -> RegistryCfg:
        return self._cfg

    @property
    def uc(self) -> VolumeFileService:
        """Expose the underlying VolumeFileService for callers that need it."""
        return self._uc

    @property
    def store(self):
        """Underlying :class:`RegistryStore` (for advanced callers)."""
        return self._store

    @property
    def cache_key(self) -> str:
        """Cache key bound to the *store identity* (backend-aware)."""
        return self._store.cache_key

    # -- path builders -----------------------------------------------

    def volume_root(self) -> str:
        c = self._cfg
        return f"/Volumes/{c.catalog}/{c.schema}/{c.volume}"

    def _resolve_domains_folder(self) -> str:
        """Return the actual domains sub-folder name inside the volume.

        New registries use ``domains/``.  If that folder does not exist
        but the legacy ``projects/`` folder does, fall back transparently
        so that existing registries keep working.
        """
        if self._resolved_domains_folder is not None:
            return self._resolved_domains_folder

        root = self.volume_root()
        new_path = f"{root}/{_DOMAINS_FOLDER}"
        ok, _, _ = self._uc.list_directory(new_path, dirs_only=True)
        if ok:
            self._resolved_domains_folder = _DOMAINS_FOLDER
            return _DOMAINS_FOLDER

        legacy_path = f"{root}/{_LEGACY_DOMAINS_FOLDER}"
        ok_legacy, _, _ = self._uc.list_directory(legacy_path, dirs_only=True)
        if ok_legacy:
            logger.info(
                "Using legacy '%s/' folder in registry volume", _LEGACY_DOMAINS_FOLDER
            )
            self._resolved_domains_folder = _LEGACY_DOMAINS_FOLDER
            return _LEGACY_DOMAINS_FOLDER

        self._resolved_domains_folder = _DOMAINS_FOLDER
        return _DOMAINS_FOLDER

    def domains_path(self) -> str:
        return f"{self.volume_root()}/{self._resolve_domains_folder()}"

    def domain_path(self, folder: str) -> str:
        return f"{self.domains_path()}/{folder}"

    def version_path(self, folder: str, version: str) -> str:
        """Return the version directory: ``.../domains/{folder}/V{version}``."""
        return f"{self.domain_path(folder)}/V{version}"

    def version_file_path(self, folder: str, version: str) -> str:
        return f"{self.version_path(folder, version)}/V{version}.json"

    def marker_path(self) -> str:
        return f"{self.volume_root()}/{_REGISTRY_MARKER}"

    def config_file_path(self) -> str:
        return f"{self.volume_root()}/.global_config.json"

    def history_file_path(self, folder: str) -> str:
        return f"{self.domain_path(folder)}/.schedule_history.json"

    # -- registry lifecycle ------------------------------------------

    def is_initialized(self) -> bool:
        """Return ``True`` when the active backend reports a usable registry.

        For the Volume backend this checks the ``.registry`` marker;
        for the Lakebase backend it probes for the ``registries`` row.
        """
        return self._store.is_initialized()

    def initialize(self, client) -> Tuple[bool, str]:
        """Bring the registry up to a usable state (idempotent).

        *client* is a ``DatabricksClient`` used by the Volume backend
        to create the UC Volume on first use; it is ignored by the
        Lakebase backend (where the database is provisioned separately).
        """
        c = self._cfg
        if (c.backend or "volume").lower() == "volume":
            return self._store.initialize(client=client)
        # Lakebase: also ensure the binary volume exists so document
        # uploads keep working regardless of which backend stores the
        # registry rows.
        if client is not None and c.catalog and c.schema and c.volume:
            try:
                volumes = client.list_volumes(c.catalog, c.schema)
                if c.volume not in volumes:
                    client.create_volume(c.catalog, c.schema, c.volume)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Could not ensure binary volume exists: %s", exc)
        return self._store.initialize()

    # -- domain CRUD -------------------------------------------------

    def list_domains(self) -> Tuple[bool, List[str], str]:
        """Return sorted domain folder names (hidden dirs excluded)."""
        return self._store.list_domain_folders()

    def list_domains_cached(self) -> Tuple[bool, List[str], str]:
        """Like :meth:`list_domains` but with an in-memory TTL cache."""
        cached = get_cached_registry_names(self.cache_key)
        if cached is not None:
            return True, cached, ""
        ok, names, msg = self.list_domains()
        if ok:
            set_cached_registry_names(self.cache_key, names)
        return ok, names, msg

    def domain_exists(self, folder: str) -> bool:
        """Check whether a domain folder already exists in the registry."""
        return self._store.domain_exists(folder)

    def list_domain_details(self) -> Tuple[bool, List[Dict[str, Any]], str]:
        """List domains with name, URI, description and enriched version list.

        Each entry in ``versions`` is a dict::

            {"version": "2", "active": True,
             "last_update": "2025-…", "last_build": "2025-…"}
        """
        return self._store.list_domains_with_metadata()

    def list_domain_details_cached(self) -> Tuple[bool, List[Dict[str, Any]], str]:
        """Like :meth:`list_domain_details` but with an in-memory TTL cache."""
        cached = get_cached_registry_details(self.cache_key)
        if cached is not None:
            return True, cached, ""
        ok, details, msg = self.list_domain_details()
        if ok:
            set_cached_registry_details(self.cache_key, details)
        return ok, details, msg

    @staticmethod
    def normalize_entity_uri(raw: str) -> str:
        """Restore double-slash after scheme when proxies collapse it."""
        if _SCHEME_RE.match(raw):
            return raw.replace(":/", "://", 1)
        return raw

    async def resolve_uri_to_domain(
        self,
        entity_uri: str,
        current_domain_name: str,
        current_folder: str,
        current_base_uri: str,
    ) -> Optional[str]:
        """Find the registry domain whose base URI longest-prefix matches *entity_uri*.

        Returns the domain folder name to bridge to, or ``None`` when the URI
        already belongs to the current domain (no switch needed) or when no
        match is found. *current_base_uri* should be normalized with
        ``rstrip('/')`` by the caller.
        """
        from back.core.helpers import run_blocking

        current_name = current_domain_name.strip().lower()
        current_folder_l = current_folder.strip().lower()
        current_base = current_base_uri.rstrip("/")

        try:
            ok, details, msg = await run_blocking(self.list_domain_details)
            if not ok:
                logger.warning(
                    "Could not list registry domains for URI resolution: %s",
                    msg,
                )
                return None

            best_match: Optional[str] = None
            best_len = 0

            for p in details:
                base = (p.get("base_uri") or "").rstrip("/")
                if not base:
                    continue
                if entity_uri.startswith(base) and len(base) > best_len:
                    best_match = p["name"]
                    best_len = len(base)

            if not best_match:
                logger.debug("No registry domain matches URI %s", entity_uri)
                return None

            if best_match.strip().lower() in (current_name, current_folder_l):
                logger.debug(
                    "URI %s belongs to the current domain; no switch needed",
                    entity_uri,
                )
                return None

            if current_base and entity_uri.startswith(current_base):
                logger.debug(
                    "URI %s matches current domain base URI; no switch needed",
                    entity_uri,
                )
                return None

            logger.info("URI %s resolved to domain '%s'", entity_uri, best_match)
            return best_match

        except Exception as exc:
            logger.warning("Error resolving domain for URI %s: %s", entity_uri, exc)
            return None

    @staticmethod
    def _extract_latest_ontology(doc: Dict[str, Any]) -> Dict[str, Any]:
        """Return the ontology dict from the latest version in a domain document."""
        versions = doc.get("versions")
        if versions:
            latest_key = sorted(versions.keys(), reverse=True)
            if latest_key:
                return versions[latest_key[0]].get("ontology", {})
        return doc.get("ontology", {})

    def list_mcp_domains(
        self, require_ontology: bool = False
    ) -> Tuple[bool, List[Dict[str, str]], str]:
        """List domains that have an MCP-enabled version.

        Returns ``(ok, domains, message)`` where each domain is
        ``{"name": ..., "description": ...}``.  When *require_ontology* is
        ``True`` only domains whose MCP version has a non-empty ``classes``
        list are included.
        """
        ok, names, msg = self._store.list_domain_folders()
        if not ok:
            return False, [], msg

        result: List[Dict[str, str]] = []
        for name in names:
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
                result.append(
                    {"name": name, "description": info.get("description", "")}
                )
            except Exception:
                logger.debug("Could not inspect domain %s", name)
        return True, result, ""

    def delete_domain(self, folder: str) -> List[str]:
        """Delete a domain (rows + binary directory) and return any errors."""
        errors: List[str] = list(self._store.delete_domain(folder))
        # Always wipe the binary directory: documents/ + *.lbug.tar.gz
        # live on the Volume regardless of backend.
        try:
            errors.extend(self.recursive_delete(self.domain_path(folder)))
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc))
        invalidate_registry_cache(self.cache_key)
        return errors

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
            logger.warning(
                "recursive_delete: could not remove directory %s: %s", dir_path, d_msg
            )

        return errors

    # -- version management ------------------------------------------

    def list_versions(self, folder: str) -> Tuple[bool, List[str], str]:
        """Return version strings (e.g. ``['2', '1']``) for a domain folder."""
        return self._store.list_versions(folder)

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
        """Read and parse a version document from the active backend."""
        return self._store.read_version(folder, version)

    def write_version(self, folder: str, version: str, data: str) -> Tuple[bool, str]:
        """Persist a version document.

        ``data`` is accepted as either a JSON string (legacy) or a
        ``dict`` (new). The store always receives a parsed dict.
        """
        if isinstance(data, str):
            try:
                payload = json.loads(data) if data else {}
            except json.JSONDecodeError as exc:
                return False, f"Invalid JSON: {exc}"
        else:
            payload = data
        ok, msg = self._store.write_version(folder, version, payload)
        if ok:
            invalidate_registry_cache(self.cache_key)
        return ok, msg

    def delete_version(self, folder: str, version: str) -> Tuple[bool, str]:
        """Delete a version (rows + binary directory)."""
        ok, msg = self._store.delete_version(folder, version)
        if not ok:
            return False, msg
        # Also remove the binary version dir on the Volume (documents/,
        # *.lbug.tar.gz). Errors are non-fatal — the JSON side is the
        # source of truth.
        try:
            errors = self.recursive_delete(self.version_path(folder, version))
            if errors:
                logger.warning(
                    "Volume cleanup for %s/V%s left errors: %s",
                    folder,
                    version,
                    "; ".join(errors),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Volume cleanup raised for %s/V%s: %s", folder, version, exc
            )
        invalidate_registry_cache(self.cache_key)
        return True, ""

    # -- load domain from registry (stateless) -----------------------

    def load_latest_domain_data(self, folder: str) -> Tuple[bool, dict, str, str]:
        """Load the latest version for *folder*.

        Returns ``(ok, data_dict, version_str, error_msg)``.
        """
        latest = self.get_latest_version(folder)
        if not latest:
            return False, {}, "", f'No versions found for domain "{folder}"'
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
            if data.get("info", {}).get("mcp_enabled"):
                return ver, data
        return None, {}

    def load_mcp_domain_data(self, folder: str) -> Tuple[bool, dict, str, str]:
        """Load the MCP-enabled version for *folder*.

        Falls back to the latest version when no version has
        ``mcp_enabled`` set.

        Returns ``(ok, data_dict, version_str, error_msg)``.
        """
        ver, data = self.find_mcp_version(folder)
        if ver:
            return True, data, ver, ""
        return self.load_latest_domain_data(folder)

    # -- document operations -------------------------------------------

    def copy_version_documents(
        self,
        folder: str,
        src_version: str,
        dst_version: str,
    ) -> Tuple[int, List[str]]:
        """Copy all documents from one version directory to another.

        Returns ``(copied_count, error_messages)``.
        """
        src_docs = f"{self.version_path(folder, src_version)}/documents"
        dst_docs = f"{self.version_path(folder, dst_version)}/documents"
        ok, items, msg = self._uc.list_directory(src_docs)
        if not ok:
            if "not found" in msg.lower():
                return 0, []
            return 0, [msg]
        errors: List[str] = []
        copied = 0
        for item in items:
            if item.get("is_directory"):
                continue
            name = item["name"]
            src_file = f"{src_docs}/{name}"
            dst_file = f"{dst_docs}/{name}"
            r_ok, content, r_msg = self._uc.read_binary_file(src_file)
            if not r_ok:
                errors.append(f"Read {name}: {r_msg}")
                continue
            w_ok, w_msg = self._uc.write_binary_file(dst_file, content)
            if not w_ok:
                errors.append(f"Write {name}: {w_msg}")
                continue
            copied += 1
        return copied, errors

    # -- bridge aggregation ---------------------------------------------

    def list_all_bridges(self) -> Tuple[bool, List[Dict[str, Any]], str]:
        """Collect all bridges across every domain in the registry.

        Iterates over each domain, loads its latest version, and extracts
        bridges from ``ontology.classes[].bridges``.

        Returns ``(ok, domains_with_bridges, error_msg)`` where each entry
        has ``name``, ``base_uri``, and ``bridges`` (list of bridge dicts
        with ``source_class``, ``source_class_uri``, ``source_emoji``,
        ``target_domain``, ``target_class_name``, ``target_class_uri``,
        ``label``).
        """
        ok, items, msg = self._uc.list_directory(self.domains_path(), dirs_only=True)
        if not ok:
            return False, [], msg

        result: List[Dict[str, Any]] = []
        for item in sorted(items, key=lambda i: i["name"]):
            name = item["name"]
            if name.startswith("."):
                continue

            try:
                d_ok, data, _ver, d_msg = self.load_latest_domain_data(name)
                if not d_ok:
                    continue

                ontology = self._extract_latest_ontology(data)
                base_uri = ontology.get("base_uri", "")
                raw_classes = ontology.get("classes", [])

                bridges: List[Dict[str, Any]] = []
                for cls in raw_classes:
                    cls_bridges = cls.get("bridges") or []
                    if not cls_bridges:
                        continue
                    for b in cls_bridges:
                        bridges.append(
                            {
                                "source_class": cls.get("name", ""),
                                "source_class_uri": cls.get("uri", ""),
                                "source_emoji": cls.get("emoji", "📦"),
                                "target_domain": b.get("target_domain")
                                or b.get("target_project", ""),
                                "target_class_name": b.get("target_class_name", ""),
                                "target_class_uri": b.get("target_class_uri", ""),
                                "label": b.get("label", ""),
                            }
                        )

                result.append(
                    {
                        "name": name,
                        "base_uri": base_uri,
                        "bridges": bridges,
                    }
                )
            except Exception:
                logger.debug("Could not read bridges for domain %s", name)

        return True, result, ""

    # -- one-time layout migration -------------------------------------

    def migrate_domain_layout(self, folder: str) -> Tuple[bool, str]:
        """Migrate a single domain from the flat layout to the versioned layout.

        Flat layout (old)::

            domains/{folder}/v1.json
            domains/{folder}/v2.json
            domains/{folder}/documents/
            domains/{folder}/ontobricks_*.lbug.tar.gz

        Versioned layout (new)::

            domains/{folder}/V1/V1.json
            domains/{folder}/V1/documents/
            domains/{folder}/V1/ontobricks_*.lbug.tar.gz
            domains/{folder}/V2/V2.json
            domains/{folder}/V2/documents/
            domains/{folder}/V2/ontobricks_*.lbug.tar.gz

        Returns ``(ok, message)``.
        """
        base = self.domain_path(folder)

        ok, items, msg = self._uc.list_directory(base)
        if not ok:
            return False, f"Cannot list {base}: {msg}"

        flat_versions: List[str] = []
        lbug_files: List[Dict[str, str]] = []
        has_documents = False

        for item in items:
            name = item["name"]
            if name.startswith("v") and name.endswith(".json"):
                flat_versions.append(name[1:-5])  # "v1.json" -> "1"
            elif name.endswith(".lbug.tar.gz"):
                lbug_files.append({"name": name, "path": item["path"]})
            elif name == "documents" and item.get("is_directory"):
                has_documents = True

        if not flat_versions:
            return True, "No flat version files found — nothing to migrate"

        flat_versions.sort(
            key=lambda v: [int(x) for x in v.split(".")],
            reverse=True,
        )
        latest_version = flat_versions[0]
        errors: List[str] = []

        for ver in flat_versions:
            old_file = f"{base}/v{ver}.json"
            new_file = self.version_file_path(folder, ver)
            r_ok, content, r_msg = self._uc.read_file(old_file)
            if not r_ok:
                errors.append(f"Read v{ver}.json: {r_msg}")
                continue
            w_ok, w_msg = self._uc.write_file(new_file, content)
            if not w_ok:
                errors.append(f"Write V{ver}/V{ver}.json: {w_msg}")
                continue
            d_ok, d_msg = self._uc.delete_file(old_file)
            if not d_ok:
                errors.append(f"Delete old v{ver}.json: {d_msg}")

        for lbug in lbug_files:
            name = lbug["name"]
            matched_version: Optional[str] = None
            for ver in flat_versions:
                if f"_V{ver}" in name:
                    matched_version = ver
                    break
            target_version = matched_version or latest_version
            old_path = f"{base}/{name}"
            new_path = f"{self.version_path(folder, target_version)}/{name}"
            r_ok, content, r_msg = self._uc.read_binary_file(old_path)
            if not r_ok:
                errors.append(f"Read {name}: {r_msg}")
                continue
            w_ok, w_msg = self._uc.write_binary_file(new_path, content)
            if not w_ok:
                errors.append(f"Write {name}: {w_msg}")
                continue
            d_ok, d_msg = self._uc.delete_file(old_path)
            if not d_ok:
                errors.append(f"Delete old {name}: {d_msg}")

        if has_documents:
            src_docs = f"{base}/documents"
            dst_docs = f"{self.version_path(folder, latest_version)}/documents"
            doc_ok, doc_items, doc_msg = self._uc.list_directory(src_docs)
            if doc_ok:
                for item in doc_items:
                    if item.get("is_directory"):
                        continue
                    name = item["name"]
                    r_ok, content, r_msg = self._uc.read_binary_file(
                        f"{src_docs}/{name}"
                    )
                    if not r_ok:
                        errors.append(f"Read doc {name}: {r_msg}")
                        continue
                    w_ok, w_msg = self._uc.write_binary_file(
                        f"{dst_docs}/{name}", content
                    )
                    if not w_ok:
                        errors.append(f"Write doc {name}: {w_msg}")
                        continue
                    self._uc.delete_file(f"{src_docs}/{name}")
                self._uc.delete_directory(src_docs)

        if errors:
            return False, f"Migration completed with errors: {'; '.join(errors)}"
        return True, f"Migrated {len(flat_versions)} version(s) to versioned layout"
