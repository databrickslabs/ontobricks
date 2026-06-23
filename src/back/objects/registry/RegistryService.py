"""
Registry Service for OntoBricks.

Centralises all domain-registry management (config resolution, path
construction, domain CRUD, version management) behind a single
``RegistryService`` class and a lightweight ``RegistryCfg`` dataclass.

The registry is **Lakebase-only**: JSON-shaped registry data (domains,
versions, permissions, schedules, global config) lives in the Postgres
schema named by ``lakebase_schema``. The Unity Catalog Volume triplet
(``catalog``/``schema``/``volume``) is kept around solely for
domain-scoped binary artefacts — the ``documents/`` uploads imported by
the ontology designer. The historical JSON-on-Volume backend was
removed in v0.4.0; existing deployments must run
``scripts/migrate-registry-to-lakebase.sh`` once to copy their data
into Lakebase.

New registries store domain folders under ``/domains/``.  For backward
compatibility, if the new folder does not exist but the legacy
``/projects/`` folder does, the service transparently falls back to it
when reading binaries.

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

    Lakebase is the sole registry backend. ``lakebase_schema`` is the
    Postgres schema for registry tables.

    ``lakebase_database`` (optional) overrides the bound Postgres
    database name. When empty (the default), the runtime uses the
    ``PGDATABASE`` env var auto-injected by the Databricks Apps
    runtime from the ``database`` resource binding. Setting this lets
    an admin point the registry at any other database that lives on
    the same Lakebase instance — provided the app's service principal
    has ``CONNECT`` on it. The Lakebase JWT scope is per-instance, so
    no token re-mint is required.

    The ``catalog``/``schema``/``volume`` triplet locates the Unity
    Catalog Volume where domain-scoped binary artefacts (the
    ``documents/`` uploads imported by the ontology designer) live; it
    is not used for registry rows.
    """

    catalog: str
    schema: str
    volume: str
    lakebase_schema: str = "ontobricks_registry"
    lakebase_database: str = ""

    # -- constructors ------------------------------------------------

    @classmethod
    def from_volume_path(
        cls,
        path: str,
        *,
        lakebase_schema: str = "ontobricks_registry",
        lakebase_database: str = "",
    ) -> RegistryCfg:
        """Parse ``/Volumes/<catalog>/<schema>/<volume>`` into a RegistryCfg."""
        parts = path.strip("/").split("/")
        if len(parts) >= 4 and parts[0].lower() == "volumes":
            return cls(
                catalog=parts[1],
                schema=parts[2],
                volume=parts[3],
                lakebase_schema=lakebase_schema,
                lakebase_database=lakebase_database,
            )
        logger.warning(
            "Cannot parse volume path '%s'; expected /Volumes/<c>/<s>/<v>", path
        )
        return cls(
            catalog="",
            schema="",
            volume="",
            lakebase_schema=lakebase_schema,
            lakebase_database=lakebase_database,
        )

    @classmethod
    def from_domain(
        cls,
        domain,
        settings,
        *,
        prefer_volume_binding: bool = False,
    ) -> RegistryCfg:
        """Build from a *DomainSession* and *Settings* with env-var fallbacks.

        Resolution order (highest priority first):

        1. ``domain.settings["registry"]`` — admin choices made from the
           Settings UI for the Lakebase-side knobs (``lakebase_schema`` /
           ``lakebase_database``). These win even on a Databricks Apps
           deployment where the Volume is bound by the platform.
        2. **Lakebase ``registries`` row** — when reachable, the
           catalog/schema/volume triplet is read from the row so binary
           archive paths and Delta view names follow where artefacts
           were archived. **However**, when step 3's bound Volume path
           is present and parses successfully, its catalog/schema/volume
           **replace** the row values for the UC triplet. Otherwise a
           Databricks Apps deployment whose bundle Volume name changed
           (or whose ``registries`` row still holds the env default
           ``OntoBricksRegistry``) would keep resolving binary paths
           against the wrong Volume while the platform had mounted the
           resource named in ``REGISTRY_VOLUME_PATH``.
        3. ``settings.registry_volume_path`` — when present (Databricks
           Apps with a bound Volume resource) and parses as
           ``/Volumes/<c>/<s>/<v>``, that triplet is the authoritative UC
           location for this process (merged into step 2 when the
           registries row is found; used alone when step 2 does not
           return a row).
        4. ``settings.*`` env vars — last-resort fallback for catalog,
           schema, volume, ``lakebase_schema`` and ``lakebase_database``.

        ``prefer_volume_binding`` (Initialize path only): when ``True``
        the Lakebase row read in step 2 is skipped. Lets the Initialize
        flow re-pin the registry triplet to the *current* Volume
        binding so a re-bind + re-init cycle propagates the new
        catalog/schema/volume into the ``registries`` row. Without this
        flag, re-initialising on a re-bound app silently re-upserts the
        stale triplet that was already in the row, leaving downstream
        artefact paths pointing at the previous Volume.
        """
        env_lb_schema = (
            getattr(settings, "lakebase_schema", "ontobricks_registry")
            or "ontobricks_registry"
        )
        env_lb_database = getattr(settings, "lakebase_database", "") or ""

        reg = domain.settings.get("registry", {}) if domain is not None else {}
        lb_schema = reg.get("lakebase_schema") or env_lb_schema
        lb_database = reg.get("lakebase_database") or env_lb_database

        vol_path = getattr(settings, "registry_volume_path", "")
        bound_cfg: Optional[RegistryCfg] = None
        if vol_path:
            parsed = cls.from_volume_path(
                vol_path,
                lakebase_schema=lb_schema,
                lakebase_database=lb_database,
            )
            if parsed.catalog and parsed.schema and parsed.volume:
                bound_cfg = parsed

        # Read catalog/schema/volume from the Lakebase ``registries`` row
        # so binary-archive paths and Delta view names point where
        # artefacts actually live. When ``bound_cfg`` is set (Databricks
        # Apps Volume resource binding), its triplet replaces the row's
        # so UC paths match the mounted Volume. Fail-soft — if Lakebase
        # is unreachable we fall through to the bound-Volume / env-var
        # chain. Skipped when ``prefer_volume_binding`` is True
        # (Initialize path) so re-binding the Volume + re-running
        # Initialize propagates the new triplet into the row.
        if not prefer_volume_binding:
            try:
                from back.objects.registry.store.lakebase.store import (
                    fetch_lakebase_registry_triplet,
                )

                triplet = fetch_lakebase_registry_triplet(lb_schema, lb_database)
            except Exception:  # noqa: BLE001 -- best-effort, Lakebase may be unreachable
                triplet = None
            if triplet:
                cat, sch, vol = triplet
                if bound_cfg is not None:
                    cat, sch, vol = (
                        bound_cfg.catalog,
                        bound_cfg.schema,
                        bound_cfg.volume,
                    )
                return cls(
                    catalog=cat,
                    schema=sch,
                    volume=vol or _DEFAULT_VOLUME,
                    lakebase_schema=lb_schema,
                    lakebase_database=lb_database,
                )

        if vol_path:
            return cls.from_volume_path(
                vol_path,
                lakebase_schema=lb_schema,
                lakebase_database=lb_database,
            )

        return cls(
            catalog=reg.get("catalog") or settings.registry_catalog,
            schema=reg.get("schema") or settings.registry_schema,
            volume=reg.get("volume") or settings.registry_volume or _DEFAULT_VOLUME,
            lakebase_schema=lb_schema,
            lakebase_database=lb_database,
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
            lakebase_schema=d.get("lakebase_schema") or "ontobricks_registry",
            lakebase_database=d.get("lakebase_database") or "",
        )

    # -- helpers -----------------------------------------------------

    @property
    def is_configured(self) -> bool:
        return bool(self.catalog and self.schema and self.volume)

    def as_dict(self) -> Dict[str, str]:
        """Dict representation for backward compatibility with legacy callers."""
        return {
            "catalog": self.catalog,
            "schema": self.schema,
            "volume": self.volume,
            "lakebase_schema": self.lakebase_schema,
            "lakebase_database": self.lakebase_database,
        }


# ------------------------------------------------------------------
# RegistryService — all I/O operations
# ------------------------------------------------------------------


class RegistryService:
    """Encapsulates every registry operation.

    JSON-shaped data (domains, versions, permissions, schedules,
    global config) is routed through the Lakebase
    :class:`RegistryStore`. Domain-scoped binary artefacts (the
    ``documents/`` uploads imported by the ontology designer) stay on
    the Unity Catalog Volume and are managed via :attr:`uc`.
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
    def from_context(
        cls,
        domain,
        settings,
        *,
        prefer_volume_binding: bool = False,
    ) -> RegistryService:
        """One-call factory: resolve config + build VolumeFileService + store.

        ``prefer_volume_binding`` (Initialize path only) is forwarded to
        :meth:`RegistryCfg.from_domain` — see that docstring for why it
        matters when re-running Initialize against a re-bound app.
        """
        from back.core.helpers import get_databricks_host_and_token

        cfg = RegistryCfg.from_domain(
            domain, settings, prefer_volume_binding=prefer_volume_binding
        )
        host, token = get_databricks_host_and_token(domain, settings)
        uc = VolumeFileService(host=host, token=token)
        return cls(cfg, uc)

    @staticmethod
    def _build_store(cfg: RegistryCfg, uc: VolumeFileService):
        """Build the Lakebase :class:`RegistryStore`.

        ``uc`` is accepted for parity with the constructor signature
        but unused — registry rows live entirely in Postgres. We route
        through :class:`RegistryFactory` and forward both the schema
        *and the database override* (``cfg.lakebase_database``).
        Forgetting the latter would silently fall back to the bound
        ``PGDATABASE`` even when the admin picked a different database
        in Settings.
        """
        from back.objects.registry.store import RegistryFactory

        return RegistryFactory.lakebase(
            registry_cfg=cfg,
            schema=cfg.lakebase_schema,
            database=cfg.lakebase_database,
        )

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
        """Cache key bound to the Lakebase store identity."""
        return self._store.cache_key

    # -- path builders (Unity Catalog Volume side) -------------------
    #
    # Domain-scoped binary artefacts (the ``documents/`` uploads
    # imported by the ontology designer) live on the Unity Catalog
    # Volume regardless of where the JSON registry rows live (now
    # always Lakebase). The path builders below produce those Volume
    # paths and are intentionally store-agnostic — they
    # talk to :class:`VolumeFileService` directly via :attr:`uc`.

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
        """Return ``True`` when the Lakebase store reports a usable registry."""
        return self._store.is_initialized()

    def initialize(self, client) -> Tuple[bool, str]:
        """Bring the registry up to a usable state (idempotent).

        *client* is a ``DatabricksClient`` used to ensure the UC Volume
        for domain-scoped binary artefacts (the ``documents/`` uploads)
        exists. Registry rows live in Lakebase Postgres.

        Callers should construct this service with
        ``RegistryService.from_context(..., prefer_volume_binding
        =True)`` so :attr:`cfg` reflects the *current* Volume binding
        rather than the (potentially stale) Lakebase ``registries``
        row. Without that flag, re-running Initialize after re-binding
        the Volume resource silently re-upserts the old triplet.
        """
        c = self._cfg
        # Ensure the binary volume exists. Failures here are surfaced
        # to the caller (instead of being swallowed as a warning) so
        # the admin sees exactly what went wrong if the service
        # principal lacks ``CREATE VOLUME`` on the target schema. The
        # store's ``initialize`` still runs after a creation failure
        # so the schema/tables come up regardless — the Volume can be
        # granted/created out-of-band and a re-run will be a no-op on
        # the schema side.
        volume_msg = ""
        if client is not None and c.catalog and c.schema and c.volume:
            try:
                volumes = client.list_volumes(c.catalog, c.schema)
                if c.volume not in volumes:
                    if client.create_volume(c.catalog, c.schema, c.volume):
                        volume_msg = (
                            f" Created binary volume "
                            f"{c.catalog}.{c.schema}.{c.volume}."
                        )
                        logger.info(
                            "Created binary volume %s.%s.%s",
                            c.catalog,
                            c.schema,
                            c.volume,
                        )
                    else:
                        volume_msg = (
                            f" WARNING: could not create binary volume "
                            f"{c.catalog}.{c.schema}.{c.volume} — check "
                            f"the app service principal's CREATE VOLUME "
                            f"privilege on {c.catalog}.{c.schema}."
                        )
                        logger.warning(
                            "Failed to create binary volume %s.%s.%s",
                            c.catalog,
                            c.schema,
                            c.volume,
                        )
                else:
                    volume_msg = (
                        f" Binary volume "
                        f"{c.catalog}.{c.schema}.{c.volume} already exists."
                    )
            except Exception as exc:  # noqa: BLE001
                volume_msg = (
                    f" WARNING: volume probe failed for "
                    f"{c.catalog}.{c.schema}.{c.volume}: {exc}"
                )
                logger.warning(
                    "Could not ensure binary volume exists: %s", exc
                )

        ok, store_msg = self._store.initialize()
        return ok, (store_msg + volume_msg).strip()

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
        # Always wipe the binary directory (documents/ uploads live on
        # the Unity Catalog Volume).
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
            # Directory doesn't exist — nothing to delete, not an error.
            if "not found" in msg.lower() or "404" in msg:
                logger.info("recursive_delete: %s does not exist, skipping", dir_path)
                return errors
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
        """Read and parse a version document from Lakebase."""
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
        # Also remove the binary version dir on the Volume (documents/
        # uploads). Errors are non-fatal — the JSON side is the source
        # of truth.
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

    # -- build-run trace (analytics) ---------------------------------

    def record_build_run(self, folder: str, entry: dict) -> None:
        """Persist a build-run trace row for *folder* (best-effort).

        Never raises — a failed trace must not break a build. See
        :meth:`RegistryStore.record_build_run`.
        """
        try:
            self._store.record_build_run(folder, entry)
        except Exception as exc:  # noqa: BLE001
            logger.warning("record_build_run(%s) raised: %s", folder, exc)

    def load_build_runs(
        self, folder: str, *, version: Optional[str] = None, limit: int = 100
    ) -> list:
        """Newest-first build runs for *folder* (optionally one version)."""
        return self._store.load_build_runs(folder, version=version, limit=limit)

    def build_analytics(
        self, folder: str, *, version: Optional[str] = None
    ) -> dict:
        """Aggregate build statistics for *folder* (optionally one version)."""
        return self._store.build_analytics(folder, version=version)

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

    def find_published_version(self, folder: str) -> Tuple[Optional[str], dict]:
        """Find the numeric-latest ``PUBLISHED`` version for *folder*.

        The API/MCP surface serves the highest-numbered version whose
        lifecycle ``status`` is ``PUBLISHED``. Multiple PUBLISHED versions
        may coexist; this returns the most recent one.

        Returns ``(version_str, data_dict)`` or ``(None, {})`` when no
        version is PUBLISHED.
        """
        for ver in self.list_versions_sorted(folder):
            ok, data, _ = self.read_version(folder, ver)
            if not ok:
                continue
            if data.get("info", {}).get("status") == "PUBLISHED":
                return ver, data
        return None, {}

    # Backwards-compatible alias. The old "MCP-enabled" flag has been
    # replaced by the lifecycle ``status``; "the MCP version" now means
    # "the latest PUBLISHED version".
    def find_mcp_version(self, folder: str) -> Tuple[Optional[str], dict]:
        return self.find_published_version(folder)

    def load_published_domain_data(
        self, folder: str
    ) -> Tuple[bool, dict, str, str]:
        """Load the numeric-latest PUBLISHED version for *folder*.

        Unlike :meth:`load_latest_domain_data` there is **no fallback** to
        a non-PUBLISHED version — the API/MCP surface only serves data for
        PUBLISHED versions.

        Returns ``(ok, data_dict, version_str, error_msg)``.
        """
        ver, data = self.find_published_version(folder)
        if ver:
            return True, data, ver, ""
        return (
            False,
            {},
            "",
            f'No PUBLISHED version available for domain "{folder}"',
        )

    def set_version_status(
        self, folder: str, version: str, status: str
    ) -> Tuple[bool, str]:
        """Set the lifecycle ``status`` of a single (domain, version)."""
        ok, msg = self._store.update_version_status(folder, version, status)
        if ok:
            invalidate_registry_cache(self.cache_key)
        return ok, msg

    # -- review / validation audit log -------------------------------

    def record_review_event(
        self,
        folder: str,
        version: str,
        actor: str,
        action: str,
        *,
        from_status: str = "",
        to_status: str = "",
        comment: str = "",
        meta: Optional[dict] = None,
    ) -> Tuple[bool, str]:
        """Append a review-audit row for ``(folder, version)`` (best-effort)."""
        return self._store.record_review_event(
            folder,
            version,
            actor,
            action,
            from_status=from_status,
            to_status=to_status,
            comment=comment,
            meta=meta,
        )

    def list_review_events(
        self, folder: str, version: Optional[str] = None
    ) -> list:
        """Oldest-first review events for *folder* (optionally one version)."""
        return self._store.list_review_events(folder, version)

    def list_all_review_events(self) -> list:
        """All review events across the registry (oldest-first)."""
        return self._store.list_all_review_events()

    # -- collaborative comments + tasks ------------------------------

    def insert_comment(
        self,
        folder: str,
        version: str,
        *,
        author: str,
        body: str,
        parent_id: Optional[str] = None,
    ) -> Optional[dict]:
        """Append a discussion comment; return the created row or None."""
        return self._store.insert_comment(
            folder,
            version,
            author=author,
            body=body,
            parent_id=parent_id,
        )

    def list_comments(
        self,
        folder: str,
        version: Optional[str] = None,
        *,
        include_resolved: bool = True,
    ) -> list:
        """Oldest-first comments for *folder* (optionally scoped to version)."""
        return self._store.list_comments(
            folder,
            version,
            include_resolved=include_resolved,
        )

    def resolve_comment(
        self, folder: str, comment_id: str, *, resolved: bool = True
    ) -> Tuple[bool, str]:
        """Flip a comment's ``resolved`` flag."""
        return self._store.resolve_comment(folder, comment_id, resolved=resolved)

    def insert_task(
        self,
        folder: str,
        version: str,
        *,
        assignee: str,
        created_by: str,
        title: str,
        description: str = "",
        due_date: Optional[str] = None,
        comment_id: Optional[str] = None,
    ) -> Optional[dict]:
        """Create a task; return the created row or None."""
        return self._store.insert_task(
            folder,
            version,
            assignee=assignee,
            created_by=created_by,
            title=title,
            description=description,
            due_date=due_date,
            comment_id=comment_id,
        )

    def list_tasks(self, folder: str, version: Optional[str] = None) -> list:
        """Newest-first tasks for *folder* (optionally one *version*)."""
        return self._store.list_tasks(folder, version)

    def list_tasks_for_assignee(self, assignee: str) -> list:
        """All tasks across the registry assigned to *assignee*."""
        return self._store.list_tasks_for_assignee(assignee)

    def update_task_status(
        self, folder: str, task_id: str, status: str
    ) -> Tuple[bool, str]:
        """Set a task's ``status``."""
        return self._store.update_task_status(folder, task_id, status)

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

        Iterates over each domain
        (via :meth:`RegistryStore.list_domain_folders`),
        loads its latest version, and extracts
        bridges from ``ontology.classes[].bridges``.

        Returns ``(ok, domains_with_bridges, error_msg)`` where each entry
        has ``name``, ``base_uri``, and ``bridges`` (list of bridge dicts
        with ``source_class``, ``source_class_uri``, ``source_emoji``,
        ``target_domain``, ``target_class_name``, ``target_class_uri``,
        ``label``).
        """
        ok, names, msg = self._store.list_domain_folders()
        if not ok:
            return False, [], msg

        result: List[Dict[str, Any]] = []
        for name in sorted(names):
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

        Versioned layout (new)::

            domains/{folder}/V1/V1.json
            domains/{folder}/V1/documents/
            domains/{folder}/V2/V2.json
            domains/{folder}/V2/documents/

        Returns ``(ok, message)``.
        """
        base = self.domain_path(folder)

        ok, items, msg = self._uc.list_directory(base)
        if not ok:
            return False, f"Cannot list {base}: {msg}"

        flat_versions: List[str] = []
        has_documents = False

        for item in items:
            name = item["name"]
            if name.startswith("v") and name.endswith(".json"):
                flat_versions.append(name[1:-5])  # "v1.json" -> "1"
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
