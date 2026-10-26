"""Registry location (catalog, schema, volume, backend) resolved from session, domain, or volume path."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from back.core.logging import get_logger

logger = get_logger(__name__)

_DEFAULT_VOLUME = "OntoBricksRegistry"


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
           schema, volume. When those are empty, the session
           ``domain.settings["registry"]`` catalog/schema/volume is used.
           A volume equal to the seeded default ``OntoBricksRegistry``
           (DomainSession and ``Settings.registry_volume``) is treated
           as unset so ``REGISTRY_VOLUME`` can override it; a custom
           session volume still wins when env is that default.

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
            catalog=cls._first_nonempty(
                getattr(settings, "registry_catalog", ""),
                reg.get("catalog"),
            ),
            schema=cls._first_nonempty(
                getattr(settings, "registry_schema", ""),
                reg.get("schema"),
            ),
            volume=cls._first_named_volume(
                getattr(settings, "registry_volume", ""),
                reg.get("volume"),
            ),
            lakebase_schema=lb_schema,
            lakebase_database=lb_database,
        )

    @staticmethod
    def _first_nonempty(*values: object) -> str:
        for raw in values:
            text = str(raw or "").strip()
            if text:
                return text
        return ""

    @staticmethod
    def _first_named_volume(*values: object) -> str:
        """Last-resort volume: skip the seeded ``OntoBricksRegistry`` sentinel."""
        for raw in values:
            text = str(raw or "").strip()
            if text and text != _DEFAULT_VOLUME:
                return text
        return _DEFAULT_VOLUME

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
