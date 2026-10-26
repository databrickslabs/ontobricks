"""Registry/session resolve, snapshots, and small DigitalTwin helpers.

Extracted from :class:`DigitalTwin` (Fowler Extract Class).
``DigitalTwin`` keeps one-line delegators for callers.
"""

from __future__ import annotations

import importlib
import time

from back.core.errors import InfrastructureError, NotFoundError, ValidationError
from back.core.helpers import extract_local_name
from back.core.logging import get_logger

logger = get_logger(__name__)


def _dt():
    from back.objects.digitaltwin.DigitalTwin import DigitalTwin

    return DigitalTwin


def _dt_mod():
    return importlib.import_module("back.objects.digitaltwin.DigitalTwin")


class TwinResolve:
    """Resolve registry location and named domains; small URI/range helpers."""

    def __init__(self, domain) -> None:
        self._domain = domain

    # ------------------------------------------------------------------
    # Backend label (instance method)
    # ------------------------------------------------------------------

    def effective_backend_label(self) -> str:
        """Derive a human-readable backend label from the domain configuration."""
        domain = self._domain
        ts = getattr(domain, "triplestore", None) or {}
        backend = ts.get("backend", "")
        if backend:
            return backend
        delta = getattr(domain, "delta", None) or {}
        if delta.get("catalog"):
            return "Delta (SQL Warehouse)"
        return "Lakebase"

    # ------------------------------------------------------------------
    # Registry / domain resolution (static -- API helpers)
    # ------------------------------------------------------------------

    @staticmethod
    def resolve_registry(
        session_mgr,
        settings,
        registry_catalog=None,
        registry_schema=None,
        registry_volume=None,
    ):
        """Resolve registry location: explicit query params -> session -> env.

        Always carries ``lakebase_schema`` / ``lakebase_database`` from the
        session/env so callers that pass the dict straight to
        ``RegistryCfg.from_dict`` get the correct Lakebase schema rather than
        the hardcoded ``"ontobricks_registry"`` default.
        """
        from back.objects.registry import RegistryCfg

        base = RegistryCfg.from_session(session_mgr, settings)
        return {
            "catalog": registry_catalog or base.catalog,
            "schema": registry_schema or base.schema,
            "volume": registry_volume or base.volume,
            "lakebase_schema": base.lakebase_schema,
            "lakebase_database": base.lakebase_database,
        }

    @staticmethod
    def resolve_domain(
        domain_name,
        session_mgr,
        settings,
        registry_catalog=None,
        registry_schema=None,
        registry_volume=None,
        domain_version=None,
        *,
        read_only=False,
    ):
        """Return the session to operate on; optionally load from registry by name/version.

        ``read_only`` (default ``False``) tunes the resolve for read-only
        query paths (status / stats / triples-find / GraphQL reads):

        * the PUBLISHED document is served from a TTL cache
          (:meth:`RegistryService.load_published_domain_data_cached`),
          skipping the newest→oldest version scan;
        * OWL/R2RML are **not** regenerated (read paths never consume
          generated content); and
        * the session is **not** persisted (``save()`` skipped).

        Write/generation paths (build, ontology/R2RML export,
        design-status) must keep the default ``read_only=False``.
        """
        from back.objects.registry import RegistryCfg, RegistryService

        domain = _dt_mod().get_domain(session_mgr)
        if not domain_name:
            return domain

        t0 = time.perf_counter()
        reg = _dt().resolve_registry(
            session_mgr, settings, registry_catalog, registry_schema, registry_volume
        )
        cfg = RegistryCfg.from_dict(reg)
        if not cfg.is_configured:
            raise ValidationError(
                "Registry not configured — cannot resolve domain_name"
            )
        svc = RegistryService(cfg, _dt().uc_from_domain(domain, settings))
        if domain_version:
            ok, data, msg = svc.read_version(domain_name, domain_version)
            if not ok:
                if "not found" in msg.lower():
                    raise NotFoundError(msg)
                raise InfrastructureError(msg)
            if data.get("info", {}).get("status") != "PUBLISHED":
                raise ValidationError(
                    f"Version {domain_version} of domain '{domain_name}' is not "
                    f"PUBLISHED; the API only serves PUBLISHED versions"
                )
            version = domain_version
        elif read_only:
            ok, data, version, err = svc.load_published_domain_data_cached(domain_name)
            if not ok:
                raise NotFoundError(err)
        else:
            ok, data, version, err = svc.load_published_domain_data(domain_name)
            if not ok:
                raise NotFoundError(err)
        t_registry = time.perf_counter()

        domain.clear_generated_content()
        domain.import_from_file(data, version=version)
        domain.domain_folder = domain_name
        t_import = time.perf_counter()

        t_gen = t_import
        if not read_only:
            domain.ensure_generated_content()
            t_gen = time.perf_counter()
            domain.save()
        t_end = time.perf_counter()

        logger.info(
            "DigitalTwin: loaded domain '%s' version %s from registry "
            "[read_only=%s registry=%.0fms import=%.0fms gen=%.0fms save=%.0fms total=%.0fms]",
            domain_name,
            version,
            read_only,
            (t_registry - t0) * 1000,
            (t_import - t_registry) * 1000,
            (t_gen - t_import) * 1000,
            (t_end - t_gen) * 1000,
            (t_end - t0) * 1000,
        )
        return domain

    @staticmethod
    def uc_from_domain(domain, settings):
        """Build a VolumeFileService from domain session credentials."""
        from back.core.databricks import VolumeFileService
        from back.core.helpers import get_databricks_host_and_token

        host, token = get_databricks_host_and_token(domain, settings)
        return VolumeFileService(host=host, token=token)

    # ------------------------------------------------------------------
    # Misc utilities (static)
    # ------------------------------------------------------------------

    @staticmethod
    def is_datatype_range(range_val: str) -> bool:
        """Return True if a property range looks like a datatype (not an object property)."""
        low = range_val.lower()
        return any(
            kw in low
            for kw in (
                "xsd:",
                "string",
                "integer",
                "decimal",
                "date",
                "boolean",
                "float",
                "double",
                "time",
                "long",
                "int",
                "short",
                "byte",
            )
        )

    @staticmethod
    def make_snapshot(domain):
        """Create a lightweight snapshot of domain session state for background threads."""
        from back.objects.digitaltwin.models import DomainSnapshot

        return DomainSnapshot(domain)

    @staticmethod
    def extract_local_id(uri: str) -> str:
        """Extract the local entity identifier from a URI.

        Entity subjects are minted by R2RML as ``{base_uri}{Class}/{id}``, and
        ``base_uri`` normally ends in ``#``. The fragment is therefore
        ``Class/id``, not the bare id, so the class segment is stripped here.
        Class URIs such as ``{base}#Customer`` have no slash and are returned
        unchanged.
        """
        local = extract_local_name(uri)
        if "/" in local:
            local = local.rsplit("/", 1)[-1]
        return local or uri

    @staticmethod
    def is_owlrl_available() -> bool:
        """Check whether the ``owlrl`` reasoning library is importable."""
        try:
            import owlrl as _owlrl  # noqa: F401

            return True
        except ImportError:
            return False

