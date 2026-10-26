"""Scheduled tasks, run history, and build analytics Settings.

Extracted from :class:`SettingsService` (Fowler Extract Class).
``SettingsService`` keeps one-line delegators for callers.
"""

from __future__ import annotations

import importlib
from typing import Any, Dict, Optional

from back.core.errors import (
    InfrastructureError,
    NotFoundError,
    OntoBricksError,
    ValidationError,
)
from shared.config.settings import Settings
from back.core.logging import get_logger
from back.objects.session import SessionManager
from back.objects.domain.settings.SettingsService import SettingsService

# Package ``__init__`` binds ``SettingsService`` as the class, which shadows
# the submodule. Tests patch that module's globals (``get_domain``).
_ss = importlib.import_module("back.objects.domain.settings.SettingsService")

logger = get_logger(__name__)


class ScheduleSettings:
    """Schedules, scheduler status, build/analytics runs, and cohort-rule picker."""

    @staticmethod
    def human_size(nbytes: int) -> str:
        """Return a human-readable file size string."""
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if abs(nbytes) < 1024:
                return f"{nbytes:.1f} {unit}" if unit != "B" else f"{nbytes} B"
            nbytes /= 1024  # type: ignore[assignment]
        return f"{nbytes:.1f} PB"

    @staticmethod
    def list_schedules_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Every schedule of every task type, plus the type catalogue.

        The catalogue lets the settings UI build its type selector and
        per-type columns from the backend registry instead of hardcoding
        the list a second time.
        """
        from back.objects.registry.schedule import task_type_catalog

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        scheduler = SettingsService._get_scheduler()
        try:
            entries = scheduler.get_all_schedules(host, token, registry_cfg)
            return {
                "success": True,
                "schedules": entries,
                "task_types": task_type_catalog(),
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("list_schedules failed: %s", e)
            raise InfrastructureError("Failed to list schedules", detail=str(e)) from e

    @staticmethod
    def save_schedule_result(
        data: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Create or update a schedule of any task type.

        Per-type options arrive in ``config`` and are validated by the
        task type itself, so this method never branches on the type.
        """
        try:
            task_type = (data.get("task_type") or "build").strip()
            domain_name = (
                data.get("domain_name") or data.get("project_name") or ""
            ).strip()
            target_key = (data.get("target_key") or "").strip()
            interval_minutes = int(data.get("interval_minutes", 60))
            enabled = bool(data.get("enabled", True))
            version = (data.get("version") or "latest").strip()
            config = data.get("config")
            if not isinstance(config, dict):
                config = {}

            if not domain_name:
                raise ValidationError("Domain name is required")

            _, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )

            scheduler = SettingsService._get_scheduler()
            ok, msg = scheduler.save_schedule(
                host,
                token,
                registry_cfg,
                settings,
                task_type,
                domain_name,
                interval_minutes,
                target_key=target_key,
                enabled=enabled,
                version=version,
                config=config,
            )
            if not ok:
                raise ValidationError(msg)
            return {"success": ok, "message": msg}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("save_schedule failed: %s", e)
            raise InfrastructureError("Failed to save schedule", detail=str(e)) from e

    @staticmethod
    def get_schedule_history_result(
        task_type: str,
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        target_key: str = "",
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        scheduler = SettingsService._get_scheduler()
        try:
            entries = scheduler.get_schedule_history(
                host, token, registry_cfg, task_type, domain_name, target_key
            )
            return {
                "success": True,
                "task_type": task_type,
                "domain_name": domain_name,
                "target_key": target_key,
                "history": entries,
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("get_schedule_history failed for '%s': %s", domain_name, e)
            raise InfrastructureError(
                "Failed to load schedule history", detail=str(e)
            ) from e

    @staticmethod
    def get_build_runs_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        version: Optional[str] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """Return the build-run trace for *domain_name* (newest-first)."""
        try:
            domain = _ss.get_domain(session_mgr)
            svc = _ss.RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")
            runs = svc.load_build_runs(domain_name, version=version, limit=limit)
            return {
                "success": True,
                "domain_name": domain_name,
                "version": version,
                "runs": runs,
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("get_build_runs failed for '%s': %s", domain_name, e)
            raise InfrastructureError(
                "Failed to load build runs", detail=str(e)
            ) from e

    @staticmethod
    def _all_runs_result(
        kind: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        folder: Optional[str],
        limit: int,
        offset: int,
    ) -> Dict[str, Any]:
        """One page of registry-wide run history for the admin Runs page.

        *kind* is ``"build"`` or ``"analytics"``. ``folder=None`` spans every
        domain. The two kinds share every step but the registry method, so
        they share one body rather than two near-copies.
        """
        try:
            domain = _ss.get_domain(session_mgr)
            svc = _ss.RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")
            reader = (
                svc.load_all_build_runs
                if kind == "build"
                else svc.load_all_graph_analytics_runs
            )
            runs, total = reader(folder=folder, limit=limit, offset=offset)
            return {
                "success": True,
                "domain": folder,
                "runs": runs,
                "total": total,
                "limit": limit,
                "offset": offset,
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("get_all_%s_runs failed: %s", kind, e)
            raise InfrastructureError(
                f"Failed to load {kind} runs", detail=str(e)
            ) from e

    @staticmethod
    def get_all_build_runs_result(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        folder: Optional[str] = None,
        limit: int = 25,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """One page of build runs across every domain in the registry."""
        return SettingsService._all_runs_result(
            "build", session_mgr, settings, folder=folder, limit=limit, offset=offset
        )

    @staticmethod
    def get_all_analytics_runs_result(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        folder: Optional[str] = None,
        limit: int = 25,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """One page of analytics runs across every domain in the registry."""
        return SettingsService._all_runs_result(
            "analytics",
            session_mgr,
            settings,
            folder=folder,
            limit=limit,
            offset=offset,
        )

    @staticmethod
    def get_build_analytics_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        version: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return aggregate build statistics for *domain_name*."""
        try:
            domain = _ss.get_domain(session_mgr)
            svc = _ss.RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")
            analytics = svc.build_analytics(domain_name, version=version)
            return {
                "success": True,
                "domain_name": domain_name,
                "version": version,
                "analytics": analytics,
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("get_build_analytics failed for '%s': %s", domain_name, e)
            raise InfrastructureError(
                "Failed to load build analytics", detail=str(e)
            ) from e

    @staticmethod
    def scheduler_status_payload() -> Dict[str, Any]:
        scheduler = SettingsService._get_scheduler()
        return {"success": True, **scheduler.status()}

    @staticmethod
    def delete_schedule_result(
        task_type: str,
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        target_key: str = "",
    ) -> Dict[str, Any]:
        try:
            _, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )

            scheduler = SettingsService._get_scheduler()
            ok, msg = scheduler.remove_schedule(
                host, token, registry_cfg, task_type, domain_name, target_key
            )
            if not ok:
                raise NotFoundError(msg)
            return {"success": ok, "message": msg}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("delete_schedule failed: %s", e)
            raise InfrastructureError("Failed to remove schedule", detail=str(e)) from e

    @staticmethod
    def trigger_schedule_now_result(
        task_type: str,
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        target_key: str = "",
    ) -> Dict[str, Any]:
        """Fire a schedule immediately, without touching its own clock."""
        try:
            _, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            scheduler = SettingsService._get_scheduler()
            ok, msg = scheduler.run_schedule_now(
                host, token, registry_cfg, settings, task_type, domain_name, target_key
            )
            if not ok:
                raise InfrastructureError("Failed to trigger schedule", detail=msg)
            return {"success": True, "message": msg}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("trigger_schedule_now failed: %s", e)
            raise InfrastructureError(
                "Failed to trigger schedule", detail=str(e)
            ) from e

    @staticmethod
    def list_cohort_rules_for_domain_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Return ``[{id, label}]`` for the saved cohort rules of *domain_name*.

        Reads the latest version of the domain headlessly (no session
        switch) so the schedule modal can list rules for any domain
        in the registry.
        """
        try:
            _, host, token, _registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            domain_obj = _ss.get_domain(session_mgr)
            svc = _ss.RegistryService.from_context(domain_obj, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            ok, data, version, err = svc.load_latest_domain_data(domain_name)
            if not ok:
                raise NotFoundError(
                    err or f"Domain '{domain_name}' not found in registry"
                )

            doc = data if isinstance(data, dict) else {}

            # Persisted shape (Volume + Lakebase):
            #   { "info": {...},
            #     "versions": { "<v>": { "ontology": { "cohort_rules": [...] }, ... } } }
            # Try the versioned path first, then fall back to the flat
            # legacy shapes for resilience.
            ontology: Dict[str, Any] = {}
            versions = doc.get("versions") or {}
            if isinstance(versions, dict) and versions:
                version_data = versions.get(version) or versions.get(str(version))
                if version_data is None and versions:
                    # Pick the highest version key as a last resort.
                    try:
                        latest_key = max(
                            versions.keys(), key=lambda v: tuple(int(p) for p in str(v).split("."))
                        )
                    except (TypeError, ValueError):
                        latest_key = next(iter(versions))
                    version_data = versions.get(latest_key)
                if isinstance(version_data, dict):
                    ontology = version_data.get("ontology") or {}
            if not ontology:
                ontology = doc.get("ontology") or {}

            rules = (
                ontology.get("cohort_rules")
                or doc.get("cohort_rules")
                or []
            )
            simple = []
            for r in rules:
                rid = r.get("id", "")
                if not rid:
                    continue
                output = r.get("output") or {}
                uc_table = output.get("uc_table") or {}
                simple.append(
                    {
                        "id": rid,
                        "label": r.get("label", "") or rid,
                        "class_uri": r.get("class_uri", ""),
                        "output": {
                            "graph": bool(output.get("graph", True)),
                            "uc_table": (
                                {
                                    "catalog": uc_table.get("catalog", ""),
                                    "schema": uc_table.get("schema", ""),
                                    "table_name": uc_table.get(
                                        "table_name", ""
                                    ),
                                }
                                if uc_table.get("table_name")
                                else None
                            ),
                        },
                    }
                )
            return {"success": True, "rules": simple}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception(
                "list_cohort_rules_for_domain(%s) failed: %s", domain_name, e
            )
            raise InfrastructureError(
                "Failed to list cohort rules", detail=str(e)
            ) from e
