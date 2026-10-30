"""Registry domain listing, version lifecycle, and per-domain role.

Extracted from :class:`SettingsService` (Fowler Extract Class).
``SettingsService`` keeps one-line delegators for callers.
"""

from __future__ import annotations

import copy
import importlib
from contextlib import nullcontext
from typing import Any, Dict

from back.core.databricks.request_identity import get_request_identity
from back.core.errors import InfrastructureError, NotFoundError, OntoBricksError, ValidationError
from shared.config.settings import Settings
from back.core.logging import get_logger
from back.objects.registry.gitsync.GitHubConfig import GitHubConfig
from back.objects.registry.gitsync.GitHubMirror import github_mirror
from back.objects.session import SessionManager

# Package ``__init__`` binds ``SettingsService`` as the class, which shadows
# the submodule. Tests patch that module's globals (``get_domain``,
# ``RegistryService``, cache helpers).
_ss = importlib.import_module("back.objects.domain.settings.SettingsService")

logger = get_logger(__name__)


class RegistryDomainSettings:
    """Settings → Registry: domains, versions, bridges, and target-domain role."""

    @staticmethod
    def _github_enabled(svc) -> bool:
        return GitHubConfig.ready_for(svc.store)

    @staticmethod
    def _delete_github_mirror(
        folder: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        delete_github: bool,
    ) -> None:
        if not delete_github:
            return
        from back.objects.domain.settings.GitHubSettings import GitHubSettings

        GitHubSettings.delete_mirrored_domain(
            folder,
            session_mgr,
            settings,
            actor=get_request_identity().email,
        )

    @staticmethod
    def list_registry_domains_result(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        user_role: str = "",
    ) -> Dict[str, Any]:
        try:
            domain = _ss.get_domain(session_mgr)
            svc = _ss.RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            ok, result, msg = svc.list_domain_details_cached()
            if not ok:
                raise InfrastructureError("Failed to list registry domains", detail=msg)
            result = copy.deepcopy(result)
            loaded_folder = str(domain.domain_folder or "")
            loaded_version = str(domain.current_version or "")
            for item in result:
                versions = item.get("versions", []) or []
                latest = str(versions[0].get("version", "")) if versions else ""
                for version_data in versions:
                    version = str(version_data.get("version", ""))
                    deletion = _ss.version_deletion_capability(
                        user_role=user_role,
                        status=version_data.get("status", "DRAFT"),
                        is_loaded=(
                            loaded_folder == item.get("name")
                            and loaded_version == version
                        ),
                        is_latest=version == latest,
                        version_count=len(versions),
                    )
                    version_data.update(deletion)
            return {
                "success": True,
                "domains": result,
                "github_enabled": RegistryDomainSettings._github_enabled(svc),
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("List registry domains failed: %s", e)
            raise InfrastructureError(
                "Failed to list registry domains", detail=str(e)
            ) from e

    @staticmethod
    def list_registry_bridges_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Return all bridges across every domain in the registry."""
        try:
            domain = _ss.get_domain(session_mgr)
            svc = _ss.RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            ok, result, msg = svc.list_all_bridges()
            if not ok:
                raise InfrastructureError("Failed to list registry bridges", detail=msg)
            return {"success": True, "domains": result}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("List registry bridges failed: %s", e)
            raise InfrastructureError(
                "Failed to list registry bridges", detail=str(e)
            ) from e

    @staticmethod
    def delete_registry_domain_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        delete_github: bool = False,
    ) -> Dict[str, Any]:
        try:
            domain = _ss.get_domain(session_mgr)
            svc = _ss.RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            with github_mirror.paused():
                errors = svc.delete_domain(domain_name)

            if errors:
                joined = "; ".join(errors)
                raise InfrastructureError(
                    "Registry domain was only partially deleted",
                    detail=joined,
                )
            RegistryDomainSettings._delete_github_mirror(
                domain_name,
                session_mgr,
                settings,
                delete_github=delete_github,
            )

            return {
                "success": True,
                "message": f'Domain "{domain_name}" deleted from registry',
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Delete registry domain failed: %s", e)
            raise InfrastructureError(
                "Delete registry domain failed", detail=str(e)
            ) from e

    @staticmethod
    def delete_registry_version_result(
        domain_name: str,
        version: str,
        *,
        user_role: str,
        session_mgr: SessionManager,
        settings: Settings,
        delete_github: bool = False,
    ) -> Dict[str, Any]:
        try:
            domain = _ss.get_domain(session_mgr)
            svc = _ss.RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            listed, versions, list_message = svc.list_versions(domain_name)
            if not listed:
                raise InfrastructureError(
                    "Failed to list registry versions", detail=list_message
                )
            versions = sorted(
                versions,
                key=_ss.RegistryService._version_sort_key,
                reverse=True,
            )
            if version not in versions:
                raise NotFoundError(
                    f'Version {version} not found in "{domain_name}"'
                )
            ok, data, message = svc.read_version(domain_name, version)
            if not ok:
                raise InfrastructureError(
                    "Failed to read registry version", detail=message
                )
            status = (data.get("info", {}).get("status") or "DRAFT").upper()
            _ss.check_version_deletion(
                user_role=user_role,
                status=status,
                is_loaded=(
                    domain.domain_folder == domain_name
                    and domain.current_version == version
                ),
                version_count=len(versions),
            )

            last_version = len(versions) == 1
            pause = github_mirror.paused() if last_version else nullcontext()
            try:
                with pause:
                    deleted, delete_message = svc.delete_version(domain_name, version)
                    domain_deleted = False
                    if last_version:
                        domain_errors = svc.delete_domain(domain_name)
                        if domain_errors:
                            raise InfrastructureError(
                                "Version was deleted but the empty domain could not be removed",
                                detail="; ".join(domain_errors),
                            )
                        domain_deleted = True
            except InfrastructureError:
                # The registry row may already be gone when post-delete
                # Knowledge Store cleanup fails. Do not leave version-status
                # caches claiming that the deleted row still exists.
                _ss.clear_version_status_cache()
                raise
            if not deleted:
                raise InfrastructureError(
                    "Failed to delete registry version", detail=delete_message
                )
            if domain_deleted:
                RegistryDomainSettings._delete_github_mirror(
                    domain_name,
                    session_mgr,
                    settings,
                    delete_github=delete_github,
                )
            _ss.clear_version_status_cache()
            if domain_deleted:
                message = (
                    f'Version {version} was the last version; domain '
                    f'"{domain_name}" deleted'
                )
            else:
                message = f'Version {version} deleted from "{domain_name}"'
            return {
                "success": True,
                "message": message,
                "domain_deleted": domain_deleted,
            }
        except OntoBricksError:
            raise
        except Exception as exc:
            logger.exception("Delete registry version failed: %s", exc)
            raise InfrastructureError(
                "Delete registry version failed", detail=str(exc)
            ) from exc

    @staticmethod
    def resolve_domain_role(
        request,
        domain_folder: str,
        settings: Settings,
        *,
        app_role: str = "",
    ) -> str:
        """Resolve the caller's effective role on *domain_folder*.

        Unlike the session-scoped role on ``request.state.user_domain_role``
        (which is for the *loaded* domain), this resolves the role for an
        arbitrary target domain — needed when a Builder manages version
        status from Registry Browse for a domain they have not loaded.
        """
        try:
            from back.core.helpers import get_databricks_host_and_token

            email = getattr(request.state, "user_email", "") or request.headers.get(
                "x-forwarded-email", ""
            )
            domain = _ss.get_domain(_ss.SessionManager(request))
            host, token = get_databricks_host_and_token(domain, settings)
            user_token = (
                getattr(request.state, "user_token", "")
                or request.headers.get("x-forwarded-access-token", "")
            )
            registry_cfg = _ss.RegistryCfg.from_domain(domain, settings).as_dict()
            return _ss.permission_service.get_domain_role(
                email,
                host,
                token,
                registry_cfg,
                settings.ontobricks_app_name,
                domain_folder,
                user_token=user_token,
                app_role=app_role,
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "resolve_domain_role(%s) failed: %s", domain_folder, exc
            )
            return ""

    @staticmethod
    def set_registry_version_status_result(
        domain_name: str,
        version: str,
        new_status: str,
        *,
        user_role: str,
        user_domain_role: str,
        actor_email: str = "",
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Transition a version's lifecycle ``status``.

        Works on any domain in the registry — the domain does not need to
        be loaded in the current session. Enforces the lifecycle state
        machine (allowed transitions), per-transition role requirements,
        and the DRAFT→IN-REVIEW precondition (the version must have been
        built at least once, i.e. ``last_build`` is set).

        The change is recorded in the ``domain_review_events`` audit log
        (attributed to ``actor_email``) so direct lifecycle transitions are
        tracked alongside the review-workflow ones.
        """
        try:
            new_status = (new_status or "").strip().upper()
            domain = _ss.get_domain(session_mgr)
            svc = _ss.RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            sorted_versions = svc.list_versions_sorted(domain_name)
            if version not in sorted_versions:
                raise NotFoundError(f'Version {version} not found in "{domain_name}"')

            ok, data, msg = svc.read_version(domain_name, version)
            if not ok:
                raise InfrastructureError("Failed to read registry version", detail=msg)

            info = data.get("info", {})
            current_status = (info.get("status") or "DRAFT").upper()
            last_build = info.get("last_build", "") or ""
            has_ontology = _ss.RegistryService.version_document_has_ontology(
                data, version
            )

            _ss.check_status_transition(
                current_status,
                new_status,
                user_role=user_role,
                user_domain_role=user_domain_role,
                last_build=last_build,
                has_ontology=has_ontology,
            )

            ok, set_msg = svc.set_version_status(domain_name, version, new_status)
            if not ok:
                raise InfrastructureError(
                    "Failed to update version status", detail=set_msg
                )

            # Attribute the change in the audit log. Best-effort: never let a
            # failed audit write roll back the transition itself.
            try:
                action = {
                    _ss.STATUS_IN_REVIEW: "submitted",
                    _ss.STATUS_PUBLISHED: "published",
                    _ss.STATUS_DRAFT: "reopened",
                }.get(new_status, "commented")
                svc.record_review_event(
                    domain_name,
                    version,
                    actor_email or "",
                    action,
                    from_status=current_status,
                    to_status=new_status,
                    comment="",
                    meta={"source": "lifecycle"},
                )
            except Exception as audit_exc:  # noqa: BLE001
                logger.warning(
                    "audit write skipped for %s/%s status change: %s",
                    domain_name,
                    version,
                    audit_exc,
                )

            _ss.invalidate_registry_cache()
            _ss.clear_version_status_cache()

            if (
                domain.domain_folder == domain_name
                and domain.current_version == version
            ):
                domain.info["status"] = new_status
                domain.save()

            return {
                "success": True,
                "version": version,
                "status": new_status,
                "previous_status": current_status,
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Set registry version status failed: %s", e)
            raise InfrastructureError(
                "Set registry version status failed", detail=str(e)
            ) from e
