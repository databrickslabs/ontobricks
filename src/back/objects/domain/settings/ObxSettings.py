"""OBX registry export, preview, and import Settings.

Extracted from :class:`SettingsService` (Fowler Extract Class).
``SettingsService`` keeps one-line delegators for callers.
"""

from __future__ import annotations

import copy
import importlib
import json
import re
import time
from typing import Any, Dict, List, Optional

from back.core.errors import InfrastructureError, OntoBricksError, ValidationError
from shared.config.settings import Settings
from back.core.logging import get_logger
from back.objects.session import SessionManager
from back.objects.domain.settings.SettingsService import SettingsService

# Package ``__init__`` binds ``SettingsService`` as the class, which shadows
# the submodule. Tests patch that module's globals (``RegistryService``,
# ``get_domain``, ``obx_format``).
_ss = importlib.import_module("back.objects.domain.settings.SettingsService")

logger = get_logger(__name__)


class ObxSettings:
    """Build, preview, and apply ``.obx`` registry envelopes."""

    # 50 MB cap matches typical Apps upload limits and protects the
    # in-memory JSON parse on the import side.
    OBX_MAX_BYTES = 50 * 1024 * 1024

    @staticmethod
    def _resolve_versions_for_export(
        svc: _ss.RegistryService,
        folder: str,
        mode: str,
        explicit: Optional[List[str]],
    ) -> List[str]:
        """Resolve the list of versions to export for a single domain.

        ``mode`` is one of ``"all" | "active" | "latest" | "selected"``.
        For ``"selected"`` the caller must pass *explicit*; the intersection
        with the actually-present versions is returned (silent drop of
        missing versions).
        """
        available = svc.list_versions_sorted(folder)
        if not available:
            return []
        if mode == "all":
            return available
        if mode == "latest":
            return [available[0]]
        if mode == "active":
            mcp_ver, _ = svc.find_mcp_version(folder)
            return [mcp_ver] if mcp_ver else [available[0]]
        if mode == "selected":
            wanted = [str(v) for v in (explicit or [])]
            return [v for v in available if v in set(wanted)]
        raise ValidationError(
            f"Unknown export mode '{mode}' for domain '{folder}' "
            f"(expected one of: all, active, latest, selected)"
        )

    @staticmethod
    def export_registry_obx_result(
        spec: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
        exported_by: str = "",
    ) -> Dict[str, Any]:
        """Build a `.obx` envelope from the registry for the requested domains.

        ``spec`` shape::

            {
                "domains": [
                    {
                        "name": "claims",
                        "mode": "all" | "active" | "latest" | "selected",
                        "versions": ["1", "2"]   # required when mode == "selected"
                    }
                ]
            }
        """
        try:
            domain_session = _ss.get_domain(session_mgr)
            svc = _ss.RegistryService.from_context(domain_session, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            entries = (spec or {}).get("domains") or []
            if not entries:
                raise ValidationError("No domains selected for export")

            exported_domains: List[Dict[str, Any]] = []
            errors: List[str] = []
            for entry in entries:
                name = (entry.get("name") or "").strip()
                if not name:
                    errors.append("Domain entry without a name was skipped")
                    continue
                mode = entry.get("mode") or "latest"
                explicit = entry.get("versions")

                versions = SettingsService._resolve_versions_for_export(
                    svc, name, mode, explicit
                )
                if not versions:
                    errors.append(f'No versions to export for domain "{name}"')
                    continue

                version_docs: Dict[str, Any] = {}
                latest_info: Dict[str, Any] = {}
                for ver in versions:
                    ok, data, msg = svc.read_version(name, ver)
                    if not ok:
                        errors.append(f'{name} v{ver}: {msg}')
                        continue
                    version_docs[ver] = data
                    if not latest_info:
                        latest_info = data.get("info", {}) or {}

                if not version_docs:
                    continue

                exported_domains.append(
                    {
                        "name": name,
                        "info": latest_info,
                        "versions": version_docs,
                    }
                )

            if not exported_domains:
                raise ValidationError(
                    "Nothing to export (no readable versions for the selected domains)"
                )

            envelope = _ss.obx_format.build_envelope(
                exported_domains, exported_by=exported_by
            )

            today = time.strftime("%Y-%m-%d")
            filename = f"ontobricks-{today}.obx"

            return {
                "success": True,
                "filename": filename,
                "envelope": envelope,
                "domain_count": len(exported_domains),
                "version_count": sum(
                    len(d["versions"]) for d in exported_domains
                ),
                "warnings": errors,
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("OBX export failed: %s", e)
            raise InfrastructureError("OBX export failed", detail=str(e)) from e

    @staticmethod
    def _decode_obx_payload(file_bytes: bytes) -> Dict[str, Any]:
        """Parse + validate the envelope bytes, returning the upgraded envelope."""
        if not file_bytes:
            raise ValidationError("Empty .obx file")
        if len(file_bytes) > ObxSettings.OBX_MAX_BYTES:
            raise ValidationError(
                f".obx file too large ({len(file_bytes)} bytes); "
                f"max {ObxSettings.OBX_MAX_BYTES} bytes"
            )
        try:
            envelope = json.loads(file_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValidationError(
                f"Invalid .obx file: not valid JSON ({exc})"
            ) from exc
        return _ss.obx_format.load(envelope)

    @staticmethod
    def _camelcase_import_name(value: str) -> str:
        """Return a valid CamelCase display name for an imported domain."""
        parts = re.findall(r"[A-Za-z0-9]+", value or "")
        candidate = "".join(part[:1].upper() + part[1:] for part in parts)
        if candidate and candidate[0].isalpha():
            return candidate[:64]
        return "ImportedDomain"

    @staticmethod
    def _suggest_import_name(svc: _ss.RegistryService, display_name: str) -> str:
        """Suggest a free CamelCase display name for an imported copy."""
        base = SettingsService._camelcase_import_name(display_name) + "Imported"
        candidate = base[:64]
        index = 2
        while svc.domain_exists(_ss.sanitize_domain_folder(candidate)):
            suffix = str(index)
            candidate = base[: 64 - len(suffix)] + suffix
            index += 1
        return candidate

    @staticmethod
    def _prepare_renamed_version_doc(
        doc: Dict[str, Any],
        version: str,
        display_name: str,
        base_uri: str,
    ) -> Dict[str, Any]:
        """Rewrite identity and clear graph runtime in a renamed OBX version."""
        prepared = copy.deepcopy(doc)

        def rewrite(node: Dict[str, Any], *, root: bool = False) -> None:
            info = node.get("info")
            if root or isinstance(info, dict):
                info = node.setdefault("info", {})
                info["name"] = display_name
                info["last_build"] = ""

            ontology = node.get("ontology")
            if isinstance(ontology, dict):
                ontology["base_uri"] = base_uri
                ontology["base_uri_auto"] = True

            if "last_build" in node:
                node["last_build"] = ""
            triplestore = node.get("triplestore")
            if isinstance(triplestore, dict):
                triplestore["stats"] = {}

        rewrite(prepared, root=True)
        versions = prepared.get("versions")
        nested = versions.get(version) if isinstance(versions, dict) else None
        if isinstance(nested, dict):
            rewrite(nested)
        return prepared

    @staticmethod
    def preview_obx_import_result(
        file_bytes: bytes,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Parse an uploaded `.obx` file and report per-domain conflict status."""
        try:
            domain_session = _ss.get_domain(session_mgr)
            svc = _ss.RegistryService.from_context(domain_session, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            envelope = SettingsService._decode_obx_payload(file_bytes)

            domains_preview: List[Dict[str, Any]] = []
            for entry in envelope.get("domains", []):
                raw_name = (entry.get("name") or "").strip()
                if not raw_name:
                    continue
                folder = _ss.sanitize_domain_folder(raw_name)
                info = entry.get("info") or {}
                display_name = SettingsService._camelcase_import_name(
                    info.get("name") or raw_name
                )
                incoming_versions = sorted(
                    (entry.get("versions") or {}).keys(),
                    key=lambda v: [int(x) for x in v.split(".") if x.isdigit()] or [0],
                    reverse=True,
                )

                exists = svc.domain_exists(folder)
                conflicting_versions: List[str] = []
                if exists:
                    existing = set(svc.list_versions_sorted(folder))
                    conflicting_versions = [
                        v for v in incoming_versions if v in existing
                    ]

                domains_preview.append(
                    {
                        "name": folder,
                        "original_name": raw_name,
                        "incoming_versions": incoming_versions,
                        "exists": exists,
                        "conflicting_versions": conflicting_versions,
                        "suggested_new_name": (
                            SettingsService._suggest_import_name(svc, display_name)
                            if exists
                            else display_name
                        ),
                        "display_name": display_name,
                        "info": info,
                    }
                )

            return {
                "success": True,
                "format_version": envelope.get("format_version"),
                "ontobricks_version": envelope.get("ontobricks_version", ""),
                "exported_at": envelope.get("exported_at", ""),
                "exported_by": envelope.get("exported_by", ""),
                "domains": domains_preview,
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("OBX import preview failed: %s", e)
            raise InfrastructureError(
                "Failed to read .obx file", detail=str(e)
            ) from e

    @staticmethod
    def import_registry_obx_result(
        file_bytes: bytes,
        decisions: List[Dict[str, Any]],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Apply per-domain decisions and write the contents of *file_bytes*
        into the registry.

        Each decision: ``{"name": <folder>, "action": "skip"|"overwrite"|"rename",
        "new_name": <str>}``. Missing entries default to ``"skip"`` so callers
        can't accidentally overwrite a domain they didn't review.
        """
        try:
            domain_session = _ss.get_domain(session_mgr)
            svc = _ss.RegistryService.from_context(domain_session, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            envelope = SettingsService._decode_obx_payload(file_bytes)

            decision_map: Dict[str, Dict[str, Any]] = {}
            for d in decisions or []:
                key = (d.get("name") or "").strip()
                if key:
                    decision_map[key] = d

            summary = {
                "imported_versions": 0,
                "skipped_domains": 0,
                "renamed_domains": 0,
                "overwritten_versions": 0,
                "errors": [],
                "domains": [],
            }

            for entry in envelope.get("domains", []):
                raw_name = (entry.get("name") or "").strip()
                if not raw_name:
                    summary["errors"].append("Domain entry without a name was skipped")
                    continue

                folder = _ss.sanitize_domain_folder(raw_name)
                decision = decision_map.get(folder) or decision_map.get(raw_name) or {}
                action = (decision.get("action") or "skip").lower()

                if action == "skip":
                    summary["skipped_domains"] += 1
                    summary["domains"].append({"name": folder, "action": "skipped"})
                    continue

                target_folder = folder
                rename_display_name = ""
                rename_base_uri = ""
                if action == "rename":
                    rename_display_name = (
                        decision.get("new_name") or ""
                    ).strip()
                    if not re.fullmatch(
                        r"[A-Z][A-Za-z0-9]{0,63}", rename_display_name
                    ):
                        raise ValidationError(
                            "Imported domain name must be CamelCase alphanumeric"
                        )
                    target_folder = _ss.sanitize_domain_folder(rename_display_name)
                    if svc.domain_exists(target_folder):
                        summary["errors"].append(
                            f'Rename target "{target_folder}" already exists; '
                            f'"{folder}" was skipped'
                        )
                        summary["skipped_domains"] += 1
                        summary["domains"].append(
                            {"name": folder, "action": "skipped_rename_conflict"}
                        )
                        continue
                    rename_base_uri = _ss.build_auto_base_uri(
                        rename_display_name,
                        _ss.resolve_default_base_uri(domain_session, settings),
                    )
                    summary["renamed_domains"] += 1
                elif action != "overwrite":
                    raise ValidationError(
                        f"Unknown import action '{action}' for domain '{folder}'"
                    )

                existing = (
                    set(svc.list_versions_sorted(target_folder))
                    if svc.domain_exists(target_folder)
                    else set()
                )
                versions = entry.get("versions") or {}
                wrote = 0
                overwrote = 0
                for ver, doc in versions.items():
                    if not isinstance(doc, dict):
                        summary["errors"].append(
                            f"{target_folder} v{ver}: payload is not an object, skipped"
                        )
                        continue
                    is_overwrite = ver in existing
                    document_to_write = (
                        SettingsService._prepare_renamed_version_doc(
                            doc,
                            ver,
                            rename_display_name,
                            rename_base_uri,
                        )
                        if action == "rename"
                        else doc
                    )
                    ok, msg = svc.write_version(
                        target_folder, ver, json.dumps(document_to_write)
                    )
                    if not ok:
                        summary["errors"].append(
                            f"{target_folder} v{ver}: {msg}"
                        )
                        continue
                    wrote += 1
                    if is_overwrite:
                        overwrote += 1

                summary["imported_versions"] += wrote
                summary["overwritten_versions"] += overwrote
                summary["domains"].append(
                    {
                        "name": target_folder,
                        "original_name": folder,
                        "action": action,
                        "versions_written": wrote,
                        "versions_overwritten": overwrote,
                    }
                )

            _ss.invalidate_registry_cache()

            return {
                "success": True,
                "message": (
                    f"Imported {summary['imported_versions']} version(s) "
                    f"across {len(summary['domains'])} domain(s)"
                ),
                **summary,
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("OBX import failed: %s", e)
            raise InfrastructureError("OBX import failed", detail=str(e)) from e
