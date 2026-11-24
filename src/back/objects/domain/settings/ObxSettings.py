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
            f"Unknown export mode '{mode}' for context '{folder}' "
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
                raise ValidationError("No contexts selected for export")

            exported_domains: List[Dict[str, Any]] = []
            errors: List[str] = []
            for entry in entries:
                name = (entry.get("name") or "").strip()
                if not name:
                    errors.append("Context entry without a name was skipped")
                    continue
                mode = entry.get("mode") or "latest"
                explicit = entry.get("versions")

                versions = SettingsService._resolve_versions_for_export(
                    svc, name, mode, explicit
                )
                if not versions:
                    errors.append(f'No versions to export for context "{name}"')
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
                    "Nothing to export (no readable versions for the selected contexts)"
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
    def _normalize_base_prefix(uri: str) -> str:
        return (uri or "").rstrip("/#")

    @staticmethod
    def _rewrite_uri_prefix(value: str, old_prefix: str, new_prefix: str) -> str:
        if not value or not old_prefix:
            return value
        old = ObxSettings._normalize_base_prefix(old_prefix)
        new = ObxSettings._normalize_base_prefix(new_prefix)
        if not old or not value.startswith(old):
            return value
        rest = value[len(old) :]
        if rest and rest[0] not in "/#":
            return value
        sep = (
            "#"
            if (new_prefix or "").endswith("#")
            else ("/" if (new_prefix or "").endswith("/") else "#")
        )
        if rest.startswith("#") or rest.startswith("/"):
            rest = rest[1:]
        return f"{new}{sep}{rest}" if rest else f"{new}{sep}"

    @staticmethod
    def _rewrite_uri_strings(node: Any, old_prefix: str, new_prefix: str) -> Any:
        if isinstance(node, dict):
            return {
                k: ObxSettings._rewrite_uri_strings(v, old_prefix, new_prefix)
                for k, v in node.items()
            }
        if isinstance(node, list):
            return [
                ObxSettings._rewrite_uri_strings(v, old_prefix, new_prefix)
                for v in node
            ]
        if isinstance(node, str):
            return ObxSettings._rewrite_uri_prefix(node, old_prefix, new_prefix)
        return node

    @staticmethod
    def _prepare_renamed_version_doc(
        doc: Dict[str, Any],
        version: str,
        display_name: str,
        base_uri: str,
    ) -> Dict[str, Any]:
        """Rewrite identity, URIs, and clear graph runtime for a renamed OBX version."""
        prepared = copy.deepcopy(doc)
        old_base = ""
        ont = prepared.get("ontology")
        if isinstance(ont, dict):
            old_base = ont.get("base_uri") or ""
        if not old_base:
            nested0 = (prepared.get("versions") or {}).get(version) or {}
            if isinstance(nested0, dict):
                n_ont = nested0.get("ontology") or {}
                if isinstance(n_ont, dict):
                    old_base = n_ont.get("base_uri") or ""

        def rewrite_identity(node: Dict[str, Any], *, root: bool = False) -> None:
            info = node.get("info")
            if root or isinstance(info, dict):
                info = node.setdefault("info", {})
                info["name"] = display_name
                info["last_build"] = ""
            ontology = node.get("ontology")
            if isinstance(ontology, dict):
                ontology["base_uri"] = base_uri
                ontology["base_uri_auto"] = True
                ontology["name"] = display_name
            if "last_build" in node:
                node["last_build"] = ""
            triplestore = node.get("triplestore")
            if isinstance(triplestore, dict):
                triplestore["stats"] = {}

        rewrite_identity(prepared, root=True)
        versions = prepared.get("versions")
        nested = versions.get(version) if isinstance(versions, dict) else None
        if isinstance(nested, dict):
            rewrite_identity(nested)

        if old_base and ObxSettings._normalize_base_prefix(
            old_base
        ) != ObxSettings._normalize_base_prefix(base_uri):
            rewritten = ObxSettings._rewrite_uri_strings(
                prepared, old_base, base_uri
            )
            prepared.clear()
            prepared.update(rewritten)
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
    def _mark_highest_new_as_draft(
        prepared_by_ver: Dict[str, Dict[str, Any]],
        new_versions: List[str],
    ) -> None:
        if not new_versions:
            return
        from back.objects.registry.RegistryService import RegistryService

        highest = max(new_versions, key=RegistryService._version_sort_key)
        doc = prepared_by_ver[highest]
        doc.setdefault("info", {})["status"] = "DRAFT"
        nested = (doc.get("versions") or {}).get(highest)
        if isinstance(nested, dict):
            nested.setdefault("info", {})["status"] = "DRAFT"

    @staticmethod
    def _prepare_docs_for_import(
        versions: Dict[str, Any],
        *,
        action: str,
        existing: set,
        rename_display_name: str,
        rename_base_uri: str,
        target_folder: str,
        errors: List[str],
    ) -> tuple[Dict[str, Dict[str, Any]], List[str], List[str]]:
        """Return ``(prepared_by_ver, to_write, skipped)``; never touches existing rows."""
        prepared: Dict[str, Dict[str, Any]] = {}
        to_write: List[str] = []
        skipped: List[str] = []
        for ver, doc in versions.items():
            if not isinstance(doc, dict):
                errors.append(
                    f"{target_folder} v{ver}: payload is not an object, skipped"
                )
                continue
            if ver in existing:
                skipped.append(ver)
                continue
            body = (
                ObxSettings._prepare_renamed_version_doc(
                    doc, ver, rename_display_name, rename_base_uri
                )
                if action == "rename"
                else copy.deepcopy(doc)
            )
            prepared[ver] = body
            to_write.append(ver)
        ObxSettings._mark_highest_new_as_draft(prepared, to_write)
        return prepared, to_write, skipped

    @staticmethod
    def _resolve_import_target(
        svc: Any,
        domain_session: Any,
        settings: Settings,
        folder: str,
        decision: Dict[str, Any],
        action: str,
        summary: Dict[str, Any],
    ) -> Optional[tuple[str, str, str, str]]:
        """Return ``(target_folder, action_label, rename_name, rename_uri)`` or None to skip."""
        if action == "rename":
            rename_display_name = (decision.get("new_name") or "").strip()
            if not re.fullmatch(r"[A-Z][A-Za-z0-9]{0,63}", rename_display_name):
                raise ValidationError(
                    "Imported context name must be CamelCase alphanumeric"
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
                return None
            rename_base_uri = _ss.build_auto_base_uri(
                rename_display_name,
                _ss.resolve_default_base_uri(domain_session, settings),
            )
            summary["renamed_domains"] += 1
            return target_folder, "rename", rename_display_name, rename_base_uri
        if action in ("overwrite", "add_missing"):
            return folder, "add_missing", "", ""
        raise ValidationError(
            f"Unknown import action '{action}' for context '{folder}'"
        )

    @staticmethod
    def _import_one_domain_entry(
        entry: Dict[str, Any],
        decision_map: Dict[str, Dict[str, Any]],
        svc: Any,
        domain_session: Any,
        settings: Settings,
        summary: Dict[str, Any],
    ) -> None:
        raw_name = (entry.get("name") or "").strip()
        if not raw_name:
            summary["errors"].append("Context entry without a name was skipped")
            return
        folder = _ss.sanitize_domain_folder(raw_name)
        decision = decision_map.get(folder) or decision_map.get(raw_name) or {}
        action = (decision.get("action") or "skip").lower()
        if action == "skip":
            summary["skipped_domains"] += 1
            summary["domains"].append({"name": folder, "action": "skipped"})
            return
        resolved = ObxSettings._resolve_import_target(
            svc, domain_session, settings, folder, decision, action, summary
        )
        if resolved is None:
            return
        target_folder, action_label, rename_name, rename_uri = resolved
        existing = (
            set(svc.list_versions_sorted(target_folder))
            if svc.domain_exists(target_folder)
            else set()
        )
        prepared, to_write, skipped = ObxSettings._prepare_docs_for_import(
            entry.get("versions") or {},
            action="rename" if action == "rename" else "add_missing",
            existing=existing,
            rename_display_name=rename_name,
            rename_base_uri=rename_uri,
            target_folder=target_folder,
            errors=summary["errors"],
        )
        wrote = 0
        for ver in to_write:
            ok, msg = svc.write_version(
                target_folder, ver, json.dumps(prepared[ver])
            )
            if not ok:
                summary["errors"].append(f"{target_folder} v{ver}: {msg}")
                continue
            wrote += 1
        summary["imported_versions"] += wrote
        summary["versions_skipped"] += len(skipped)
        summary["domains"].append(
            {
                "name": target_folder,
                "original_name": folder,
                "action": action_label,
                "versions_written": wrote,
                "skipped_versions": skipped,
            }
        )

    @staticmethod
    def import_registry_obx_result(
        file_bytes: bytes,
        decisions: List[Dict[str, Any]],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Apply per-domain decisions and write *file_bytes* into the registry.

        Each decision: ``{"name": <folder>,
        "action": "skip"|"overwrite"|"add_missing"|"rename", "new_name": <str>}``.
        ``overwrite`` and ``add_missing`` both write only missing versions.
        Missing entries default to ``"skip"``.
        """
        try:
            domain_session = _ss.get_domain(session_mgr)
            svc = _ss.RegistryService.from_context(domain_session, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")
            envelope = SettingsService._decode_obx_payload(file_bytes)
            decision_map: Dict[str, Dict[str, Any]] = {
                (d.get("name") or "").strip(): d
                for d in (decisions or [])
                if (d.get("name") or "").strip()
            }
            summary: Dict[str, Any] = {
                "imported_versions": 0,
                "skipped_domains": 0,
                "renamed_domains": 0,
                "versions_skipped": 0,
                "errors": [],
                "domains": [],
            }
            for entry in envelope.get("domains", []):
                ObxSettings._import_one_domain_entry(
                    entry, decision_map, svc, domain_session, settings, summary
                )
            _ss.invalidate_registry_cache()
            return {
                "success": True,
                "message": (
                    f"Imported {summary['imported_versions']} version(s) "
                    f"across {len(summary['domains'])} context(s)"
                ),
                **summary,
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("OBX import failed: %s", e)
            raise InfrastructureError("OBX import failed", detail=str(e)) from e
