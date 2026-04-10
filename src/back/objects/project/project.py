"""Session-scoped project management (non-HTTP).

Use :class:`Project` with a :class:`~back.objects.session.ProjectSession` instance.
Routes should construct ``Project(session, settings)`` and call methods.
"""
from __future__ import annotations

import copy
import json
import os
import re
import threading
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from shared.config.settings import Settings
from back.core.errors import ValidationError
from shared.config.constants import DEFAULT_BASE_URI, DEFAULT_LOG_LEVEL, DEFAULT_GRAPH_NAME
from back.core.databricks import (
    DatabricksClient,
    MetadataService,
    VolumeFileService,
    build_metadata_dict,
    get_catalog_schema_from_metadata,
    has_metadata as check_has_metadata,
    validate_metadata,
)
from back.core.helpers import get_databricks_host_and_token, resolve_warehouse_id, run_blocking
from back.core.logging import get_logger
from back.objects.registry import RegistryService
from back.objects.session import sanitize_project_folder
from back.core.task_manager import get_task_manager
from back.objects.project._metadata_tasks import run_metadata_load_task, run_metadata_update_task
from back.objects.project.version_status import (
    clear_version_status_cache,
    get_cached_version_status,
    set_cached_version_status,
)

if TYPE_CHECKING:
    from back.objects.session.project_session import ProjectSession

logger = get_logger(__name__)


def merge_table_metadata(
    old_table: dict,
    new_columns: list,
    table_comment: str,
    catalog: str,
    schema: str,
    table_name: str,
) -> None:
    """Merge freshly-fetched UC metadata into an existing table dict in-place.

    Preserves user-edited column comments that the UC schema has lost.
    Shared by :meth:`Project.update_metadata_tables` and the async
    task variant in ``_metadata_tasks``.
    """
    old_table["full_name"] = f"{catalog}.{schema}.{table_name}"
    if table_comment:
        old_table["comment"] = table_comment
        old_table["description"] = table_comment
    if new_columns:
        old_column_comments: Dict[str, str] = {}
        for col in old_table.get("columns", []):
            col_name = col.get("col_name") or col.get("name", "")
            if col.get("comment"):
                old_column_comments[col_name] = col["comment"]
        for col in new_columns:
            col_name = col.get("col_name") or col.get("name", "")
            if col_name in old_column_comments and not col.get("comment"):
                col["comment"] = old_column_comments[col_name]
        old_table["columns"] = new_columns


class Project:
    """Project management operations for the current session-backed project."""

    def __init__(self, session: "ProjectSession", settings: Optional[Settings] = None) -> None:
        self._s = session
        self._settings = settings

    def _require_settings(self) -> Settings:
        if self._settings is None:
            raise ValidationError("Settings are required for this operation")
        return self._settings

    def get_project_info(self) -> Dict[str, Any]:
        """Get current project information.
        
        Args:
            project: ProjectSession instance
            
        Returns:
            dict: Project info with stats
        """
        delta = self._s.delta

        _name = self._s.info.get('name', '')
        _version = getattr(self._s, "current_version", "1") or "1"
        if _name:
            _safe = re.sub(r'[^a-z0-9_]', '_', _name.lower())
            _view_name = f"triplestore_{_safe}_V{_version}"
        else:
            _view_name = delta.get('table_name', '')
        parts = [delta.get('catalog', ''), delta.get('schema', ''), _view_name]
        view_table = '.'.join(p for p in parts if p)
        graph_name = f"{self._s.info.get('name', DEFAULT_GRAPH_NAME)}_V{_version}"

        project_info = {
            'name': self._s.info.get('name', 'NewProject'),
            'description': self._s.info.get('description', ''),
            'author': self._s.info.get('author', ''),
            'version': self._s.current_version,
            'base_uri': self._s.ontology.get('base_uri', ''),
            'base_uri_auto': self._s.ontology.get('base_uri_auto', None),
            'llm_endpoint': self._s.info.get('llm_endpoint', ''),
            'mcp_enabled': self._s.info.get('mcp_enabled', False),
            'view_table': view_table,
            'graph_name': graph_name,
        }

        return {
            'success': True,
            'info': project_info,
            'name': project_info.get('name', 'NewProject'),
            'config': {},
            'registry': {
                'catalog': self._s.registry.get('catalog', ''),
                'schema': self._s.registry.get('schema', ''),
                'volume': self._s.registry.get('volume', ''),
            },
            'project_folder': self._s.project_folder,
            'delta': {
                'catalog': delta.get('catalog', ''),
                'schema': delta.get('schema', ''),
                'table_name': delta.get('table_name', '')
            },
            'ladybug': dict(self._s.ladybug),
            "stats": self.get_project_stats(),
        }


    def get_project_stats(self) -> Dict[str, int]:
        """Get project statistics.
        
        Counts respect excluded items and filter relationships to
        ObjectProperties only, matching the Mapping Summary logic.
        """
        all_classes = self._s.get_classes()
        all_properties = self._s.get_properties()
        assignment = self._s.assignment or {}

        excluded_entity_uris = {
            m.get('ontology_class') for m in assignment.get('entities', [])
            if m.get('excluded')
        }
        excluded_rel_uris = {
            m.get('property') for m in assignment.get('relationships', [])
            if m.get('excluded')
        }

        active_classes = [c for c in all_classes if c.get('uri') not in excluded_entity_uris]
        excluded_names = set()
        for c in all_classes:
            if c.get('uri') in excluded_entity_uris:
                for key in ('name', 'localName'):
                    if c.get(key):
                        excluded_names.add(c[key])

        active_props = [
            p for p in all_properties
            if p.get('type') == 'ObjectProperty'
            and p.get('uri') not in excluded_rel_uris
            and p.get('domain') not in excluded_names
            and p.get('range') not in excluded_names
        ]

        active_class_uris = {c.get('uri') for c in active_classes}
        active_prop_uris = {p.get('uri') for p in active_props}

        entity_mappings = [
            m for m in self._s.get_entity_mappings()
            if (m.get('ontology_class') or m.get('class_uri')) in active_class_uris
        ]
        relationship_mappings = [
            m for m in self._s.get_relationship_mappings()
            if m.get('property') in active_prop_uris
        ]

        return {
            'entities': len(active_classes),
            'relationships': len(active_props),
            'entity_mappings': len(entity_mappings),
            'relationship_mappings': len(relationship_mappings),
        }


    def save_project_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Save project information.
        
        Args:
            project: ProjectSession instance
            data: Project info data
            
        Returns:
            dict: Updated project info
        """
        # Update info (without version - version is stored separately)
        self._s.info.update({
            'name': data.get('name', self._s.info.get('name', 'NewProject')),
            'description': data.get('description', self._s.info.get('description', '')),
            'author': data.get('author', self._s.info.get('author', '')),
            'llm_endpoint': data.get('llm_endpoint', self._s.info.get('llm_endpoint', '')),
            'mcp_enabled': data.get('mcp_enabled', self._s.info.get('mcp_enabled', False)),
        })

        # Update delta sub-node (view location)
        delta_data = data.get('delta') or {}
        if delta_data:
            cur = self._s.delta
            for key in ('catalog', 'schema', 'table_name'):
                if key in delta_data:
                    cur[key] = delta_data[key]

        # Update ontology base_uri if provided
        base_uri = data.get('base_uri') or data.get('uri')
        if base_uri:
            self._s.ontology['base_uri'] = base_uri

        if 'base_uri_auto' in data:
            self._s.ontology['base_uri_auto'] = bool(data['base_uri_auto'])

        # Update version separately
        if data.get('version'):
            self._s.current_version = data.get('version')

        self._s.save()

        return {
            'name': self._s.info.get('name'),
            'description': self._s.info.get('description'),
            'author': self._s.info.get('author'),
            'version': self._s.current_version,
            'base_uri': self._s.ontology.get('base_uri', ''),
            'base_uri_auto': self._s.ontology.get('base_uri_auto', None),
            'llm_endpoint': self._s.info.get('llm_endpoint', ''),
            'mcp_enabled': self._s.info.get('mcp_enabled', False),
        }


    def get_project_template_data(self) -> Dict[str, Any]:
        """Get project data for template rendering.
        
        Args:
            project: ProjectSession instance
            
        Returns:
            dict: Template data
        """
        delta = self._s.delta
        reg = self._s.registry

        return {
            'name': self._s.info.get('name', 'NewProject'),
            'description': self._s.info.get('description', ''),
            'base_uri': self._s.ontology.get('base_uri', ''),
            'base_uri_auto': self._s.ontology.get('base_uri_auto', None),
            'version': self._s.current_version,
            'author': self._s.info.get('author', ''),
            'llm_endpoint': self._s.info.get('llm_endpoint', ''),
            'mcp_enabled': self._s.info.get('mcp_enabled', False),
            'delta': delta,
            'ladybug': self._s.ladybug,
            'has_ontology': len(self._s.get_classes()) > 0,
            'has_mapping': len(self._s.get_entity_mappings()) > 0,
            'has_design': bool(self._s.design_layout.get('views')),
            'registry': reg,
            'project_folder': self._s.project_folder,
        }


    def import_project(self, project_data: Dict[str, Any], 
                       selected_version: str = None) -> Dict[str, Any]:
        """Import project from data.
        
        Args:
            project: ProjectSession instance
            project_data: Project data dictionary
            selected_version: Optional version to load
            
        Returns:
            dict: Import result with stats
        """
        from back.objects.ontology import Ontology
        from back.core.w3c import R2RMLGenerator
        
        self._s.import_from_file(project_data, version=selected_version)
        
        # Auto-generate OWL if ontology has classes
        owl_generated = False
        if self._s.get_classes():
            try:
                owl_content = Ontology.generate_owl(
                    self._s.ontology,
                    self._s.constraints,
                    self._s.swrl_rules,
                    self._s.axioms,
                    self._s.expressions,
                )
                self._s.generated['owl'] = owl_content
                owl_generated = True
            except Exception as e:
                logger.warning("Could not auto-generate OWL: %s", e)
        
        # Auto-generate R2RML if mappings exist
        r2rml_generated = False
        if self._s.get_entity_mappings():
            try:
                base_uri = self._s.ontology.get('base_uri', DEFAULT_BASE_URI)
                generator = R2RMLGenerator(base_uri)
                r2rml_content = generator.generate_mapping(self._s.assignment, self._s.ontology)
                self._s.set_r2rml(r2rml_content)
                r2rml_generated = True
            except Exception as e:
                logger.warning("Could not auto-generate R2RML: %s", e)
        
        self._s.save()
        
        return {
            'success': True,
            'message': 'Project imported',
            'name': self._s.info.get('name', 'NewProject'),
            'version': self._s.current_version,
            'stats': {
                'entities': len(self._s.get_classes()),
                'relationships': len(self._s.get_properties()),
                'constraints': len(self._s.constraints),
                'mappings': len(self._s.get_entity_mappings())
            },
            'generated': {
                'owl': owl_generated,
                'r2rml': r2rml_generated
            }
        }


    # -------------------------------------------------------------------
    # Registry & LadybugDB sync
    # -------------------------------------------------------------------


    def build_registry_service(self) -> RegistryService:
        """Build a RegistryService from the current project session."""
        return RegistryService.from_context(self._s, self._require_settings())


    def resolve_ladybug_db_name(self) -> str:
        """Resolve the effective db_name for LadybugDB (project name + version)."""
        name = self._s.info.get("name", DEFAULT_GRAPH_NAME)
        version = getattr(self._s, "current_version", "1") or "1"
        return f"{name}_V{version}"


    def sync_ladybug_to_volume(self, uc_service) -> str:
        """Push the local LadybugDB graph to the registry Volume.

        Returns an empty string on success, or a warning message on failure.
        """
        from back.core.triplestore.ladybugdb import sync_to_volume

        uc_path = self._s.uc_project_path
        if not uc_path:
            return "Registry path not configured — graph not synced"
        db_name = self.resolve_ladybug_db_name()
        ok, msg = sync_to_volume(uc_service, uc_path, db_name)
        if not ok:
            logger.warning("LadybugDB sync-to-volume failed: %s", msg)
            return msg
        return ""


    def sync_ladybug_from_volume(self, uc_service) -> str:
        """Pull the LadybugDB graph from the registry Volume to local disk.

        Returns an empty string on success, or a warning message on failure.
        """
        from back.core.triplestore.ladybugdb import sync_from_volume

        uc_path = self._s.uc_project_path
        if not uc_path:
            return "Registry path not configured — graph not restored"
        db_name = self.resolve_ladybug_db_name()
        ok, msg = sync_from_volume(uc_service, uc_path, db_name)
        if not ok:
            logger.warning("LadybugDB sync-from-volume failed: %s", msg)
            return msg
        return ""


    @staticmethod
    def list_projects_result(svc: RegistryService) -> Dict[str, Any]:
        """List project folders in the registry Volume."""
        try:
            if not svc.cfg.is_configured:
                return {'success': False, 'message': 'Registry not configured. Go to Settings.', 'projects': []}
            ok, names, msg = svc.list_projects()
            if not ok:
                return {'success': False, 'message': msg, 'projects': []}
            return {'success': True, 'projects': names}
        except Exception as e:
            logger.exception("List projects failed: %s", e)
            return {'success': False, 'message': str(e), 'projects': []}


    @staticmethod
    def list_project_versions_result(svc: RegistryService, project_name: str) -> Dict[str, Any]:
        """List available versions for a project in the registry."""
        try:
            if not svc.cfg.is_configured:
                return {'success': False, 'message': 'Registry not configured', 'versions': []}
            ok, versions, msg = svc.list_versions(project_name)
            if not ok:
                return {'success': False, 'message': msg, 'versions': []}
            return {'success': True, 'versions': versions}
        except Exception as e:
            logger.exception("List project versions failed: %s", e)
            return {'success': False, 'message': str(e), 'versions': []}


    def list_version_details(self, svc: RegistryService) -> Dict[str, Any]:
        """List all versions with their description and mcp_enabled flag.

        Reads each version JSON from the registry to extract per-version metadata.
        """
        try:
            if not svc.cfg.is_configured:
                return {'success': False, 'message': 'Registry not configured', 'versions': []}

            folder = self._s.uc_project_folder
            if not folder:
                return {'success': False, 'message': 'Project not saved to registry', 'versions': []}

            sorted_versions = svc.list_versions_sorted(folder)
            if not sorted_versions:
                return {'success': True, 'versions': [], 'current_version': self._s.current_version}

            latest = sorted_versions[0] if sorted_versions else None
            details: List[Dict[str, Any]] = []

            for ver in sorted_versions:
                ok, data, _msg = svc.read_version(folder, ver)
                if not ok:
                    details.append({
                        'version': ver,
                        'description': '',
                        'mcp_enabled': False,
                        'is_active': ver == latest,
                        'is_current': ver == self._s.current_version,
                        'error': _msg,
                    })
                    continue

                info = data.get('info', {})
                details.append({
                    'version': ver,
                    'description': info.get('description', ''),
                    'mcp_enabled': info.get('mcp_enabled', False),
                    'author': info.get('author', ''),
                    'last_update': info.get('last_update', ''),
                    'is_active': ver == latest,
                    'is_current': ver == self._s.current_version,
                })

            return {
                'success': True,
                'versions': details,
                'current_version': self._s.current_version,
                'project_folder': folder,
            }
        except Exception as e:
            logger.exception("List version details failed: %s", e)
            return {'success': False, 'message': str(e), 'versions': []}


    def set_version_mcp(self, svc: RegistryService, version: str, enabled: bool) -> Dict[str, Any]:
        """Toggle the ``mcp_enabled`` flag for a single version.

        Only one version may have ``mcp_enabled=True`` at a time.  When
        *enabled* is ``True`` any other version that currently has the flag
        is updated to ``False`` first.
        """
        try:
            folder = self._s.uc_project_folder
            if not folder:
                return {'success': False, 'message': 'Project not saved to registry'}

            sorted_versions = svc.list_versions_sorted(folder)
            if version not in sorted_versions:
                return {'success': False, 'message': f'Version {version} not found'}

            if enabled:
                for ver in sorted_versions:
                    if ver == version:
                        continue
                    ok, data, _ = svc.read_version(folder, ver)
                    if not ok:
                        continue
                    info = data.get('info', {})
                    if info.get('mcp_enabled'):
                        info['mcp_enabled'] = False
                        data['info'] = info
                        svc.write_version(folder, ver, json.dumps(data))

            ok, data, msg = svc.read_version(folder, version)
            if not ok:
                return {'success': False, 'message': msg}

            data.setdefault('info', {})['mcp_enabled'] = enabled
            svc.write_version(folder, version, json.dumps(data))

            if version == self._s.current_version:
                self._s.info['mcp_enabled'] = enabled

            return {'success': True, 'version': version, 'mcp_enabled': enabled}
        except Exception as e:
            logger.exception("set_version_mcp failed: %s", e)
            return {'success': False, 'message': str(e)}


    def save_project_to_uc(self, svc: RegistryService) -> Dict[str, Any]:
        """Save project into the registry Volume under /projects/<name>/v{ver}.json."""
        try:
            c = svc.cfg
            if not c.is_configured:
                return {'success': False, 'message': 'Registry not configured. Go to Settings.'}

            folder = sanitize_project_folder(self._s.info.get('name', 'untitled_project'))
            version = self._s.current_version or '1'
            export_data = self._s.export_for_save()
            content = json.dumps(export_data, indent=2)
            ok, message = svc.write_version(folder, version, content)

            if ok:
                self._s.clear_change_flags()
                self._s.project_folder = folder
                reg_settings = self._s.settings.setdefault('registry', {})
                if not reg_settings.get('catalog'):
                    reg_settings['catalog'] = c.catalog
                if not reg_settings.get('schema'):
                    reg_settings['schema'] = c.schema
                if not reg_settings.get('volume'):
                    reg_settings['volume'] = c.volume
                self._s.save()
                clear_version_status_cache()
                graph_warning = self.sync_ladybug_to_volume(svc.uc)
                filename = f"v{version}.json"
                msg = f'Project saved to {c.catalog}.{c.schema}.{c.volume}/projects/{folder}/{filename}'
                if graph_warning:
                    msg += f" (graph sync warning: {graph_warning})"
                return {'success': True, 'message': msg}
            return {'success': False, 'message': message}
        except Exception as e:
            logger.exception("Save project to UC failed: %s", e)
            return {'success': False, 'message': str(e)}


    def load_project_from_uc(
        self,
        svc: RegistryService,
        project_name: str,
        version: str,
    ) -> Dict[str, Any]:
        """Load project from registry Volume."""
        try:
            if not project_name or not version:
                return {'success': False, 'message': 'Project name and version are required'}
            c = svc.cfg
            if not c.catalog or not c.volume:
                return {'success': False, 'message': 'Registry not configured'}
            r_ok, project_data, r_msg = svc.read_version(project_name, version)
            if not r_ok:
                return {'success': False, 'message': r_msg}

            self._s.clear_generated_content()
            self._s.import_from_file(project_data, version=version)
            loaded_entities = len(self._s.get_entity_mappings())
            loaded_rels = len(self._s.get_relationship_mappings())
            loaded_classes = len(self._s.get_classes())
            logger.info(
                "load-from-uc v%s: %d classes, %d entity mappings, %d rel mappings",
                version, loaded_classes, loaded_entities, loaded_rels
            )
            sorted_versions = svc.list_versions_sorted(project_name)
            is_latest = sorted_versions[0] == version if sorted_versions else True
            self._s.project_folder = project_name
            self._s.is_active_version = is_latest
            reg_settings = self._s.settings.setdefault('registry', {})
            if not reg_settings.get('catalog'):
                reg_settings['catalog'] = c.catalog
            if not reg_settings.get('schema'):
                reg_settings['schema'] = c.schema
            if not reg_settings.get('volume'):
                reg_settings['volume'] = c.volume
            self._s.ensure_generated_content()
            self._s.save()
            clear_version_status_cache()
            graph_warning = self.sync_ladybug_from_volume(svc.uc)
            ts_stats = self._s.triplestore.setdefault('stats', {})
            ts_stats.pop('status', None)
            ts_stats.pop('dt_existence', None)
            self._s.save()
            status = "Active" if is_latest else "Inactive (read-only)"
            msg = f'Project loaded: {project_name} v{version} ({status})'
            if graph_warning:
                msg += f" (graph sync warning: {graph_warning})"
            return {
                'success': True,
                'message': msg,
                'is_active': is_latest,
                'version': version
            }
        except Exception as e:
            logger.exception("Load project from UC failed: %s", e)
            return {'success': False, 'message': str(e)}


    def create_new_project_version(self, svc: RegistryService) -> Dict[str, Any]:
        """Create a new version of the project and save to registry."""
        try:
            reg = self._s.registry
            if not reg.get('catalog') or not self._s.project_folder:
                return {'success': False, 'message': 'Project must be saved to Unity Catalog first'}
            if not self._s.is_active_version:
                return {
                    'success': False,
                    'message': 'Cannot create new version from an inactive version. Load the latest version first.'
                }
            entity_count = len(self._s.get_entity_mappings())
            rel_count = len(self._s.get_relationship_mappings())
            class_count = len(self._s.get_classes())
            logger.info(
                "create-version: session has %d classes, %d entity mappings, %d rel mappings",
                class_count, entity_count, rel_count
            )
            current_version = self._s.current_version or '1'
            parts = current_version.split('.')
            new_version = str(int(parts[0]) + 1) if parts else "2"
            self._s.current_version = new_version
            export_data = self._s.export_for_save()
            exported_entities = len(
                export_data.get('versions', {}).get(new_version, {}).get('assignment', {}).get('entities', [])
            )
            exported_rels = len(
                export_data.get('versions', {}).get(new_version, {}).get('assignment', {}).get('relationships', [])
            )
            logger.info(
                "create-version v%s: exported %d entity mappings, %d rel mappings",
                new_version, exported_entities, exported_rels
            )
            content = json.dumps(export_data, indent=2)
            folder = self._s.uc_project_folder
            c = svc.cfg
            ok, message = svc.write_version(folder, new_version, content)
            if not ok:
                self._s.current_version = current_version
                return {'success': False, 'message': f'Failed to save new version: {message}'}
            self._s.clear_generated_content()
            self._s.save()
            filename = f"v{new_version}.json"
            return {
                'success': True,
                'message': f'Version {new_version} created: {c.catalog}.{c.schema}.{c.volume}/projects/{folder}/{filename}',
                'new_version': new_version,
                'previous_version': current_version
            }
        except Exception as e:
            logger.exception("Create new version failed: %s", e)
            return {'success': False, 'message': str(e)}


    def get_version_status(self, refresh: bool = False) -> Dict[str, Any]:
        """Get current version status and available versions from registry (with TTL cache)."""
        try:
            version = self._s.current_version or '1'
            reg = self._s.registry
            project_folder = self._s.project_folder
            has_registry = bool(reg.get('catalog') and reg.get('volume') and project_folder)
            cache_key = f"{reg.get('catalog','')}.{reg.get('schema','')}.{reg.get('volume','')}/{project_folder}/{version}"

            if not refresh:
                cached = get_cached_version_status(cache_key)
                if cached is not None:
                    return cached

            available_versions: List[str] = []
            if has_registry:
                try:
                    svc = self.build_registry_service()
                    folder = self._s.uc_project_folder
                    available_versions = svc.list_versions_sorted(folder)
                except Exception as e:
                    logger.warning("Could not fetch versions from UC: %s", e)
                    available_versions = [version]
            else:
                available_versions = [version]

            is_latest = not available_versions or version == available_versions[0]
            is_active = self._s.is_active_version
            result = {
                'success': True,
                'version': version,
                'is_active': is_active,
                'is_latest': is_latest,
                'available_versions': available_versions,
                'has_registry': has_registry,
                'registry': {
                    'catalog': reg.get('catalog', ''),
                    'schema': reg.get('schema', ''),
                    'volume': reg.get('volume', ''),
                } if has_registry else None,
                'project_folder': project_folder
            }
            set_cached_version_status(cache_key, result)
            return result
        except Exception as e:
            logger.exception("Get version status failed: %s", e)
            return {'success': False, 'message': str(e)}


    # -------------------------------------------------------------------
    # Design views & map layout
    # -------------------------------------------------------------------


    def get_design_views(self) -> Dict[str, Any]:
        try:
            design_layout = self._s._data.get('design_layout', {})
            if 'views' in design_layout:
                views = list(design_layout.get('views', {}).keys())
                current_view = design_layout.get('current_view')
                if views and not current_view:
                    current_view = views[0]
                elif not views:
                    current_view = None
            else:
                has_entities = bool(design_layout.get('entities'))
                has_relationships = bool(design_layout.get('relationships'))
                has_inheritances = bool(design_layout.get('inheritances'))
                if has_entities or has_relationships or has_inheritances:
                    views = ['default']
                    current_view = 'default'
                else:
                    views = []
                    current_view = None
            return {'success': True, 'views': views, 'current_view': current_view}
        except Exception as e:
            logger.warning("get_design_views failed: %s", e, exc_info=True)
            return {'success': False, 'message': str(e)}


    def create_design_view(self, view_name: str, copy_from: Optional[str]) -> Dict[str, Any]:
        try:
            if not view_name:
                return {'success': False, 'message': 'View name is required'}
            design_layout = self._s._data.get('design_layout', {})
            if 'views' not in design_layout:
                existing_map = design_layout.get('map', {})
                old_entities = design_layout.get('entities', [])
                old_relationships = design_layout.get('relationships', [])
                old_inheritances = design_layout.get('inheritances', [])
                old_visibility = design_layout.get('visibility')
                views: Dict[str, Any] = {}
                if old_entities or old_relationships or old_inheritances:
                    default_view = {
                        'entities': old_entities,
                        'relationships': old_relationships,
                        'inheritances': old_inheritances
                    }
                    if old_visibility:
                        default_view['visibility'] = old_visibility
                    views['default'] = default_view
                design_layout = {
                    'current_view': 'default' if views else None,
                    'views': views,
                    'map': existing_map
                }
            design_layout.pop('entities', None)
            design_layout.pop('relationships', None)
            design_layout.pop('inheritances', None)
            design_layout.pop('visibility', None)
            design_layout.pop('positions', None)
            if view_name in design_layout.get('views', {}):
                return {'success': False, 'message': f'View "{view_name}" already exists'}
            if copy_from and copy_from in design_layout.get('views', {}):
                design_layout['views'][view_name] = copy.deepcopy(design_layout['views'][copy_from])
            else:
                design_layout['views'][view_name] = {'entities': [], 'relationships': [], 'inheritances': []}
            self._s._data['design_layout'] = design_layout
            self._s.save()
            return {'success': True, 'views': list(design_layout['views'].keys())}
        except Exception as e:
            logger.exception("Create design view failed: %s", e)
            return {'success': False, 'message': str(e)}


    def rename_design_view(self, old_name: str, new_name: str) -> Dict[str, Any]:
        try:
            if not old_name or not new_name:
                return {'success': False, 'message': 'Both old and new names are required'}
            design_layout = self._s._data.get('design_layout', {})
            if 'views' not in design_layout:
                return {'success': False, 'message': 'No views exist'}
            if old_name not in design_layout['views']:
                return {'success': False, 'message': f'View "{old_name}" not found'}
            if new_name in design_layout['views']:
                return {'success': False, 'message': f'View "{new_name}" already exists'}
            design_layout['views'][new_name] = design_layout['views'].pop(old_name)
            if design_layout.get('current_view') == old_name:
                design_layout['current_view'] = new_name
            self._s._data['design_layout'] = design_layout
            self._s.save()
            return {'success': True, 'views': list(design_layout['views'].keys())}
        except Exception as e:
            logger.exception("Rename design view failed: %s", e)
            return {'success': False, 'message': str(e)}


    def delete_design_view(self, view_name: str) -> Dict[str, Any]:
        try:
            if not view_name:
                return {'success': False, 'message': 'View name is required'}
            design_layout = self._s._data.get('design_layout', {})
            if 'views' not in design_layout:
                return {'success': False, 'message': 'No views exist'}
            if view_name not in design_layout['views']:
                return {'success': False, 'message': f'View "{view_name}" not found'}
            if len(design_layout['views']) <= 1:
                return {'success': False, 'message': 'Cannot delete the last view'}
            del design_layout['views'][view_name]
            if design_layout.get('current_view') == view_name:
                design_layout['current_view'] = list(design_layout['views'].keys())[0]
            self._s._data['design_layout'] = design_layout
            self._s.save()
            return {
                'success': True,
                'views': list(design_layout['views'].keys()),
                'current_view': design_layout['current_view']
            }
        except Exception as e:
            logger.exception("Delete design view failed: %s", e)
            return {'success': False, 'message': str(e)}


    def switch_design_view(self, view_name: str) -> Dict[str, Any]:
        try:
            if not view_name:
                return {'success': False, 'message': 'View name is required'}
            design_layout = self._s._data.get('design_layout', {})
            if 'views' not in design_layout:
                return {'success': False, 'message': 'No views exist'}
            if view_name not in design_layout['views']:
                return {'success': False, 'message': f'View "{view_name}" not found'}
            design_layout['current_view'] = view_name
            self._s._data['design_layout'] = design_layout
            self._s.save()
            return {
                'success': True,
                'current_view': view_name,
                'layout': design_layout['views'][view_name]
            }
        except Exception as e:
            logger.exception("Switch design view failed: %s", e)
            return {'success': False, 'message': str(e)}


    def get_current_design_view(self) -> Dict[str, Any]:
        try:
            design_layout = self._s._data.get('design_layout', {})
            if 'views' in design_layout:
                current_view = design_layout.get('current_view', 'default')
                layout = design_layout['views'].get(current_view, {})
            else:
                current_view = 'default'
                layout = {k: v for k, v in design_layout.items() if k not in ['views', 'current_view']}
            return {'success': True, 'current_view': current_view, 'layout': layout}
        except Exception as e:
            logger.exception("Get current design view failed: %s", e)
            return {'success': False, 'message': str(e)}


    def save_current_design_view(self, layout_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            layout_work = dict(layout_data)
            if 'entities' in layout_work and layout_work['entities']:
                layout_work['entities'] = [
                    {
                        'id': e.get('id'),
                        'name': e.get('name'),
                        'x': e.get('x'),
                        'y': e.get('y'),
                        'properties': e.get('properties'),
                        'color': e.get('color')
                    }
                    for e in layout_work['entities']
                ]
            design_layout = self._s._data.get('design_layout', {})
            if 'views' not in design_layout:
                existing_map = design_layout.get('map', {})
                design_layout = {
                    'current_view': 'default',
                    'views': {'default': {}},
                    'map': existing_map
                }
            design_layout.pop('entities', None)
            design_layout.pop('relationships', None)
            design_layout.pop('inheritances', None)
            design_layout.pop('visibility', None)
            design_layout.pop('positions', None)
            if 'current_view' not in design_layout:
                design_layout['current_view'] = 'default'
            if 'map' not in design_layout:
                design_layout['map'] = {}
            current_view = design_layout.get('current_view', 'default')
            if current_view not in design_layout['views']:
                design_layout['views'][current_view] = {}
            design_layout['views'][current_view] = layout_work
            self._s._data['design_layout'] = design_layout
            self._s.save()
            return {'success': True, 'current_view': current_view}
        except Exception as e:
            logger.exception("Save current design view failed: %s", e)
            return {'success': False, 'message': str(e)}


    def get_map_layout(self) -> Dict[str, Any]:
        try:
            design_layout = self._s._data.get('design_layout', {})
            map_layout = design_layout.get('map', {})
            return {'success': True, 'layout': map_layout}
        except Exception as e:
            logger.exception("Get map layout failed: %s", e)
            return {'success': False, 'message': str(e)}


    def save_map_layout(self, layout_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if 'design_layout' not in self._s._data:
                self._s._data['design_layout'] = {}
            self._s._data['design_layout']['map'] = layout_data
            self._s.save()
            return {'success': True}
        except Exception as e:
            logger.exception("Save map layout failed: %s", e)
            return {'success': False, 'message': str(e)}


    # -------------------------------------------------------------------
    # Session debug
    # -------------------------------------------------------------------


    def get_session_debug_response(self) -> Dict[str, Any]:
        if os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper() != "DEBUG":
            return {"success": False, "detail": "session-debug is only available when LOG_LEVEL=DEBUG"}
        data = self._s._data.copy()
        if 'databricks' in data:
            db = data['databricks'].copy()
            if db.get('token'):
                db['token'] = '***MASKED***'
            data['databricks'] = db
        if 'generated' in data:
            gen = data['generated'].copy()
            if gen.get('owl') and len(gen['owl']) > 500:
                gen['owl'] = gen['owl'][:500] + f'... ({len(data["generated"]["owl"])} chars total)'
            if gen.get('sql') and len(gen['sql']) > 500:
                gen['sql'] = gen['sql'][:500] + f'... ({len(data["generated"]["sql"])} chars total)'
            data['generated'] = gen
        if 'assignment' in data and data['assignment'].get('r2rml_output'):
            if len(data['assignment']['r2rml_output']) > 500:
                data['assignment'] = data['assignment'].copy()
                data['assignment']['r2rml_output'] = (
                    data['assignment']['r2rml_output'][:500]
                    + f'... ({len(self._s._data["assignment"]["r2rml_output"])} chars total)'
                )
        return {'success': True, 'session_data': data}


    # -------------------------------------------------------------------
    # Unity Catalog metadata
    # -------------------------------------------------------------------


    def get_metadata_response(self) -> Dict[str, Any]:
        metadata = self._s.catalog_metadata
        has_meta = check_has_metadata(metadata)
        return {
            'success': True,
            'has_metadata': has_meta,
            'metadata': metadata if has_meta else None
        }


    async def list_schema_tables_result(
        self,
        catalog: str,
        schema: str,
    ) -> Dict[str, Any]:
        try:
            if not catalog or not schema:
                return {'success': False, 'message': 'Catalog and schema are required'}
            st = self._require_settings()
            host, token = get_databricks_host_and_token(self._s, st)
            warehouse_id = resolve_warehouse_id(self._s, st)
            if not host or not warehouse_id:
                return {'success': False, 'message': 'Databricks not configured. Please configure connection in Settings.'}
            client = DatabricksClient(host=host, token=token, warehouse_id=warehouse_id)
            tables = await run_blocking(client.get_tables, catalog, schema)
            existing_metadata = self._s.catalog_metadata
            existing_table_names = set()
            if existing_metadata and existing_metadata.get('tables'):
                existing_table_names = {t['name'] for t in existing_metadata['tables']}
            table_list = []
            for table_name in sorted(tables):
                table_list.append({
                    'name': table_name,
                    'already_loaded': table_name in existing_table_names
                })
            return {
                'success': True,
                'tables': table_list,
                'total_count': len(tables),
                'existing_count': len(existing_table_names)
            }
        except Exception as e:
            logger.exception("List schema tables failed: %s", e)
            return {'success': False, 'message': str(e)}


    def initialize_metadata_result(
        self,
        catalog: str,
        schema: str,
        selected_tables: Optional[List[str]],
    ) -> Dict[str, Any]:
        try:
            if not catalog or not schema:
                return {'success': False, 'message': 'Catalog and schema are required'}
            st = self._require_settings()
            host, token = get_databricks_host_and_token(self._s, st)
            warehouse_id = resolve_warehouse_id(self._s, st)
            if not host or not warehouse_id:
                return {'success': False, 'message': 'Databricks not configured. Please configure connection in Settings.'}
            service = MetadataService(host=host, token=token, warehouse_id=warehouse_id)
            existing_metadata = self._s.catalog_metadata
            if selected_tables is not None:
                success, message, metadata = service.load_selected_tables(
                    catalog=catalog,
                    schema=schema,
                    table_names=selected_tables,
                    existing_metadata=existing_metadata
                )
            else:
                success, message, metadata = service.load_schema_metadata(
                    catalog=catalog,
                    schema=schema,
                    existing_metadata=existing_metadata
                )
            if not success:
                return {'success': False, 'message': message}
            self._s._data['project']['metadata'] = metadata
            self._s.save()
            existing_count = len(existing_metadata.get('tables', []))
            new_count = len(metadata.get('tables', [])) - existing_count
            return {
                'success': True,
                'message': message,
                'metadata': metadata,
                'new_tables_count': max(0, new_count),
                'existing_tables_count': existing_count
            }
        except Exception as e:
            logger.exception("Initialize metadata failed: %s", e)
            return {'success': False, 'message': str(e)}


    def save_metadata_tables(self, tables: List[Dict[str, Any]]) -> Dict[str, Any]:
        try:
            existing_metadata = self._s.catalog_metadata
            catalog, schema = get_catalog_schema_from_metadata(existing_metadata)
            if not catalog:
                catalog = existing_metadata.get('catalog', '')
            if not schema:
                schema = existing_metadata.get('schema', '')
            for table in tables:
                if not table.get('full_name') and table.get('name'):
                    if catalog and schema:
                        table['full_name'] = f"{catalog}.{schema}.{table['name']}"
                    else:
                        table['full_name'] = table['name']
            metadata = build_metadata_dict(tables)
            is_valid, error_msg = validate_metadata(metadata)
            if not is_valid:
                return {'success': False, 'message': error_msg}
            self._s._data['project']['metadata'] = metadata
            self._s.save()
            return {
                'success': True,
                'message': f'Saved metadata with {len(tables)} tables',
                'metadata': metadata
            }
        except Exception as e:
            logger.exception("Save metadata failed: %s", e)
            return {'success': False, 'message': str(e)}


    def clear_metadata(self) -> Dict[str, Any]:
        self._s._data['project']['metadata'] = {}
        self._s.save()
        return {'success': True, 'message': 'Metadata cleared'}


    def start_metadata_initialize_async(
        self,
        catalog: str,
        schema: str,
        selected_tables: Optional[List[str]],
    ) -> Dict[str, Any]:
        try:
            if not catalog or not schema:
                return {'success': False, 'message': 'Catalog and schema are required'}
            st = self._require_settings()
            host, token = get_databricks_host_and_token(self._s, st)
            warehouse_id = resolve_warehouse_id(self._s, st)
            if not host or not warehouse_id:
                return {'success': False, 'message': 'Databricks not configured. Please configure connection in Settings.'}
            existing_metadata = self._s.catalog_metadata
            tables_count = len(selected_tables) if selected_tables else 'all'
            tm = get_task_manager()
            task = tm.create_task(
                name=f"Load Metadata ({tables_count} tables from {catalog}.{schema})",
                task_type="metadata_load",
                steps=[
                    {'name': 'connect', 'description': 'Connecting to Unity Catalog'},
                    {'name': 'fetch', 'description': 'Fetching table metadata'},
                    {'name': 'save', 'description': 'Saving metadata'}
                ]
            )
            thread = threading.Thread(
                target=run_metadata_load_task,
                args=(
                    task.id, host, token, warehouse_id, catalog, schema,
                    selected_tables, existing_metadata,
                ),
                daemon=True,
            )
            thread.start()
            return {'success': True, 'task_id': task.id, 'message': 'Task started'}
        except Exception as e:
            logger.exception("Initialize metadata async failed: %s", e)
            return {'success': False, 'message': str(e)}


    def start_metadata_update_async(
        self,
        table_names: Optional[List[str]],
    ) -> Dict[str, Any]:
        try:
            existing_metadata = self._s.catalog_metadata
            if not existing_metadata or not existing_metadata.get('tables'):
                return {'success': False, 'message': 'No metadata loaded to update'}
            catalog, schema = get_catalog_schema_from_metadata(existing_metadata)
            if not catalog or not schema:
                return {'success': False, 'message': 'Cannot determine catalog/schema from table full_names'}
            st = self._require_settings()
            host, token = get_databricks_host_and_token(self._s, st)
            warehouse_id = resolve_warehouse_id(self._s, st)
            if not host or not warehouse_id:
                return {'success': False, 'message': 'Databricks not configured. Please configure connection in Settings.'}
            existing_tables = {t['name']: t for t in existing_metadata.get('tables', [])}
            if table_names:
                tables_to_update = [name for name in table_names if name in existing_tables]
            else:
                tables_to_update = list(existing_tables.keys())
            if not tables_to_update:
                return {'success': False, 'message': 'No tables found to update'}
            tm = get_task_manager()
            task = tm.create_task(
                name=f"Update Metadata ({len(tables_to_update)} tables)",
                task_type="metadata_update",
                steps=[
                    {'name': 'connect', 'description': 'Connecting to Unity Catalog'},
                    {'name': 'update', 'description': f'Updating {len(tables_to_update)} table(s)'},
                    {'name': 'save', 'description': 'Saving metadata'}
                ]
            )
            thread = threading.Thread(
                target=run_metadata_update_task,
                args=(
                    task.id, host, token, warehouse_id, catalog, schema,
                    tables_to_update, existing_metadata, existing_tables,
                ),
                daemon=True,
            )
            thread.start()
            return {'success': True, 'task_id': task.id, 'message': 'Task started'}
        except Exception as e:
            logger.exception("Update metadata async failed: %s", e)
            return {'success': False, 'message': str(e)}


    def update_metadata_tables(
        self,
        table_names: Optional[List[str]],
    ) -> Dict[str, Any]:
        try:
            logger.debug("Metadata update received request with table_names: %s", table_names)
            existing_metadata = self._s.catalog_metadata
            logger.debug("Metadata update: existing metadata has %s tables", len(existing_metadata.get('tables', [])))
            if not existing_metadata or not existing_metadata.get('tables'):
                return {'success': False, 'message': 'No metadata loaded to update'}
            catalog, schema = get_catalog_schema_from_metadata(existing_metadata)
            if not catalog or not schema:
                return {'success': False, 'message': 'Cannot determine catalog/schema from table full_names'}
            st = self._require_settings()
            host, token = get_databricks_host_and_token(self._s, st)
            warehouse_id = resolve_warehouse_id(self._s, st)
            if not host or not warehouse_id:
                return {'success': False, 'message': 'Databricks not configured. Please configure connection in Settings.'}
            client = DatabricksClient(host=host, token=token, warehouse_id=warehouse_id)
            existing_tables = {t['name']: t for t in existing_metadata.get('tables', [])}
            if table_names:
                tables_to_update = [name for name in table_names if name in existing_tables]
            else:
                tables_to_update = list(existing_tables.keys())
            if not tables_to_update:
                return {'success': False, 'message': 'No tables found to update'}
            updated_count = 0
            errors: List[str] = []
            for table_name in tables_to_update:
                try:
                    logger.debug("Metadata update: updating table: %s", table_name)
                    old_table = existing_tables[table_name]
                    new_columns = client.get_table_columns(catalog, schema, table_name)
                    logger.debug("Metadata update: got %s columns for %s", len(new_columns) if new_columns else 0, table_name)
                    table_comment = client.get_table_comment(catalog, schema, table_name)
                    logger.debug("Metadata update: table comment from UC: %s", table_comment)
                    merge_table_metadata(old_table, new_columns, table_comment, catalog, schema, table_name)
                    updated_count += 1
                    logger.debug("Metadata update: successfully updated %s", table_name)
                except Exception as e:
                    logger.exception("Metadata update: error updating table %s: %s", table_name, e)
                    errors.append(f"{table_name}: {str(e)}")
            self._s._data['project']['metadata'] = existing_metadata
            self._s.save()
            message = f'Updated {updated_count} of {len(tables_to_update)} tables'
            if errors:
                message += f'. Errors: {"; ".join(errors[:3])}'
                if len(errors) > 3:
                    message += f' (+{len(errors) - 3} more)'
            return {
                'success': True,
                'message': message,
                'updated_count': updated_count,
                'total_count': len(tables_to_update),
                'errors': errors,
                'metadata': existing_metadata
            }
        except Exception as e:
            logger.exception("Update metadata failed: %s", e)
            return {'success': False, 'message': str(e)}


    # -------------------------------------------------------------------
    # Project documents (volume path helper for routes)
    # -------------------------------------------------------------------


    def get_documents_volume_path(self) -> Optional[str]:
        """Return /Volumes/.../projects/<folder>/documents base path, or None if UC is not configured."""
        path = self._s.uc_project_path
        if not path:
            return None
        return f"{path}/documents"

    def count_documents_in_volume(self, settings: Settings) -> Optional[int]:
        """Count files under the project documents volume path.

        Returns ``0`` when there is no UC project path or the folder is missing.
        Returns ``None`` when credentials are missing or listing fails.
        """
        base_path = self.get_documents_volume_path()
        if not base_path:
            return 0
        host, token = get_databricks_host_and_token(self._s, settings)
        if not host or not token:
            return None
        uc = VolumeFileService(host=host, token=token)
        success, items, message = uc.list_directory(base_path)
        if not success and "not found" in message.lower():
            return 0
        if not success:
            logger.warning(
                "count_documents_in_volume: list failed for %s: %s", base_path, message
            )
            return None
        return len(items)
