"""Digital twin domain: SPARQL/R2RML pipeline, triple-store helpers, registry resolution."""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set

from back.core.errors import InfrastructureError, NotFoundError, ValidationError
from back.core.helpers import sql_escape as escape_sql_value, extract_local_name
from back.core.logging import get_logger
from back.objects.digitaltwin.constants import RDF_TYPE, RDFS_LABEL
from back.objects.digitaltwin.models import ProjectSnapshot
from back.objects.session import get_project

logger = get_logger(__name__)


class DigitalTwin:
    """Centralizes digital-twin query pipeline, data quality, and API resolution helpers.

    Constructed with a project (``ProjectSession`` or snapshot) for instance
    methods that need project state.  Pure transforms and background-thread
    runners are exposed as ``@staticmethod``.
    """

    RDF_TYPE = RDF_TYPE
    RDFS_LABEL = RDFS_LABEL
    ProjectSnapshot = ProjectSnapshot

    def __init__(self, project) -> None:
        self._project = project

    # ------------------------------------------------------------------
    # Private helpers (static)
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_base_uri(uri: str) -> str:
        """Ensure base_uri ends with exactly one '/' separator."""
        return uri.rstrip('/').rstrip('#') + '/'

    @staticmethod
    def _safe_class_label(class_label: str, class_uri: str) -> str:
        """Return a non-empty sanitized class label for use in URI templates.

        Falls back to the local name extracted from class_uri when class_label
        is empty, preventing double-slash URIs like base_uri//{id}.
        """
        name = (class_label or '').strip().replace(' ', '_')
        if name:
            return name
        if class_uri:
            if '#' in class_uri:
                name = class_uri.split('#')[-1].strip()
            elif '/' in class_uri:
                name = class_uri.rstrip('/').split('/')[-1].strip()
            if name:
                return name.replace(' ', '_')
        return 'Entity'

    # ------------------------------------------------------------------
    # R2RML mapping augmentation (static -- pure transforms)
    # ------------------------------------------------------------------

    @staticmethod
    def augment_mappings_from_config(entity_mappings, mapping_config, base_uri, ontology_config=None):
        """Augment R2RML mappings with data from mapping_config to ensure all attributes are included.

        Args:
            entity_mappings: dict of entity class URIs to mapping info
            mapping_config: mapping configuration from session
            base_uri: base URI for the ontology
            ontology_config: ontology configuration (used to skip excluded classes)

        Returns:
            dict: Augmented entity mappings
        """
        base_uri = DigitalTwin._normalize_base_uri(base_uri)

        if not mapping_config:
            return entity_mappings

        ontology_config = ontology_config or {}
        all_dsm = (mapping_config or {}).get('entities', (mapping_config or {}).get('data_source_mappings', []))
        excluded_class_uris = {m.get('ontology_class') for m in all_dsm if m.get('excluded')}

        data_source_mappings = mapping_config.get('entities', mapping_config.get('data_source_mappings', []))

        for dsm in data_source_mappings:
            class_uri = dsm.get('ontology_class', '')
            class_label = dsm.get('ontology_class_label', '')
            sql_query = dsm.get('sql_query', '').strip()
            id_column = dsm.get('id_column', '')
            label_column = dsm.get('label_column', '')
            attribute_mappings = dsm.get('attribute_mappings', {})

            if not class_uri or not sql_query:
                continue

            if class_uri in excluded_class_uris:
                continue

            full_class_uri = class_uri if class_uri.startswith('http') else f"{base_uri}{class_uri}"

            sanitized_label = DigitalTwin._safe_class_label(class_label, class_uri)

            if full_class_uri not in entity_mappings:
                entity_mappings[full_class_uri] = {
                    'table': None,
                    'id_column': id_column,
                    'label_column': label_column,
                    'uri_template': f"{base_uri}{sanitized_label}/{{" + id_column + "}}",
                    'sql_query': sql_query,
                    'predicates': {}
                }

            mapping = entity_mappings[full_class_uri]

            if not mapping.get('sql_query') and sql_query:
                mapping['sql_query'] = sql_query

            if label_column and not mapping.get('label_column'):
                mapping['label_column'] = label_column

            if label_column and 'http://www.w3.org/2000/01/rdf-schema#label' not in mapping.get('predicates', {}):
                mapping.setdefault('predicates', {})['http://www.w3.org/2000/01/rdf-schema#label'] = {
                    'type': 'column',
                    'column': label_column
                }

            for attr_name, column_name in attribute_mappings.items():
                if column_name:
                    pred_uri = f"{base_uri}{attr_name.replace(' ', '_')}"
                    mapping.setdefault('predicates', {})[pred_uri] = {
                        'type': 'column',
                        'column': column_name
                    }

        return entity_mappings

    @staticmethod
    def augment_relationships_from_config(relationship_mappings, mapping_config, base_uri, ontology_config=None):
        """Augment relationship mappings from mapping_config.

        Args:
            relationship_mappings: list of relationship mappings
            mapping_config: mapping configuration from session
            base_uri: base URI for the ontology
            ontology_config: ontology configuration for fallback class lookup

        Returns:
            list: Augmented relationship mappings
        """
        base_uri = DigitalTwin._normalize_base_uri(base_uri)

        ontology_config = ontology_config or {}
        if not mapping_config:
            return relationship_mappings

        all_dsm = (mapping_config or {}).get('entities', (mapping_config or {}).get('data_source_mappings', []))
        excluded_entity_uris = {m.get('ontology_class') for m in all_dsm if m.get('excluded')}
        excluded_class_names = set()
        for c in ontology_config.get('classes', []):
            if c.get('uri') in excluded_entity_uris:
                excluded_class_names.add(c.get('name') or c.get('localName') or '')

        all_rm = (mapping_config or {}).get('relationships', (mapping_config or {}).get('relationship_mappings', []))
        excluded_prop_uris = {m.get('property') for m in all_rm if m.get('excluded')}
        for p in ontology_config.get('properties', []):
            if p.get('domain') in excluded_class_names or p.get('range') in excluded_class_names:
                if p.get('uri'):
                    excluded_prop_uris.add(p['uri'])

        rel_configs = mapping_config.get('relationships', mapping_config.get('relationship_mappings', []))
        data_source_mappings = mapping_config.get('entities', mapping_config.get('data_source_mappings', []))

        entity_lookup = {}
        for dsm in data_source_mappings:
            class_uri = dsm.get('ontology_class', '')
            class_label = dsm.get('ontology_class_label', '')
            id_column = dsm.get('id_column', '')

            full_uri = class_uri if class_uri.startswith('http') else f"{base_uri}{class_uri}"

            sanitized_label = DigitalTwin._safe_class_label(class_label, class_uri)
            entity_info = {
                'uri_base': f"{base_uri}{sanitized_label}/",
                'id_column': id_column
            }

            entity_lookup[class_label] = entity_info
            entity_lookup[class_label.lower()] = entity_info
            entity_lookup[sanitized_label] = entity_info
            entity_lookup[class_uri] = entity_info
            entity_lookup[full_uri] = entity_info

            if '#' in class_uri:
                local_name = class_uri.split('#')[-1]
                entity_lookup[local_name] = entity_info
            elif '/' in class_uri:
                local_name = class_uri.split('/')[-1]
                entity_lookup[local_name] = entity_info

        ontology_property_lookup = {}
        ontology_classes = ontology_config.get('classes', [])
        for prop in ontology_config.get('properties', []) or ontology_config.get('object_properties', []):
            prop_uri = prop.get('uri', '')
            prop_label = prop.get('label', '') or prop.get('name', '')
            domain = prop.get('domain', '') or prop.get('source', '')
            range_val = prop.get('range', '') or prop.get('target', '')

            domain_label = ''
            for cls in ontology_classes:
                if cls.get('uri') == domain or cls.get('name') == domain or cls.get('label') == domain:
                    domain_label = cls.get('label', '') or cls.get('name', '')
                    break

            range_label = ''
            for cls in ontology_classes:
                if cls.get('uri') == range_val or cls.get('name') == range_val or cls.get('label') == range_val:
                    range_label = cls.get('label', '') or cls.get('name', '')
                    break

            prop_info = {
                'domain_label': domain_label,
                'range_label': range_label
            }

            if prop_uri:
                ontology_property_lookup[prop_uri] = prop_info
            if prop_label:
                ontology_property_lookup[prop_label] = prop_info

        for rel in rel_configs:
            sql_query = rel.get('sql_query', '').strip()
            predicate_uri = rel.get('property', '')
            predicate_label = rel.get('property_label', '')
            source_class = rel.get('source_class', '')
            target_class = rel.get('target_class', '')
            source_class_label = rel.get('source_class_label', '')
            target_class_label = rel.get('target_class_label', '')
            source_column = rel.get('source_id_column', '')
            target_column = rel.get('target_id_column', '')

            if not sql_query or not source_column or not target_column:
                continue

            if predicate_uri in excluded_prop_uris:
                continue

            if predicate_uri and predicate_uri.startswith(('http://', 'https://')):
                if not predicate_uri.startswith(base_uri):
                    local = predicate_uri.split('#')[-1] if '#' in predicate_uri else predicate_uri.rstrip('/').split('/')[-1]
                    predicate_uri = f"{base_uri}{local.replace(' ', '_')}"
            elif predicate_uri:
                predicate_uri = f"{base_uri}{predicate_uri.replace(' ', '_')}"
            elif predicate_label:
                predicate_uri = f"{base_uri}{predicate_label.replace(' ', '_')}"
            else:
                predicate_uri = f"{base_uri}relatesTo"

            rel_domain = rel.get('domain', '')
            rel_range = rel.get('range', '')
            direction = rel.get('direction', 'forward')

            source_label = source_class_label or extract_local_name(source_class) or ''
            target_label = target_class_label or extract_local_name(target_class) or ''

            if not source_label:
                source_label = extract_local_name(rel_range if direction == 'reverse' else rel_domain)
            if not target_label:
                target_label = extract_local_name(rel_domain if direction == 'reverse' else rel_range)

            if not source_label or not target_label:
                prop_info = (
                    ontology_property_lookup.get(predicate_uri) or
                    ontology_property_lookup.get(predicate_label) or
                    {}
                )
                if not source_label:
                    source_label = prop_info.get('domain_label', '')
                if not target_label:
                    target_label = prop_info.get('range_label', '')

            source_local = extract_local_name(source_class)
            source_info = (
                entity_lookup.get(source_class) or
                entity_lookup.get(source_label) or
                entity_lookup.get(source_label.lower() if source_label else '') or
                entity_lookup.get(source_label.replace(' ', '_') if source_label else '') or
                (entity_lookup.get(source_local) if source_local else None) or
                (entity_lookup.get(source_local.lower()) if source_local else None) or
                {
                    'uri_base': f"{base_uri}{source_label.replace(' ', '_') if source_label else 'Entity'}/",
                    'id_column': source_column
                }
            )

            target_local = extract_local_name(target_class)
            target_info = (
                entity_lookup.get(target_class) or
                entity_lookup.get(target_label) or
                entity_lookup.get(target_label.lower() if target_label else '') or
                entity_lookup.get(target_label.replace(' ', '_') if target_label else '') or
                (entity_lookup.get(target_local) if target_local else None) or
                (entity_lookup.get(target_local.lower()) if target_local else None) or
                {
                    'uri_base': f"{base_uri}{target_label.replace(' ', '_') if target_label else 'Entity'}/",
                    'id_column': target_column
                }
            )

            subject_template = source_info['uri_base'] + "{" + source_column + "}"
            object_template = target_info['uri_base'] + "{" + target_column + "}"

            existing_rel = None
            for r in relationship_mappings:
                if r.get('predicate') == predicate_uri and r.get('sql_query') == sql_query:
                    existing_rel = r
                    break

            if existing_rel:
                old_subj = existing_rel.get('subject_template', '')
                old_obj = existing_rel.get('object_template', '')
                if '/Source/' in old_subj or '/Target/' in old_subj or '/Entity/' in old_subj or '/UnknownEntity/' in old_subj:
                    existing_rel['subject_template'] = subject_template
                if '/Source/' in old_obj or '/Target/' in old_obj or '/Entity/' in old_obj or '/UnknownEntity/' in old_obj:
                    existing_rel['object_template'] = object_template
            else:
                relationship_mappings.append({
                    'predicate': predicate_uri,
                    'sql_query': sql_query,
                    'subject_template': subject_template,
                    'object_template': object_template,
                    'subject_column': source_column,
                    'object_column': target_column
                })

        return relationship_mappings

    # ------------------------------------------------------------------
    # Triplestore cache (instance methods -- use self._project)
    # ------------------------------------------------------------------

    def get_ts_cache(self, section: str) -> Optional[dict]:
        """Read a cached triplestore section (e.g. ``'stats'``, ``'status'``) from the project."""
        ts = self._project.triplestore or {}
        stats = ts.get('stats', {})
        if isinstance(stats, dict):
            return stats.get(section)
        return None

    def set_ts_cache(self, section: str, data: dict):
        """Write a cached triplestore section and persist to session."""
        ts = self._project.triplestore
        if 'stats' not in ts:
            ts['stats'] = {}
        ts['stats'][section] = data
        self._project.save()

    async def get_or_fetch_graph_status(self, settings) -> Dict[str, Any]:
        """Return graph triplestore status from session cache, or fetch live and cache."""
        cached = self.get_ts_cache('status')
        if cached:
            logger.debug("get_or_fetch_graph_status: serving from cache")
            return cached
        logger.debug("get_or_fetch_graph_status: cache miss — fetching live")
        result = await self.fetch_graph_triplestore_status(settings)
        if result.get('success'):
            self.set_ts_cache('status', result)
        return result

    async def get_or_fetch_dt_existence(self, settings) -> Dict[str, Any]:
        """Return DT artefact existence from session cache, or fetch live and cache."""
        cached = self.get_ts_cache('dt_existence')
        if cached:
            logger.debug("get_or_fetch_dt_existence: serving from cache")
            return cached
        logger.debug("get_or_fetch_dt_existence: cache miss — fetching live")
        result = await self.fetch_digital_twin_existence(settings)
        self.set_ts_cache('dt_existence', result)
        return result

    # ------------------------------------------------------------------
    # Schedule sync (instance method)
    # ------------------------------------------------------------------

    def sync_last_build_from_schedule(self, settings) -> None:
        """Pull the latest successful scheduled-build timestamp into the session."""
        project = self._project
        try:
            folder = project.project_folder
            if not folder:
                return
            from back.objects.registry import get_scheduler, RegistryCfg
            scheduler = get_scheduler()
            if not scheduler._started:
                return
            from back.core.helpers import get_databricks_host_and_token
            host, token = get_databricks_host_and_token(project, settings)
            registry_cfg = RegistryCfg.from_project(project, settings).as_dict()
            if not host or not registry_cfg.get('catalog'):
                return
            from back.objects.session import global_config_service
            cfg = global_config_service.load(host, token, registry_cfg)
            schedules = cfg.get('schedules') or {}
            sched = schedules.get(folder)
            if not sched:
                return
            if sched.get('last_status') != 'success':
                return
            sched_ts = sched.get('last_run', '')
            if sched_ts and sched_ts > (project.last_build or ''):
                logger.info("Syncing last_build from schedule: %s -> %s", project.last_build or '(empty)', sched_ts)
                project.last_build = sched_ts
                project.save()
        except Exception as exc:
            logger.debug("sync_last_build_from_schedule: %s", exc)

    # ------------------------------------------------------------------
    # Live Digital Twin status (instance methods)
    # ------------------------------------------------------------------

    async def fetch_graph_triplestore_status(self, settings) -> Dict[str, Any]:
        """Live graph backend row count and paths."""
        from back.core.helpers import effective_graph_name, effective_view_table, run_blocking
        from back.core.triplestore import get_triplestore

        project = self._project
        try:
            graph_name = effective_graph_name(project)
            view_table = effective_view_table(project)
            graph_store = get_triplestore(project, settings, backend="graph")
            graph_ok = False
            graph_count = 0
            graph_path = None
            if graph_store:
                try:
                    exists = await run_blocking(graph_store.table_exists, graph_name)
                    if exists:
                        gs = await run_blocking(graph_store.get_status, graph_name)
                        graph_count = int(gs.get("count", 0) or 0)
                        graph_ok = graph_count > 0
                        graph_path = gs.get("path")
                except Exception as e:
                    logger.warning("Graph status check failed: %s", e)

            build_stamp = (project.triplestore or {}).get("build_last_update")
            result: Dict[str, Any] = {
                "success": True,
                "has_data": graph_ok,
                "count": graph_count,
                "view_table": view_table,
                "graph_name": graph_name,
            }
            if build_stamp and graph_ok:
                result["last_modified"] = build_stamp
            if graph_path:
                result["path"] = graph_path
            if not graph_ok:
                result["reason"] = (
                    "Graph does not exist yet" if not graph_count else "Graph is empty"
                )
            return result
        except Exception as e:
            logger.exception("fetch_graph_triplestore_status failed: %s", e)
            return {
                "success": False,
                "has_data": False,
                "count": 0,
                "message": str(e),
            }

    async def fetch_digital_twin_existence(self, settings) -> Dict[str, Any]:
        """Live checks for SQL view, snapshot table, local/registry Ladybug archives."""
        import asyncio
        import os

        from shared.config.constants import DEFAULT_GRAPH_NAME
        from back.core.databricks import DatabricksClient, VolumeFileService
        from back.core.helpers import (
            effective_graph_name,
            effective_view_table,
            get_databricks_host_and_token,
            resolve_warehouse_id,
            run_blocking,
        )
        from back.core.triplestore import get_triplestore
        from back.core.triplestore import IncrementalBuildService
        from back.core.triplestore.ladybugdb import graph_volume_path, local_db_path

        project = self._project
        view_table = effective_view_table(project)
        graph_name = effective_graph_name(project)
        last_built = project.last_build or None
        last_update = project.last_update or None

        snapshot_table = IncrementalBuildService.snapshot_table_name(
            (project.info or {}).get("name", DEFAULT_GRAPH_NAME),
            getattr(project, "delta", None) or {},
            version=getattr(project, "current_version", "1"),
        )

        host, token = get_databricks_host_and_token(project, settings)
        wh_id = resolve_warehouse_id(project, settings)

        db_name = graph_name or DEFAULT_GRAPH_NAME
        lb_cfg = getattr(project, "ladybug", None) or {}
        if not lb_cfg and hasattr(project, "triplestore"):
            lb_cfg = (project.triplestore or {}).get("ladybug", {})
        local_base = lb_cfg.get("db_path", "/tmp/ontobricks")
        local_path = local_db_path(db_name, local_base)

        uc_path = project.uc_project_path
        registry_lbug_path = graph_volume_path(uc_path, db_name) if uc_path else ""

        result: Dict[str, Any] = {
            "view_exists": None,
            "local_lbug_exists": os.path.exists(local_path),
            "registry_lbug_exists": None,
            "view_table": view_table,
            "graph_name": graph_name,
            "local_lbug_path": local_path,
            "registry_lbug_path": registry_lbug_path,
            "last_update": last_update,
            "last_built": last_built,
            "snapshot_table": snapshot_table,
            "snapshot_exists": None,
        }

        async def _check_view() -> Optional[bool]:
            if not (view_table and "." in view_table):
                return None
            try:
                view_store = get_triplestore(project, settings, backend="view")
                if view_store:
                    return await run_blocking(view_store.table_exists, view_table)
            except Exception as e:
                logger.debug("VIEW existence check failed: %s", e)
            return None

        async def _check_snapshot() -> Optional[bool]:
            if not (snapshot_table and "." in snapshot_table):
                return None
            try:
                if host and wh_id:
                    client = DatabricksClient(host=host, token=token, warehouse_id=wh_id)
                    incr_svc = IncrementalBuildService(client)
                    return await run_blocking(incr_svc.snapshot_exists, snapshot_table)
            except Exception as e:
                logger.debug("Snapshot existence check failed: %s", e)
            return None

        async def _check_registry() -> Optional[bool]:
            if not (uc_path and registry_lbug_path):
                return None
            try:
                if host and token:
                    uc = VolumeFileService(host=host, token=token)
                    parent_dir = registry_lbug_path.rsplit("/", 1)[0]
                    archive_name = registry_lbug_path.rsplit("/", 1)[1]
                    ok, items, _ = await run_blocking(
                        uc.list_directory, parent_dir, extensions=[".tar.gz"]
                    )
                    if ok and items:
                        return any(f["name"] == archive_name for f in items)
                    return False
            except Exception as e:
                logger.warning("Registry .lbug existence check failed: %s", e)
            return None

        view_ok, snap_ok, reg_ok = await asyncio.gather(
            _check_view(),
            _check_snapshot(),
            _check_registry(),
        )

        result["view_exists"] = view_ok
        result["snapshot_exists"] = snap_ok
        result["registry_lbug_exists"] = reg_ok

        return result

    # ------------------------------------------------------------------
    # SPARQL execution pipeline (instance method)
    # ------------------------------------------------------------------

    async def execute_spark_query(
        self,
        sparql_query: str,
        r2rml_content: str,
        limit: int,
        settings,
    ) -> Dict[str, Any]:
        """Execute a SPARQL query on Databricks using R2RML mapping."""
        from shared.config.constants import DEFAULT_BASE_URI
        from back.core.w3c import sparql
        from back.core.helpers import get_databricks_client, run_blocking
        import traceback

        project = self._project
        try:
            client = get_databricks_client(project, settings)

            if not client:
                return {
                    'success': False,
                    'message': 'Databricks is not configured. Please configure your Databricks connection in Settings.'
                }

            if not client.warehouse_id:
                return {
                    'success': False,
                    'message': 'No SQL warehouse configured. Please configure your Databricks connection in Settings.'
                }

            if not client.host or not client.warehouse_id:
                missing = []
                if not client.host:
                    missing.append('host')
                if not client.warehouse_id:
                    missing.append('warehouse_id')
                return {'success': False, 'message': f'Databricks configuration incomplete. Missing: {", ".join(missing)}.'}

            if not client.has_valid_auth():
                return {'success': False, 'message': 'Databricks authentication not configured.'}

            entity_mappings, relationship_mappings = sparql.extract_r2rml_mappings(r2rml_content)
            base_uri = project.ontology.get('base_uri', DEFAULT_BASE_URI)

            entity_mappings = DigitalTwin.augment_mappings_from_config(entity_mappings, project.assignment, base_uri, project.ontology)
            relationship_mappings = DigitalTwin.augment_relationships_from_config(relationship_mappings, project.assignment, base_uri, project.ontology)

            if not entity_mappings and not relationship_mappings:
                return {'success': False, 'message': 'No valid R2RML TriplesMap found.'}

            result = sparql.translate_sparql_to_spark(sparql_query, entity_mappings, limit, relationship_mappings)
            if not result['success']:
                return result

            spark_sql = result['sql']
            select_vars = result['variables']

            try:
                results = await run_blocking(client.execute_query, spark_sql)
            except Exception as e:
                logger.exception("Databricks query execution failed: %s", e)
                error_msg = str(e)
                if 'NoneType' in error_msg or 'request' in error_msg:
                    return {
                        'success': False,
                        'message': 'Databricks connection failed. Please verify your configuration.',
                        'generated_sql': spark_sql
                    }
                return {'success': False, 'message': f'Spark SQL execution error: {error_msg}', 'generated_sql': spark_sql}

            if results:
                columns = select_vars if select_vars else list(results[0].keys())
                return {
                    'success': True,
                    'results': results,
                    'columns': columns,
                    'count': len(results),
                    'engine': 'spark',
                    'generated_sql': spark_sql,
                    'tables_queried': list(set(m.get('table', '') for m in entity_mappings.values() if m.get('table')))
                }
            else:
                return {
                    'success': True, 'results': [], 'columns': select_vars,
                    'count': 0, 'engine': 'spark', 'generated_sql': spark_sql
                }

        except ValueError as e:
            logger.exception("Spark query ValueError: %s", e)
            return {'success': False, 'message': str(e)}
        except Exception as e:
            logger.exception("Spark query error: %s", e)
            return {'success': False, 'message': f'Spark query error: {str(e)}', 'traceback': traceback.format_exc()}

    # ------------------------------------------------------------------
    # Triplestore stats (instance method)
    # ------------------------------------------------------------------

    def classify_predicates(self, top_predicates: list) -> list:
        """Classify predicates into 'attribute' or 'relationship' kinds."""
        project = self._project
        attr_predicates = {
            RDF_TYPE, RDFS_LABEL,
            'http://www.w3.org/2000/01/rdf-schema#comment',
            'http://www.w3.org/2000/01/rdf-schema#seeAlso',
        }
        rel_predicates = {'http://www.w3.org/2002/07/owl#sameAs'}

        obj_prop_uris = set()
        data_prop_uris = set()
        for p in project.get_properties():
            p_uri = p.get('uri', '')
            if p.get('type') == 'ObjectProperty':
                obj_prop_uris.add(p_uri)
            else:
                data_prop_uris.add(p_uri)

        classified = []
        for r in top_predicates:
            uri = r['predicate']
            cnt = int(r['cnt'])
            if uri in attr_predicates or uri in data_prop_uris:
                kind = 'attribute'
            elif uri in rel_predicates or uri in obj_prop_uris:
                kind = 'relationship'
            else:
                kind = 'relationship'
            classified.append({'uri': uri, 'count': cnt, 'kind': kind})
        return classified

    # ------------------------------------------------------------------
    # Backend label (instance method)
    # ------------------------------------------------------------------

    def effective_backend_label(self) -> str:
        """Derive a human-readable backend label from the project configuration."""
        project = self._project
        ts = getattr(project, 'triplestore', None) or {}
        backend = ts.get('backend', '')
        if backend:
            return backend
        delta = getattr(project, 'delta', None) or {}
        if delta.get('catalog'):
            return 'Delta (SQL Warehouse)'
        return 'LadybugDB'

    # ------------------------------------------------------------------
    # Data quality: private helpers (static)
    # ------------------------------------------------------------------

    @staticmethod
    def _count_class_population_sql(store, table: str, class_uri: str, cache: dict = None) -> Optional[int]:
        """Count distinct subjects of a given rdf:type class in the triple store."""
        if not class_uri:
            return None
        if cache is None:
            cache = {}
        key = (table, class_uri)
        if key in cache:
            return cache[key]
        try:
            sql = (
                f"SELECT COUNT(DISTINCT subject) AS cnt FROM {table} "
                f"WHERE predicate = '{RDF_TYPE}' AND object = '{escape_sql_value(class_uri)}'"
            )
            rows = store.execute_query(sql) or []
            total = int(rows[0]["cnt"]) if rows else 0
            cache[key] = total
            return total
        except Exception:
            return None

    @staticmethod
    def _enrich_with_population(result: dict, total_population: Optional[int]) -> dict:
        """Add total_population and pass_pct to a check result dict."""
        if total_population is not None and total_population > 0:
            violation_count = len(result.get("violations") or [])
            pass_pct = max(0.0, round(
                ((total_population - violation_count) / total_population) * 100, 1,
            ))
            if violation_count > 0:
                pass_pct = min(pass_pct, 99.9)
            result["total_population"] = total_population
            result["pass_pct"] = pass_pct
            if violation_count > 0:
                result["message"] = (
                    f"{violation_count} violations found — "
                    f"{pass_pct}% pass on {total_population} entities"
                )
        return result

    @staticmethod
    def _load_predicates_from_table(store, table: str) -> set:
        """Query distinct predicates from the triplestore table for URI resolution."""
        try:
            rows = store.execute_query(
                f"SELECT DISTINCT predicate FROM {table}"
            ) or []
            preds = {r.get("predicate", "") for r in rows if r.get("predicate")}
            logger.info("Loaded %d distinct predicates from %s", len(preds), table)
            return preds
        except Exception as exc:
            logger.warning("Could not load predicates from %s: %s", table, exc)
            return set()

    @staticmethod
    def _resolve_shape_uri_for_sql(shape: dict, available_predicates: set) -> dict:
        """Return a shallow copy of *shape* with property_uri resolved against *available_predicates*."""
        from back.core.w3c import resolve_prop_uri

        prop_uri = shape.get("property_uri", "")
        if not prop_uri or not available_predicates:
            return shape

        resolved = resolve_prop_uri(prop_uri, available_predicates)
        if resolved != prop_uri:
            shape = {**shape, "property_uri": resolved}
            logger.info(
                "SQL DQ: resolved property_uri '%s' → '%s' for shape '%s'",
                prop_uri, resolved, shape.get("label", shape.get("id", "?")),
            )
        return shape

    @staticmethod
    def _count_class_population_graph(triples: list, class_uri: str, cache: dict = None) -> Optional[int]:
        """Count distinct subjects of a given rdf:type class from in-memory triples."""
        if not class_uri:
            return None
        if cache is None:
            cache = {}
        if class_uri in cache:
            return cache[class_uri]
        subjects = set()
        for t in triples:
            if isinstance(t, dict):
                s, p, o = t.get("subject", ""), t.get("predicate", ""), t.get("object", "")
            else:
                s, p, o = t
            if p == RDF_TYPE and o == class_uri:
                subjects.add(s)
        total = len(subjects)
        cache[class_uri] = total
        return total

    @staticmethod
    def _swrl_target_class_uri(rule, base_uri, uri_map):
        """Return the class URI of the SWRL violation subject."""
        from back.core.reasoning.SWRLParser import SWRLParser

        ante_atoms = SWRLParser.parse_atoms(rule.get("antecedent", ""))
        cons_atoms = SWRLParser.parse_atoms(rule.get("consequent", ""))
        class_atoms = [a for a in ante_atoms
                       if a["arity"] == 1 and not a.get("builtin") and not a.get("negated")]
        if not class_atoms:
            return None

        viol_var = SWRLParser.determine_violation_subject(cons_atoms, class_atoms)
        for ca in class_atoms:
            if ca["args"][0] == viol_var:
                return SWRLParser.resolve_uri(ca["name"], base_uri, uri_map)
        return SWRLParser.resolve_uri(class_atoms[0]["name"], base_uri, uri_map)

    @staticmethod
    def _swrl_antecedent_population_sql(translator, store, table, params):
        """Count entities matching the SWRL antecedent (the rule's scope)."""
        try:
            count_sql = translator.build_antecedent_count_sql(table, params)
            if not count_sql:
                return None
            rows = store.execute_query(count_sql) or []
            return int(rows[0]["cnt"]) if rows else None
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Data quality: SQL checks (static -- runs in background thread)
    # ------------------------------------------------------------------

    @staticmethod
    def run_sql_checks(tm, task, shapes, triplestore_table, store, t0, total,
                       swrl_rules=None, ontology=None, decision_tables=None,
                       aggregate_rules=None):
        """Execute SHACL shapes, SWRL, decision tables and aggregate rules as SQL against the VIEW backend."""
        import time
        from back.core.w3c import SHACLService

        available_predicates = DigitalTwin._load_predicates_from_table(store, triplestore_table)

        pop_cache = {}
        results = []
        for idx, shape in enumerate(shapes):
            label = shape.get("label", shape.get("id", f"Shape {idx + 1}"))
            cat = shape.get("category", "unknown")
            progress = int((idx / total) * 100)
            tm.update_progress(task.id, progress, f"Check {idx + 1}/{total}: {label}")

            resolved_shape = DigitalTwin._resolve_shape_uri_for_sql(shape, available_predicates)
            sql = SHACLService.shape_to_sql(resolved_shape, triplestore_table)
            if not sql:
                results.append({
                    "name": label, "category": cat, "shape_id": shape.get("id"),
                    "status": "info", "message": "Cannot translate to SQL",
                    "violations": [], "sql": "",
                })
                continue

            try:
                violations = store.execute_query(sql) or []
                status = "error" if violations else "success"
                msg = f"{len(violations)} violations found" if violations else "No violations"
                result = {
                    "name": label, "category": cat, "shape_id": shape.get("id"),
                    "status": status, "message": msg,
                    "violations": violations, "sql": sql,
                    "severity": shape.get("severity", "sh:Violation"),
                }
                class_uri = shape.get("target_class_uri", "")
                pop = DigitalTwin._count_class_population_sql(store, triplestore_table, class_uri, pop_cache)
                DigitalTwin._enrich_with_population(result, pop)
                results.append(result)
            except Exception as exc:
                err = str(exc)
                if "TABLE_OR_VIEW_NOT_FOUND" in err or "does not exist" in err.lower():
                    tm.fail_task(task.id, f"View {triplestore_table} not found. Build first.")
                    return
                results.append({
                    "name": label, "category": cat, "shape_id": shape.get("id"),
                    "status": "warning", "message": f"Query error: {err}",
                    "violations": [], "sql": sql,
                })

        DigitalTwin._run_swrl_sql_checks(tm, task, results, swrl_rules, ontology,
                                         triplestore_table, store, total)

        swrl_count = len(swrl_rules) if swrl_rules else 0
        dt_count = len(decision_tables) if decision_tables else 0
        dt_offset = len(shapes) + swrl_count
        DigitalTwin._run_dt_sql_checks(tm, task, results, decision_tables, ontology,
                                       triplestore_table, store, total, dt_offset)

        agg_offset = dt_offset + dt_count
        DigitalTwin._run_agg_sql_checks(tm, task, results, aggregate_rules, ontology,
                                        triplestore_table, store, total, agg_offset)

        DigitalTwin.complete_dq_task(tm, task, results, time.time() - t0)

    @staticmethod
    def _run_swrl_sql_checks(tm, task, results, swrl_rules, ontology,
                             triplestore_table, store, total):
        if not swrl_rules:
            return
        from back.core.reasoning.SWRLSQLTranslator import SWRLSQLTranslator
        from back.core.reasoning.SWRLEngine import SWRLEngine
        translator = SWRLSQLTranslator()
        ontology = ontology or {}
        base_uri = ontology.get("base_uri", "")
        engine = SWRLEngine(ontology=ontology)
        uri_map = engine._build_uri_map()
        shape_count = total - len(swrl_rules)
        for idx, rule in enumerate(swrl_rules):
            if not rule.get("enabled", True):
                continue
            label = rule.get("name", f"SWRL Rule {idx + 1}")
            progress = int(((shape_count + idx) / total) * 100)
            tm.update_progress(task.id, progress, f"SWRL {idx + 1}/{len(swrl_rules)}: {label}")
            params = {"antecedent": rule.get("antecedent", ""), "consequent": rule.get("consequent", ""), "base_uri": base_uri, "uri_map": uri_map}
            sql = translator.build_violation_sql(triplestore_table, params)
            if not sql:
                results.append({"name": label, "category": "structural", "shape_id": f"swrl:{rule.get('name', idx)}", "status": "info", "message": "Cannot translate to SQL", "violations": [], "sql": ""})
                continue
            try:
                rows = store.execute_query(sql) or []
                violations = [{"s": r.get("s", "")} for r in rows]
                status = "error" if violations else "success"
                msg = f"{len(violations)} violations found" if violations else "No violations"
                result = {"name": label, "category": "structural", "shape_id": f"swrl:{rule.get('name', idx)}", "status": status, "message": msg, "violations": violations, "sql": "", "severity": "sh:Violation"}
                pop = DigitalTwin._swrl_antecedent_population_sql(translator, store, triplestore_table, params)
                DigitalTwin._enrich_with_population(result, pop)
                results.append(result)
            except Exception as exc:
                logger.warning("SWRL DQ check '%s' SQL failed: %s", label, exc)
                results.append({"name": label, "category": "structural", "shape_id": f"swrl:{rule.get('name', idx)}", "status": "warning", "message": f"Query error: {exc}", "violations": [], "sql": ""})

    @staticmethod
    def _run_dt_sql_checks(tm, task, results, decision_tables, ontology, triplestore_table, store, total, shape_count):
        if not decision_tables:
            return
        from back.core.reasoning.DecisionTableEngine import DecisionTableEngine
        engine = DecisionTableEngine()
        ontology = ontology or {}
        base_uri = ontology.get("base_uri", "")
        uri_map = engine._build_uri_map(ontology)
        for idx, dt in enumerate(decision_tables):
            if not dt.get("enabled", True):
                continue
            dt_name = dt.get("name", f"Decision Table {idx + 1}")
            progress = int(((shape_count + idx) / total) * 100)
            tm.update_progress(task.id, progress, f"DT {idx + 1}/{len(decision_tables)}: {dt_name}")
            resolved = engine._resolve_dt(dt, uri_map, base_uri)
            sql = engine.build_violation_sql(resolved, triplestore_table, base_uri)
            if not sql:
                results.append({"name": dt_name, "category": "conformance", "shape_id": f"dt:{dt.get('name', idx)}", "status": "info", "message": "Cannot translate to SQL", "violations": [], "sql": ""})
                continue
            try:
                rows = store.execute_query(sql) or []
                violations = [{"s": r.get("s", "")} for r in rows]
                status = "error" if violations else "success"
                msg = f"{len(violations)} violations found" if violations else "No violations"
                result = {"name": dt_name, "category": "conformance", "shape_id": f"dt:{dt.get('name', idx)}", "status": status, "message": msg, "violations": violations, "sql": sql, "severity": "sh:Violation"}
                class_uri = resolved.get("target_class_uri", "")
                pop_cache: dict = {}
                pop = DigitalTwin._count_class_population_sql(store, triplestore_table, class_uri, pop_cache)
                DigitalTwin._enrich_with_population(result, pop)
                results.append(result)
            except Exception as exc:
                logger.warning("Decision table DQ check '%s' SQL failed: %s", dt_name, exc)
                results.append({"name": dt_name, "category": "conformance", "shape_id": f"dt:{dt.get('name', idx)}", "status": "warning", "message": f"Query error: {exc}", "violations": [], "sql": ""})

    @staticmethod
    def _run_agg_sql_checks(tm, task, results, aggregate_rules, ontology, triplestore_table, store, total, shape_count):
        if not aggregate_rules:
            return
        from back.core.reasoning.AggregateRuleEngine import AggregateRuleEngine
        engine = AggregateRuleEngine()
        ontology = ontology or {}
        base_uri = ontology.get("base_uri", "")
        pop_cache: dict = {}
        for idx, rule in enumerate(aggregate_rules):
            if not rule.get("enabled", True):
                continue
            agg_name = rule.get("name", f"Aggregate Rule {idx + 1}")
            progress = int(((shape_count + idx) / total) * 100)
            tm.update_progress(task.id, progress, f"Agg {idx + 1}/{len(aggregate_rules)}: {agg_name}")
            resolved = engine._resolve_rule(dict(rule), ontology)
            sql = engine.build_sql(resolved, triplestore_table, base_uri)
            if not sql:
                results.append({"name": agg_name, "category": "conformance", "shape_id": f"agg:{rule.get('name', idx)}", "status": "info", "message": "Cannot translate to SQL", "violations": [], "sql": ""})
                continue
            try:
                rows = store.execute_query(sql) or []
                violations = [{"s": r.get("s", ""), "agg_val": r.get("agg_val", "")} for r in rows]
                status = "error" if violations else "success"
                msg = f"{len(violations)} violations found" if violations else "No violations"
                result = {"name": agg_name, "category": "conformance", "shape_id": f"agg:{rule.get('name', idx)}", "status": status, "message": msg, "violations": violations, "sql": sql, "severity": "sh:Violation"}
                class_uri = resolved.get("target_class_uri", "")
                pop = DigitalTwin._count_class_population_sql(store, triplestore_table, class_uri, pop_cache)
                DigitalTwin._enrich_with_population(result, pop)
                results.append(result)
            except Exception as exc:
                logger.warning("Aggregate rule DQ check '%s' SQL failed: %s", agg_name, exc)
                results.append({"name": agg_name, "category": "conformance", "shape_id": f"agg:{rule.get('name', idx)}", "status": "warning", "message": f"Query error: {exc}", "violations": [], "sql": ""})

    # ------------------------------------------------------------------
    # Data quality: Graph checks (static -- runs in background thread)
    # ------------------------------------------------------------------

    @staticmethod
    def run_graph_checks(tm, task, shapes, store, graph_name, proj_snap, t0, total,
                         swrl_rules=None, ontology=None, decision_tables=None, aggregate_rules=None):
        """Execute SHACL shapes, SWRL, decision tables and aggregate rules against the LadybugDB graph."""
        import time
        from back.core.w3c import SHACLService
        tm.update_progress(task.id, 5, "Loading triples from graph...")
        try:
            triples = store.query_triples(graph_name)
        except Exception as exc:
            err = str(exc)
            if "does not exist" in err.lower():
                tm.fail_task(task.id, f"Graph '{graph_name}' does not exist. Run Build first.")
            else:
                tm.fail_task(task.id, f"Error reading graph: {err}")
            return
        if not triples:
            tm.fail_task(task.id, f"Graph '{graph_name}' is empty. Run Build first.")
            return
        predicates_in_graph = {t.get("predicate", "") for t in triples if t.get("predicate")}
        logger.info("Graph DQ: loaded %d triples from '%s' — %d distinct predicates", len(triples), graph_name, len(predicates_in_graph))
        logger.debug("Graph predicates: %s", sorted(predicates_in_graph))
        tm.update_progress(task.id, 15, f"Loaded {len(triples)} triples, evaluating shapes...")
        pop_cache = {}
        results = []
        for idx, shape in enumerate(shapes):
            label = shape.get("label", shape.get("id", f"Shape {idx + 1}"))
            cat = shape.get("category", "unknown")
            progress = 15 + int((idx / total) * 80)
            tm.update_progress(task.id, progress, f"Check {idx + 1}/{total}: {label}")
            logger.info("DQ shape '%s': type=%s, class_uri='%s', prop_uri='%s', params=%s", label, shape.get("shacl_type", ""), shape.get("target_class_uri", ""), shape.get("property_uri", ""), shape.get("parameters", {}))
            try:
                violations = SHACLService.evaluate_shape_in_memory(shape, triples)
                status = "error" if violations else "success"
                msg = f"{len(violations)} violations found" if violations else "No violations"
                result = {"name": label, "category": cat, "shape_id": shape.get("id"), "status": status, "message": msg, "violations": violations, "sql": "", "severity": shape.get("severity", "sh:Violation")}
                class_uri = shape.get("target_class_uri", "")
                pop = DigitalTwin._count_class_population_graph(triples, class_uri, pop_cache)
                logger.info("DQ shape '%s': violations=%d, population=%s", label, len(violations), pop)
                DigitalTwin._enrich_with_population(result, pop)
                results.append(result)
            except Exception as exc:
                logger.warning("Graph DQ check '%s' failed: %s", label, exc)
                results.append({"name": label, "category": cat, "shape_id": shape.get("id"), "status": "warning", "message": f"Evaluation error: {exc}", "violations": [], "sql": ""})
        DigitalTwin._run_swrl_graph_checks(tm, task, results, swrl_rules, ontology, store, graph_name, total, triples, pop_cache)
        swrl_count = len(swrl_rules) if swrl_rules else 0
        dt_count = len(decision_tables) if decision_tables else 0
        dt_offset = len(shapes) + swrl_count
        DigitalTwin._run_dt_graph_checks(tm, task, results, decision_tables, ontology, store, graph_name, total, dt_offset, triples, pop_cache)
        agg_offset = dt_offset + dt_count
        DigitalTwin._run_agg_graph_checks(tm, task, results, aggregate_rules, ontology, store, graph_name, total, agg_offset, triples, pop_cache)
        DigitalTwin.complete_dq_task(tm, task, results, time.time() - t0)

    @staticmethod
    def _run_swrl_graph_checks(tm, task, results, swrl_rules, ontology, store, graph_name, total, triples=None, pop_cache=None):
        if not swrl_rules:
            return
        from back.core.reasoning.SWRLEngine import SWRLEngine
        ontology = ontology or {}
        engine = SWRLEngine(ontology=ontology)
        translator = engine._get_translator(store, graph_name)
        base_uri = ontology.get("base_uri", "")
        uri_map = engine._build_uri_map()
        shape_count = total - len(swrl_rules)
        if pop_cache is None:
            pop_cache = {}
        for idx, rule in enumerate(swrl_rules):
            if not rule.get("enabled", True):
                continue
            label = rule.get("name", f"SWRL Rule {idx + 1}")
            progress = 15 + int(((shape_count + idx) / total) * 80)
            tm.update_progress(task.id, progress, f"SWRL {idx + 1}/{len(swrl_rules)}: {label}")
            params = {"antecedent": rule.get("antecedent", ""), "consequent": rule.get("consequent", ""), "base_uri": base_uri, "uri_map": uri_map}
            query = translator.build_violation_query(params)
            if not query:
                results.append({"name": label, "category": "structural", "shape_id": f"swrl:{rule.get('name', idx)}", "status": "info", "message": "Cannot translate to Cypher", "violations": [], "sql": ""})
                continue
            try:
                conn = store._get_connection()
                rows = conn.execute(query)
                violations = [{"s": str(row[0])} for row in rows]
                status = "error" if violations else "success"
                msg = f"{len(violations)} violations found" if violations else "No violations"
                result = {"name": label, "category": "structural", "shape_id": f"swrl:{rule.get('name', idx)}", "status": status, "message": msg, "violations": violations, "sql": "", "severity": "sh:Violation"}
                class_uri = DigitalTwin._swrl_target_class_uri(rule, base_uri, uri_map)
                pop = None
                if triples is not None and class_uri:
                    pop = DigitalTwin._count_class_population_graph(triples, class_uri, pop_cache)
                if pop is not None and pop < len(violations):
                    pop = None
                DigitalTwin._enrich_with_population(result, pop)
                results.append(result)
            except Exception as exc:
                logger.warning("SWRL DQ check '%s' graph failed: %s", label, exc)
                results.append({"name": label, "category": "structural", "shape_id": f"swrl:{rule.get('name', idx)}", "status": "warning", "message": f"Query error: {exc}", "violations": [], "sql": ""})

    @staticmethod
    def _run_dt_graph_checks(tm, task, results, decision_tables, ontology, store, graph_name, total, shape_count, triples=None, pop_cache=None):
        if not decision_tables:
            return
        from back.core.reasoning.DecisionTableEngine import DecisionTableEngine
        engine = DecisionTableEngine()
        ontology = ontology or {}
        base_uri = ontology.get("base_uri", "")
        uri_map = engine._build_uri_map(ontology)
        if pop_cache is None:
            pop_cache = {}
        for idx, dt in enumerate(decision_tables):
            if not dt.get("enabled", True):
                continue
            dt_name = dt.get("name", f"Decision Table {idx + 1}")
            progress = 15 + int(((shape_count + idx) / total) * 80)
            tm.update_progress(task.id, progress, f"DT {idx + 1}/{len(decision_tables)}: {dt_name}")
            resolved = engine._resolve_dt(dt, uri_map, base_uri)
            query = engine.build_violation_cypher(resolved, graph_name, base_uri)
            if not query:
                results.append({"name": dt_name, "category": "conformance", "shape_id": f"dt:{dt.get('name', idx)}", "status": "info", "message": "Cannot translate to Cypher", "violations": [], "sql": ""})
                continue
            try:
                conn = store._get_connection()
                rows = conn.execute(query)
                violations = [{"s": str(row[0])} for row in rows]
                status = "error" if violations else "success"
                msg = f"{len(violations)} violations found" if violations else "No violations"
                result = {"name": dt_name, "category": "conformance", "shape_id": f"dt:{dt.get('name', idx)}", "status": status, "message": msg, "violations": violations, "sql": "", "severity": "sh:Violation"}
                class_uri = resolved.get("target_class_uri", "")
                pop = None
                if triples is not None and class_uri:
                    pop = DigitalTwin._count_class_population_graph(triples, class_uri, pop_cache)
                if pop is not None and pop < len(violations):
                    pop = None
                DigitalTwin._enrich_with_population(result, pop)
                results.append(result)
            except Exception as exc:
                logger.warning("Decision table DQ check '%s' graph failed: %s", dt_name, exc)
                results.append({"name": dt_name, "category": "conformance", "shape_id": f"dt:{dt.get('name', idx)}", "status": "warning", "message": f"Query error: {exc}", "violations": [], "sql": ""})

    @staticmethod
    def _run_agg_graph_checks(tm, task, results, aggregate_rules, ontology, store, graph_name, total, shape_count, triples=None, pop_cache=None):
        if not aggregate_rules:
            return
        from back.core.reasoning.AggregateRuleEngine import AggregateRuleEngine
        engine = AggregateRuleEngine()
        ontology = ontology or {}
        base_uri = ontology.get("base_uri", "")
        if pop_cache is None:
            pop_cache = {}
        for idx, rule in enumerate(aggregate_rules):
            if not rule.get("enabled", True):
                continue
            agg_name = rule.get("name", f"Aggregate Rule {idx + 1}")
            progress = 15 + int(((shape_count + idx) / total) * 80)
            tm.update_progress(task.id, progress, f"Agg {idx + 1}/{len(aggregate_rules)}: {agg_name}")
            resolved = engine._resolve_rule(dict(rule), ontology)
            query = engine.build_cypher(resolved, graph_name, base_uri)
            if not query:
                results.append({"name": agg_name, "category": "conformance", "shape_id": f"agg:{rule.get('name', idx)}", "status": "info", "message": "Cannot translate to Cypher", "violations": [], "sql": ""})
                continue
            try:
                conn = store._get_connection()
                rows = conn.execute(query)
                violations = [{"s": str(row[0]), "agg_val": str(row[1]) if len(row) > 1 else ""} for row in rows]
                status = "error" if violations else "success"
                msg = f"{len(violations)} violations found" if violations else "No violations"
                result = {"name": agg_name, "category": "conformance", "shape_id": f"agg:{rule.get('name', idx)}", "status": status, "message": msg, "violations": violations, "sql": "", "severity": "sh:Violation"}
                class_uri = resolved.get("target_class_uri", "")
                pop = None
                if triples is not None and class_uri:
                    pop = DigitalTwin._count_class_population_graph(triples, class_uri, pop_cache)
                if pop is not None and pop < len(violations):
                    pop = None
                DigitalTwin._enrich_with_population(result, pop)
                results.append(result)
            except Exception as exc:
                logger.warning("Aggregate rule DQ check '%s' graph failed: %s", agg_name, exc)
                results.append({"name": agg_name, "category": "conformance", "shape_id": f"agg:{rule.get('name', idx)}", "status": "warning", "message": f"Query error: {exc}", "violations": [], "sql": ""})

    # ------------------------------------------------------------------
    # Data quality: task completion (static)
    # ------------------------------------------------------------------

    @staticmethod
    def complete_dq_task(tm, task, results, duration):
        """Finalize a data quality task with summary counts."""
        passed = sum(1 for r in results if r["status"] == "success")
        failed = sum(1 for r in results if r["status"] == "error")
        warnings = sum(1 for r in results if r["status"] in ("warning", "info"))
        tm.complete_task(task.id, result={
            "results": results,
            "summary": {"total": len(results), "passed": passed, "failed": failed, "warnings": warnings},
            "duration_seconds": round(duration, 1),
        }, message=f"Data quality checks complete: {passed} passed, {failed} failed, {warnings} warnings")

    # ------------------------------------------------------------------
    # Legacy quality SQL builders (static)
    # ------------------------------------------------------------------

    @staticmethod
    def build_quality_sql(check_type: str, table: str, params: dict) -> Optional[str]:
        """Build SQL for a quality check against the triple store table."""
        if check_type == 'cardinality':
            return DigitalTwin._build_cardinality_sql(table, params)
        elif check_type == 'value':
            return DigitalTwin._build_value_sql(table, params)
        elif check_type in ('functional', 'inverseFunctional', 'symmetric', 'asymmetric', 'irreflexive'):
            return DigitalTwin._build_property_sql(table, check_type, params)
        elif check_type == 'requireLabels':
            return DigitalTwin._build_require_labels_sql(table, params)
        elif check_type == 'noOrphans':
            return DigitalTwin._build_no_orphans_sql(table, params)
        elif check_type == 'swrl':
            return DigitalTwin._build_swrl_sql(table, params)
        else:
            return None

    @staticmethod
    def _build_cardinality_sql(table, params):
        class_uri = escape_sql_value(params.get('class_uri', ''))
        property_uri = escape_sql_value(params.get('property_uri', ''))
        constraint_type = params.get('constraint_type', '')
        cardinality_value = int(params.get('cardinality_value', 0))
        if not class_uri or not property_uri:
            return None
        if constraint_type == 'minCardinality':
            having = f"HAVING COUNT(t2.object) < {cardinality_value}"
        elif constraint_type == 'maxCardinality':
            having = f"HAVING COUNT(t2.object) > {cardinality_value}"
        elif constraint_type == 'exactCardinality':
            having = f"HAVING COUNT(t2.object) != {cardinality_value}"
        else:
            return None
        return f"SELECT t1.subject AS s, COUNT(t2.object) AS count\nFROM {table} t1\nJOIN {table} t2\n  ON t1.subject = t2.subject\n  AND t2.predicate = '{property_uri}'\nWHERE t1.predicate = '{RDF_TYPE}'\n  AND t1.object = '{class_uri}'\nGROUP BY t1.subject\n{having}"

    @staticmethod
    def _build_value_sql(table, params):
        class_uri = escape_sql_value(params.get('class_uri', ''))
        attribute_uri = escape_sql_value(params.get('attribute_uri', ''))
        value_check_type = params.get('value_check_type', '')
        check_value = escape_sql_value(params.get('check_value', ''))
        if not class_uri or not attribute_uri:
            return None
        if value_check_type == 'notNull':
            return f"SELECT t1.subject AS s\nFROM {table} t1\nLEFT JOIN {table} t2\n  ON t1.subject = t2.subject\n  AND t2.predicate = '{attribute_uri}'\nWHERE t1.predicate = '{RDF_TYPE}'\n  AND t1.object = '{class_uri}'\n  AND t2.subject IS NULL"
        filter_clause = ''
        if value_check_type == 'startsWith':
            filter_clause = f"AND NOT LOWER(t2.object) LIKE LOWER('{check_value}%')"
        elif value_check_type == 'endsWith':
            filter_clause = f"AND NOT LOWER(t2.object) LIKE LOWER('%{check_value}')"
        elif value_check_type == 'contains':
            filter_clause = f"AND NOT LOWER(t2.object) LIKE LOWER('%{check_value}%')"
        elif value_check_type == 'equals':
            filter_clause = f"AND LOWER(t2.object) != LOWER('{check_value}')"
        elif value_check_type == 'notEquals':
            filter_clause = f"AND LOWER(t2.object) = LOWER('{check_value}')"
        elif value_check_type == 'matches':
            filter_clause = f"AND NOT t2.object RLIKE '{check_value}'"
        return f"SELECT t1.subject AS s, t2.object AS val\nFROM {table} t1\nJOIN {table} t2\n  ON t1.subject = t2.subject\n  AND t2.predicate = '{attribute_uri}'\nWHERE t1.predicate = '{RDF_TYPE}'\n  AND t1.object = '{class_uri}'\n  {filter_clause}"

    @staticmethod
    def _build_property_sql(table, check_type, params):
        property_uri = escape_sql_value(params.get('property_uri', ''))
        if not property_uri:
            return None
        if check_type == 'functional':
            return f"SELECT subject AS s, COUNT(object) AS count\nFROM {table}\nWHERE predicate = '{property_uri}'\nGROUP BY subject\nHAVING COUNT(object) > 1"
        elif check_type == 'inverseFunctional':
            return f"SELECT object AS o, COUNT(subject) AS count\nFROM {table}\nWHERE predicate = '{property_uri}'\nGROUP BY object\nHAVING COUNT(subject) > 1"
        elif check_type == 'symmetric':
            return f"SELECT t1.subject AS s, t1.object AS o\nFROM {table} t1\nLEFT JOIN {table} t2\n  ON t1.subject = t2.object\n  AND t1.object = t2.subject\n  AND t2.predicate = '{property_uri}'\nWHERE t1.predicate = '{property_uri}'\n  AND t2.subject IS NULL"
        elif check_type == 'asymmetric':
            return f"SELECT t1.subject AS s, t1.object AS o\nFROM {table} t1\nJOIN {table} t2\n  ON t1.subject = t2.object\n  AND t1.object = t2.subject\n  AND t2.predicate = '{property_uri}'\nWHERE t1.predicate = '{property_uri}'"
        elif check_type == 'irreflexive':
            return f"SELECT subject AS s\nFROM {table}\nWHERE predicate = '{property_uri}'\n  AND subject = object"
        return None

    @staticmethod
    def _build_require_labels_sql(table, params):
        return f"SELECT t1.subject AS s\nFROM {table} t1\nLEFT JOIN {table} t2\n  ON t1.subject = t2.subject\n  AND t2.predicate = '{RDFS_LABEL}'\nWHERE t1.predicate = '{RDF_TYPE}'\n  AND t2.subject IS NULL"

    @staticmethod
    def _build_no_orphans_sql(table, params):
        return f"SELECT t1.subject AS s\nFROM {table} t1\nWHERE t1.predicate = '{RDF_TYPE}'\n  AND NOT EXISTS (\n    SELECT 1 FROM {table} t2\n    WHERE t2.subject = t1.subject\n      AND t2.predicate != '{RDF_TYPE}'\n      AND t2.predicate != '{RDFS_LABEL}'\n  )"

    _swrl_sql_translator = None

    @staticmethod
    def _get_swrl_translator():
        if DigitalTwin._swrl_sql_translator is None:
            from back.core.reasoning import SWRLSQLTranslator
            DigitalTwin._swrl_sql_translator = SWRLSQLTranslator()
        return DigitalTwin._swrl_sql_translator

    @staticmethod
    def _build_swrl_sql(table, params):
        return DigitalTwin._get_swrl_translator().build_violation_sql(table, params)

    # ------------------------------------------------------------------
    # Registry / project resolution (static -- API helpers)
    # ------------------------------------------------------------------

    @staticmethod
    def resolve_registry(session_mgr, settings, registry_catalog=None, registry_schema=None, registry_volume=None):
        """Resolve registry location: explicit query params -> session -> env."""
        from back.objects.registry import RegistryCfg
        base = RegistryCfg.from_session(session_mgr, settings)
        return {"catalog": registry_catalog or base.catalog, "schema": registry_schema or base.schema, "volume": registry_volume or base.volume}

    @staticmethod
    def resolve_project(project_name, session_mgr, settings, registry_catalog=None, registry_schema=None, registry_volume=None, project_version=None):
        """Return the project to operate on; optionally load from registry by name/version."""
        from back.objects.registry import RegistryCfg, RegistryService
        project = get_project(session_mgr)
        if not project_name:
            return project
        reg = DigitalTwin.resolve_registry(session_mgr, settings, registry_catalog, registry_schema, registry_volume)
        cfg = RegistryCfg.from_dict(reg)
        if not cfg.is_configured:
            raise ValidationError("Registry not configured — cannot resolve project_name")
        svc = RegistryService(cfg, DigitalTwin.uc_from_project(project, settings))
        if project_version:
            ok, data, msg = svc.read_version(project_name, project_version)
            if not ok:
                if "not found" in msg.lower():
                    raise NotFoundError(msg)
                raise InfrastructureError(msg)
            version = project_version
        else:
            ok, data, version, err = svc.load_mcp_project_data(project_name)
            if not ok:
                if "not found" in err.lower() or "no versions" in err.lower():
                    raise NotFoundError(err)
                raise InfrastructureError(err)
        project.clear_generated_content()
        project.import_from_file(data, version=version)
        project.project_folder = project_name
        project.ensure_generated_content()
        project.save()
        logger.info("DigitalTwin: loaded project '%s' version %s from registry", project_name, version)
        return project

    @staticmethod
    def uc_from_project(project, settings):
        """Build a VolumeFileService from project credentials."""
        from back.core.databricks import VolumeFileService
        from back.core.helpers import get_databricks_host_and_token
        host, token = get_databricks_host_and_token(project, settings)
        return VolumeFileService(host=host, token=token)

    # ------------------------------------------------------------------
    # Misc utilities (static)
    # ------------------------------------------------------------------

    @staticmethod
    def is_datatype_range(range_val: str) -> bool:
        """Return True if a property range looks like a datatype (not an object property)."""
        low = range_val.lower()
        return any(kw in low for kw in ("xsd:", "string", "integer", "decimal", "date", "boolean", "float", "double", "time", "long", "int", "short", "byte"))

    @staticmethod
    def make_snapshot(project):
        """Create a lightweight snapshot of project state for background threads."""
        from back.objects.digitaltwin.models import ProjectSnapshot
        return ProjectSnapshot(project)

    @staticmethod
    def extract_local_id(uri: str) -> str:
        """Extract the local entity identifier from a URI."""
        from back.core.helpers import extract_local_name
        return extract_local_name(uri) or uri

    @staticmethod
    def expand_uri_aliases(store, table_name: str, uris: Set[str]) -> Set[str]:
        """Find alternate URI forms for a set of entity URIs."""
        if not uris:
            return uris
        local_ids = {DigitalTwin.extract_local_id(u) for u in uris}
        local_ids.discard("")
        if not local_ids:
            return uris
        patterns = [f"%/{lid}" for lid in local_ids]
        expanded = set(uris) | store.find_subjects_by_patterns(table_name, patterns)
        return expanded

    @staticmethod
    def is_owlrl_available() -> bool:
        """Check whether the ``owlrl`` reasoning library is importable."""
        try:
            import owlrl as _owlrl  # noqa: F401
            return True
        except ImportError:
            return False

    @staticmethod
    def compute_dtwin_indicator(
        project: Any,
        ts_status: Dict[str, Any],
        dt_exist: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Derive a three-state Digital Twin indicator from live graph and artefact checks.

        Returns a dict with:
            indicator: ``'green'`` | ``'orange'`` | ``'red'``
            title:     tooltip text for the navbar
            count:     triple count (0 when unknown)
            pending:   ``True`` when no status has been fetched yet
        """
        if not ts_status and not dt_exist:
            if not project.last_build:
                return {"indicator": "red", "title": "Digital Twin never built",
                        "count": 0, "pending": False}
            return {"indicator": "orange", "title": "Digital Twin status not yet checked",
                    "count": 0, "pending": True}

        graph_loaded = bool(
            ts_status and ts_status.get("has_data") and ts_status.get("count", 0) > 0
        )
        count = (ts_status or {}).get("count", 0)

        view_exists = (dt_exist or {}).get("view_exists")
        archive_exists = (dt_exist or {}).get("registry_lbug_exists")

        if graph_loaded and view_exists is not False:
            return {"indicator": "green",
                    "title": f"Digital Twin active — {count:,} triples",
                    "count": count, "pending": False}

        if not project.last_build and not graph_loaded and not view_exists and not archive_exists:
            return {"indicator": "red", "title": "Digital Twin never built",
                    "count": 0, "pending": False}

        parts = []
        if view_exists is False:
            parts.append("view missing")
        if not graph_loaded:
            parts.append("graph not loaded")
        title = "Digital Twin incomplete — " + ", ".join(parts) if parts else "Digital Twin partially available"
        return {"indicator": "orange", "title": title,
                "count": count, "pending": False}
