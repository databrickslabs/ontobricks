"""Domain logic for entity/relationship mappings, R2RML, and mapping wizard helpers."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

import requests

from shared.config.settings import get_settings
from shared.config.constants import DEFAULT_BASE_URI
from back.core.databricks import VolumeFileService
from back.core.logging import get_logger
from back.core.errors import InfrastructureError

logger = get_logger(__name__)

_MAX_DOC_CHARS = 50_000

if TYPE_CHECKING:
    from agents.agent_auto_assignment.engine import AgentResult as AutoAssignAgentResult


class Mapping:
    """Centralizes mapping operations for a project session."""

    def __init__(self, project: Any) -> None:
        self._project = project

    def auto_assign_with_agent(
        self,
        *,
        host: str,
        token: str,
        endpoint_name: str,
        client: Any,
        metadata: dict,
        ontology: dict,
        entity_mappings: Optional[list] = None,
        relationship_mappings: Optional[list] = None,
        documents: Optional[list] = None,
        on_step: Optional[Callable[[str, int], None]] = None,
        max_iterations: Optional[int] = None,
    ) -> "AutoAssignAgentResult":
        """Run ``agent_auto_assignment`` (blocking).

        ``client`` is typically a :class:`~back.core.databricks.DatabricksClient`
        built with the project warehouse. Call from a background thread when
        started from HTTP.
        """
        from agents.agent_auto_assignment import run_agent

        return run_agent(
            host=host,
            token=token,
            endpoint_name=endpoint_name,
            client=client,
            metadata=metadata,
            ontology=ontology,
            entity_mappings=entity_mappings,
            relationship_mappings=relationship_mappings,
            documents=documents,
            on_step=on_step,
            max_iterations=max_iterations,
        )

    def resolve_auto_assign_schema_context(
        self, schema_context_override: Optional[Dict[str, Any]] = None
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        """Build ``metadata``/schema payload for auto-assignment.

        If ``schema_context_override`` contains ``tables``, it is used; otherwise
        falls back to ``catalog_metadata`` tables.

        Returns ``(context, None)`` on success, or ``({}, error_message)`` when
        no tables are available.
        """
        override = schema_context_override or {}
        if override.get("tables"):
            return dict(override), None
        tables = (self._project.catalog_metadata or {}).get("tables", [])
        if not tables:
            return (
                {},
                "No metadata loaded. Please load metadata first in Project settings.",
            )
        return {"tables": tables}, None

    @staticmethod
    def build_entity_mapping(data: Dict[str, Any]) -> Dict[str, Any]:
        mapping = {
            "ontology_class": data.get("ontology_class", ""),
            "ontology_class_label": data.get("ontology_class_label", ""),
            "sql_query": data.get("sql_query", ""),
            "id_column": data.get("id_column", ""),
            "label_column": data.get("label_column", ""),
            "catalog": data.get("catalog", ""),
            "schema": data.get("schema", ""),
            "table": data.get("table", ""),
            "attribute_mappings": data.get("attribute_mappings", {}),
        }
        if data.get("excluded"):
            mapping["excluded"] = True
        return mapping

    @staticmethod
    def build_relationship_mapping(data: Dict[str, Any]) -> Dict[str, Any]:
        mapping = {
            "property": data.get("property", ""),
            "property_label": data.get("property_label", ""),
            "sql_query": data.get("sql_query", ""),
            "source_class": data.get("source_class", ""),
            "source_class_label": data.get("source_class_label", ""),
            "target_class": data.get("target_class", ""),
            "target_class_label": data.get("target_class_label", ""),
            "source_table": data.get("source_table", ""),
            "target_table": data.get("target_table", ""),
            "source_id_column": data.get("source_id_column", ""),
            "target_id_column": data.get("target_id_column", ""),
            "direction": data.get("direction", "forward"),
            "attribute_mappings": data.get("attribute_mappings", {}),
        }
        if data.get("excluded"):
            mapping["excluded"] = True
        return mapping

    def add_or_update_entity_mapping(self, data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        project = self._project
        mappings = project.get_entity_mappings()
        new_mapping = Mapping.build_entity_mapping(data)

        was_update = False
        for i, m in enumerate(mappings):
            if m.get("ontology_class") == new_mapping["ontology_class"]:
                if m.get("excluded") and "excluded" not in new_mapping:
                    new_mapping["excluded"] = True
                mappings[i] = new_mapping
                was_update = True
                break

        if not was_update:
            mappings.append(new_mapping)

        project.assignment["entities"] = mappings
        project.clear_generated_content()
        project.save()

        return was_update, new_mapping

    def delete_entity_mapping(self, ontology_class: str) -> bool:
        project = self._project
        mappings = project.get_entity_mappings()
        original_len = len(mappings)
        mappings = [m for m in mappings if m.get("ontology_class") != ontology_class]

        if len(mappings) < original_len:
            project.assignment["entities"] = mappings
            project.clear_generated_content()
            project.save()
            return True
        return False

    def add_or_update_relationship_mapping(self, data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        project = self._project
        mappings = project.get_relationship_mappings()
        new_mapping = Mapping.build_relationship_mapping(data)

        was_update = False
        for i, m in enumerate(mappings):
            if m.get("property") == new_mapping["property"]:
                if m.get("excluded") and "excluded" not in new_mapping:
                    new_mapping["excluded"] = True
                mappings[i] = new_mapping
                was_update = True
                break

        if not was_update:
            mappings.append(new_mapping)

        project.assignment["relationships"] = mappings
        project.clear_generated_content()
        project.save()

        return was_update, new_mapping

    def delete_relationship_mapping(self, property_uri: str) -> bool:
        project = self._project
        mappings = project.get_relationship_mappings()
        original_len = len(mappings)
        mappings = [m for m in mappings if m.get("property") != property_uri]

        if len(mappings) < original_len:
            project.assignment["relationships"] = mappings
            project.clear_generated_content()
            project.save()
            return True
        return False

    def save_mapping_config(self, mapping_config: Dict[str, Any]) -> Dict[str, int]:
        project = self._project
        project.assignment["entities"] = mapping_config.get(
            "entities", mapping_config.get("data_source_mappings", [])
        )
        project.assignment["relationships"] = mapping_config.get(
            "relationships", mapping_config.get("relationship_mappings", [])
        )
        if mapping_config.get("r2rml_output"):
            project.assignment["r2rml_output"] = mapping_config["r2rml_output"]

        project.clear_generated_content()
        project.save()

        return {
            "entities": len(project.get_entity_mappings()),
            "relationships": len(project.get_relationship_mappings()),
        }

    def reset_mapping(self) -> None:
        project = self._project
        project.assignment["entities"] = []
        project.assignment["relationships"] = []
        project.assignment["r2rml_output"] = ""
        project.clear_generated_content()
        project.save()

    def generate_r2rml(self) -> str:
        """Generate R2RML from current mapping configuration.

        Returns:
            The generated R2RML Turtle content.

        Raises:
            ValidationError: No entity mappings configured.
            InfrastructureError: R2RML generation failed.
        """
        from back.core.w3c import R2RMLGenerator
        from back.core.errors import ValidationError as _VE

        project = self._project
        if not project.get_entity_mappings():
            raise _VE("No entity mappings configured")

        try:
            base_uri = project.ontology.get("base_uri", DEFAULT_BASE_URI)
            generator = R2RMLGenerator(base_uri)
            r2rml_content = generator.generate_mapping(project.assignment, project.ontology)

            project.set_r2rml(r2rml_content)
            project.save()

            return r2rml_content
        except Exception as e:
            logger.exception("Failed to generate R2RML: %s", e)
            raise InfrastructureError(
                "Failed to generate R2RML mapping",
                detail=str(e),
            ) from e

    def parse_r2rml(self, r2rml_content: str) -> Dict[str, Any]:
        from back.core.w3c import R2RMLParser

        project = self._project
        parser = R2RMLParser()
        result = parser.parse(r2rml_content)

        if result.get("success"):
            project.assignment["entities"] = result.get("entity_mappings", [])
            project.assignment["relationships"] = result.get("relationship_mappings", [])
            project.assignment["r2rml_output"] = r2rml_content
            project.save()

        return result

    def get_mapping_stats(self) -> Dict[str, int]:
        project = self._project
        return {
            "entities": len(project.get_entity_mappings()),
            "relationships": len(project.get_relationship_mappings()),
        }

    @staticmethod
    def test_sql_query(client: Any, sql_query: str, limit: int = 100) -> Dict[str, Any]:
        test_query = sql_query.strip().rstrip(";")
        test_query = re.sub(r"\s+LIMIT\s+\d+\s*$", "", test_query, flags=re.IGNORECASE).strip()
        test_query = f"{test_query} LIMIT {limit}"

        rows = client.execute_query(test_query)

        columns: List[str] = []
        if rows and len(rows) > 0:
            columns = list(rows[0].keys())

        return {
            "columns": columns,
            "rows": rows or [],
            "sample_data": rows or [],
            "row_count": len(rows) if rows else 0,
        }

    @staticmethod
    def fetch_documents_for_agent(project: Any, host: str, token: str) -> List[Dict[str, Any]]:
        base_path = project.uc_project_path
        if not base_path:
            logger.debug("fetch_documents_for_agent: no registry path — skipping documents")
            return []
        base_path = f"{base_path}/documents"
        host_url = host.rstrip("/")
        if not host_url.startswith("http"):
            host_url = f"https://{host_url}"
        headers = {"Authorization": f"Bearer {token}"}
        try:
            resp = requests.get(
                f"{host_url}/api/2.0/fs/directories{base_path}",
                headers=headers,
                timeout=30,
            )
            if resp.status_code == 404:
                logger.info("fetch_documents_for_agent: documents dir not found")
                return []
            resp.raise_for_status()
            entries = resp.json().get("contents", [])
            files = [e for e in entries if not e.get("is_directory", False)]
        except Exception as e:
            logger.warning("fetch_documents_for_agent: list failed — %s", e)
            return []
        uc_service = VolumeFileService(host=host, token=token)
        result: List[Dict[str, Any]] = []
        for f in files[:20]:
            name = f.get("name", "").rstrip("/")
            if not name:
                continue
            file_path = f"{base_path}/{name}"
            ok, content, _ = uc_service.read_file(file_path)
            if not ok or not content or not isinstance(content, str):
                continue
            if len(content) > _MAX_DOC_CHARS:
                content = content[:_MAX_DOC_CHARS] + f"\n\n[…truncated, {len(content)} total chars]"
            result.append({"name": name, "content": content})
        logger.info("fetch_documents_for_agent: loaded %d document(s)", len(result))
        return result

    @staticmethod
    def build_per_item_results(
        entities: list,
        relationships: list,
        entity_mappings: list,
        relationship_mappings: list,
    ) -> list:
        results = []

        mapped_entity_uris = {
            m.get("ontology_class") or m.get("class_uri", "")
            for m in entity_mappings
            if m.get("sql_query")
        }
        for ent in entities:
            uri = ent.get("uri", "")
            name = ent.get("name", ent.get("label", "?"))
            if uri in mapped_entity_uris:
                m = next(
                    (m for m in entity_mappings if (m.get("ontology_class") or m.get("class_uri")) == uri),
                    {},
                )
                results.append(
                    {
                        "type": "entity",
                        "name": name,
                        "uri": uri,
                        "status": "success",
                        "details": f"ID: {m.get('id_column', '?')}, Label: {m.get('label_column', '?')}",
                    }
                )
            else:
                results.append(
                    {
                        "type": "entity",
                        "name": name,
                        "uri": uri,
                        "status": "failed",
                        "error": "No mapping generated by agent",
                    }
                )

        mapped_rel_uris = {m.get("property", "") for m in relationship_mappings if m.get("sql_query")}
        for rel in relationships:
            uri = rel.get("uri", "")
            name = rel.get("name", rel.get("label", "?"))
            if uri in mapped_rel_uris:
                m = next((m for m in relationship_mappings if m.get("property") == uri), {})
                results.append(
                    {
                        "type": "relationship",
                        "name": name,
                        "uri": uri,
                        "status": "success",
                        "details": f"Source: {m.get('source_id_column', '?')}, Target: {m.get('target_id_column', '?')}",
                    }
                )
            else:
                results.append(
                    {
                        "type": "relationship",
                        "name": name,
                        "uri": uri,
                        "status": "failed",
                        "error": "No mapping generated by agent",
                    }
                )

        return results

    @staticmethod
    def save_mappings_to_session(
        session_id: Optional[str],
        session_ref: Any,
        entity_mappings: Optional[list],
        relationship_mappings: Optional[list],
        *,
        existing_entity_mappings: Optional[list] = None,
        existing_relationship_mappings: Optional[list] = None,
    ) -> None:
        if not session_id:
            logger.warning("save_mappings_to_session: no session_id — skipping")
            return

        settings = get_settings()
        session_path = Path(settings.session_dir) / session_id
        try:
            if session_path.exists():
                data = json.loads(session_path.read_text())
            else:
                logger.warning("save_mappings_to_session: session file missing — using in-memory ref")
                data = dict(session_ref) if session_ref else {}

            assignment = data.setdefault("project_data", {}).setdefault("assignment", {})

            if entity_mappings is not None:
                if existing_entity_mappings is not None:
                    merged = list(existing_entity_mappings)
                    for new_m in entity_mappings:
                        uri = new_m.get("ontology_class") or new_m.get("class_uri", "")
                        idx = next(
                            (
                                i
                                for i, m in enumerate(merged)
                                if (m.get("ontology_class") or m.get("class_uri")) == uri
                            ),
                            None,
                        )
                        if idx is not None:
                            if merged[idx].get("excluded") and "excluded" not in new_m:
                                new_m["excluded"] = True
                            merged[idx] = new_m
                        else:
                            merged.append(new_m)
                    assignment["entities"] = merged
                else:
                    assignment["entities"] = entity_mappings

            if relationship_mappings is not None:
                if existing_relationship_mappings is not None:
                    merged = list(existing_relationship_mappings)
                    for new_m in relationship_mappings:
                        uri = new_m.get("property", "")
                        idx = next(
                            (i for i, m in enumerate(merged) if m.get("property") == uri),
                            None,
                        )
                        if idx is not None:
                            if merged[idx].get("excluded") and "excluded" not in new_m:
                                new_m["excluded"] = True
                            merged[idx] = new_m
                        else:
                            merged.append(new_m)
                    assignment["relationships"] = merged
                else:
                    assignment["relationships"] = relationship_mappings

            project_node = data.setdefault("project_data", {}).setdefault("project", {})
            project_node["assignment_changed"] = True

            session_path.write_text(json.dumps(data, default=str))
            e_count = len(assignment.get("entities", []))
            r_count = len(assignment.get("relationships", []))
            logger.info(
                "save_mappings_to_session: saved %d entity, %d relationship mappings to session %s",
                e_count,
                r_count,
                session_id[:8],
            )
        except Exception:
            logger.exception("save_mappings_to_session: failed to persist mappings")

    @staticmethod
    def validate_mapping_sql(
        wizard: Any,
        sql: str,
        catalog: Optional[str],
        schema: Optional[str],
        validate_plan: bool,
    ) -> Dict[str, Any]:
        if catalog and schema:
            context = wizard.get_schema_context(catalog, schema)
            is_valid, message, corrected_sql = wizard.validate_sql_static(sql, context)

            if not is_valid:
                return {
                    "success": False,
                    "valid": False,
                    "error": message,
                    "sql": sql,
                }

            if validate_plan:
                plan_valid, plan_message, plan_info = wizard.validate_sql_explain(corrected_sql)

                return {
                    "success": True,
                    "valid": plan_valid,
                    "message": plan_message,
                    "sql": corrected_sql,
                    "warnings": plan_info.get("warnings", []) if plan_info else [],
                }

            return {
                "success": True,
                "valid": True,
                "message": message,
                "sql": corrected_sql,
            }

        sql_upper = sql.upper().strip()
        if not sql_upper.startswith("SELECT"):
            return {
                "success": False,
                "valid": False,
                "error": "Query must be a SELECT statement",
            }

        for keyword in wizard.FORBIDDEN_KEYWORDS:
            if re.search(rf"\b{keyword}\b", sql_upper):
                return {
                    "success": False,
                    "valid": False,
                    "error": f"Query contains forbidden keyword: {keyword}",
                }

        return {
            "success": True,
            "valid": True,
            "message": "Basic validation passed",
            "sql": sql,
        }

    def toggle_exclude_items(self, uris: List[str], excluded: bool, item_type: str) -> int:
        project = self._project
        uri_set = set(uris)
        assignment = project.assignment

        changed = 0
        if item_type == "entity":
            entries = assignment.setdefault("entities", [])
            existing = {m.get("ontology_class"): m for m in entries}
            for uri in uri_set:
                if uri in existing:
                    if excluded:
                        existing[uri]["excluded"] = True
                    else:
                        existing[uri].pop("excluded", None)
                elif excluded:
                    entries.append({"ontology_class": uri, "excluded": True})
                changed += 1
        else:
            entries = assignment.setdefault("relationships", [])
            existing = {m.get("property"): m for m in entries}
            for uri in uri_set:
                if uri in existing:
                    if excluded:
                        existing[uri]["excluded"] = True
                    else:
                        existing[uri].pop("excluded", None)
                elif excluded:
                    entries.append({"property": uri, "excluded": True})
                changed += 1

        if item_type == "entity":
            for cls in project.ontology.get("classes", []):
                cls.pop("excluded", None)
        else:
            for prop in project.ontology.get("properties", []):
                prop.pop("excluded", None)

        project.save()
        return changed

    @staticmethod
    def compute_mapping_gaps(
        active_classes: list,
        active_props: list,
        active_entity_mappings: list,
        active_rel_mappings: list,
    ) -> tuple:
        """Compute unmapped entities, relationships, and attributes.

        Returns:
            ``(unmapped_entities, unmapped_relationships, unmapped_attributes,
            mapping_by_class, mapped_class_uris, mapped_property_uris)``
        """
        mapping_by_class = {m.get('ontology_class'): m for m in active_entity_mappings}
        mapped_class_uris = set(mapping_by_class.keys())
        mapped_property_uris = {m.get('property') for m in active_rel_mappings}

        unmapped_entities = []
        for cls in active_classes:
            uri = cls.get('uri') or cls.get('name')
            if uri not in mapped_class_uris:
                unmapped_entities.append({
                    'name': cls.get('name', ''),
                    'label': cls.get('label', cls.get('name', 'Unknown')),
                    'uri': cls.get('uri', ''),
                })

        unmapped_relationships = []
        for prop in active_props:
            uri = prop.get('uri') or prop.get('name')
            if uri not in mapped_property_uris:
                unmapped_relationships.append({
                    'name': prop.get('name', ''),
                    'label': prop.get('label', prop.get('name', 'Unknown')),
                    'uri': prop.get('uri', ''),
                    'domain': prop.get('domain', ''),
                    'range': prop.get('range', ''),
                })

        unmapped_attributes = []
        for cls in active_classes:
            cls_uri = cls.get('uri') or cls.get('name')
            data_props = cls.get('dataProperties', [])
            if not data_props or cls_uri not in mapped_class_uris:
                continue
            em = mapping_by_class.get(cls_uri, {})
            attr_map = em.get('attribute_mappings', {})
            cls_label = cls.get('label') or cls.get('name', 'Unknown')
            for dp in data_props:
                attr_name = dp.get('name') or dp.get('localName') or ''
                if attr_name and attr_name not in attr_map:
                    unmapped_attributes.append({'class': cls_label, 'attribute': attr_name})

        return (unmapped_entities, unmapped_relationships, unmapped_attributes,
                mapping_by_class, mapped_class_uris, mapped_property_uris)

    @staticmethod
    def build_mapping_issues(
        active_classes: list,
        active_props: list,
        active_entity_mappings: list,
        active_rel_mappings: list,
        unmapped_entity_count: int,
        unmapped_rel_count: int,
        unmapped_attr_count: int,
    ) -> List[str]:
        """Build human-readable mapping issues list."""
        issues: List[str] = []
        if not active_entity_mappings and active_classes:
            issues.append('No entity mappings defined')
        elif unmapped_entity_count > 0:
            issues.append(f'{unmapped_entity_count} entity(ies) not mapped')

        if active_props:
            if not active_rel_mappings:
                issues.append('No relationship mappings defined')
            elif unmapped_rel_count > 0:
                issues.append(f'{unmapped_rel_count} relationship(s) not mapped')

        if unmapped_attr_count > 0:
            issues.append(f'{unmapped_attr_count} attribute(s) not assigned')
        return issues
