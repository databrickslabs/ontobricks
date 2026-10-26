"""R2RML mapping augmentation, VIEW error diagnosis, and SPARQL-on-Spark.

Extracted from :class:`DigitalTwin` (Fowler Extract Class).
``DigitalTwin`` keeps one-line delegators for callers.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Set

from back.core.errors import InfrastructureError, ValidationError
from back.core.helpers import extract_local_name
from back.core.logging import get_logger

logger = get_logger(__name__)


def _dt():
    from back.objects.digitaltwin.DigitalTwin import DigitalTwin

    return DigitalTwin


class TwinMapping:
    """Pure mapping transforms plus the SPARQL → Spark SQL execution pipeline."""

    def __init__(self, domain) -> None:
        self._domain = domain

    # ------------------------------------------------------------------
    # Private helpers (static)
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_base_uri(uri: str) -> str:
        """Ensure base_uri ends with exactly one '/' separator."""
        return uri.rstrip("/").rstrip("#") + "/"

    @staticmethod
    def _safe_class_label(class_label: str, class_uri: str) -> str:
        """Return a non-empty sanitized class label for use in URI templates.

        Falls back to the local name extracted from class_uri when class_label
        is empty, preventing double-slash URIs like base_uri//{id}.
        """
        name = (class_label or "").strip().replace(" ", "_")
        if name:
            return name
        if class_uri:
            name = extract_local_name(class_uri).strip()
            if name:
                return name.replace(" ", "_")
        return "Entity"

    # ------------------------------------------------------------------
    # SQL column extraction
    # ------------------------------------------------------------------

    _SELECT_CLAUSE_RE = re.compile(
        r"SELECT\s+(?:DISTINCT\s+)?(.*?)\s+FROM\s",
        re.IGNORECASE | re.DOTALL,
    )
    _ALIAS_RE = re.compile(r"\bAS\s+(\w+)\s*$", re.IGNORECASE)

    @staticmethod
    def _extract_select_columns(sql_query: str) -> Set[str] | None:
        """Extract output column names from a SELECT query.

        For ``SELECT col1 AS A, col2 AS B FROM ...`` returns ``{"A", "B"}``.
        For ``SELECT col1, col2 FROM ...`` returns ``{"col1", "col2"}``.
        For Spark implicit aliases (``SELECT obj_id s, parent_obj_id t``)
        returns ``{"s", "t"}``.
        Returns ``None`` when the SELECT clause cannot be parsed reliably.
        """
        if not sql_query:
            return None
        m = TwinMapping._SELECT_CLAUSE_RE.search(sql_query)
        if not m:
            return None

        raw_cols = m.group(1)
        depth = 0
        parts: list[str] = []
        current: list[str] = []
        for ch in raw_cols:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                parts.append("".join(current).strip())
                current = []
                continue
            current.append(ch)
        parts.append("".join(current).strip())

        columns: set[str] = set()
        for part in parts:
            if not part or part == "*":
                return None
            alias_m = TwinMapping._ALIAS_RE.search(part)
            if alias_m:
                columns.add(alias_m.group(1).strip('`"'))
            else:
                tokens = part.replace("`", "").split()
                if len(tokens) >= 2 and re.fullmatch(r"[A-Za-z_][\w]*", tokens[-1]):
                    columns.add(tokens[-1])
                else:
                    token = part.rsplit(".", 1)[-1].strip().strip('`"')
                    if token:
                        columns.add(token)
        return columns if columns else None

    # ------------------------------------------------------------------
    # VIEW error diagnostics
    # ------------------------------------------------------------------

    @staticmethod
    def diagnose_view_error(
        error_msg: str,
        entity_mappings: Dict[str, Any],
        relationship_mappings: list | None = None,
    ) -> str:
        """Parse a VIEW creation error and enrich it with mapping context.

        Extracts unresolved column names from Databricks error messages, then
        searches entity and relationship mappings to identify which entity,
        source table, and attribute mapping caused the problem.

        Returns an enriched error string suitable for user-facing task messages.
        """
        # --- Permission errors (UC MANAGE / SELECT / USAGE missing) -----------
        perm_match = re.search(
            r"PERMISSION_DENIED:\s*([^\n]+)", error_msg, re.IGNORECASE
        )
        if perm_match:
            perm_detail = perm_match.group(1).strip().rstrip(".")
            return (
                f"Permission denied while creating the VIEW.\n"
                f"  Detail: {perm_detail}\n"
                f"  Fix: Grant the required privilege to the Databricks App service "
                f"principal (typically MANAGE on the target object or its parent "
                f"schema, and SELECT on all source tables). "
                f"If an object with the target name already exists as a TABLE, drop "
                f"it first — CREATE OR REPLACE VIEW cannot overwrite a TABLE."
            )

        # --- Missing source table / view -------------------------------------
        tbl_match = re.search(
            r"TABLE_OR_VIEW_NOT_FOUND[^`']*"
            r"((?:`[^`]+`|'[^']+')"
            r"(?:\.(?:`[^`]+`|'[^']+')){0,2})",
            error_msg,
        )
        if tbl_match:
            missing = tbl_match.group(1)
            return (
                f"Source table or view not found: {missing}.\n"
                f"  Fix: Verify the catalog/schema/table exists and the app service "
                f"principal has SELECT on it."
            )

        # --- Column-resolution errors ----------------------------------------
        col_match = re.search(r"name `([^`]+)` cannot be resolved", error_msg)
        if not col_match:
            col_match = re.search(
                r"Column '([^']+)' does not exist", error_msg, re.IGNORECASE
            )
        if not col_match:
            logger.warning(
                "diagnose_view_error: unrecognized database error format: %s",
                error_msg,
            )
            truncated = error_msg.strip()
            if len(truncated) > 500:
                truncated = truncated[:500] + " …"
            return (
                "VIEW creation failed with an unrecognized database error.\n"
                f"  Detail: {truncated}\n"
                "  Fix: Check source tables, column mappings and warehouse "
                "permissions. Full traceback is available in the server logs."
            )

        bad_column = col_match.group(1)

        suggestions_match = re.search(
            r"Did you mean one of the following\?\s*\[([^\]]+)\]", error_msg
        )
        suggested = suggestions_match.group(1).strip() if suggestions_match else ""

        for class_uri, mapping in (entity_mappings or {}).items():
            local_name = extract_local_name(class_uri)
            source = (
                mapping.get("sql_query") or mapping.get("table") or "unknown"
            ).strip()

            if mapping.get("id_column") == bad_column:
                return (
                    f"Column '{bad_column}' not found in source for entity '{local_name}'.\n"
                    f"  Entity: {local_name} ({class_uri})\n"
                    f"  Source: {source}\n"
                    f"  Role: id_column\n"
                    + (f"  Available columns: {suggested}\n" if suggested else "")
                    + f"  Fix: Update the ID column mapping for '{local_name}' to use a valid column name."
                )
            if mapping.get("label_column") == bad_column:
                return (
                    f"Column '{bad_column}' not found in source for entity '{local_name}'.\n"
                    f"  Entity: {local_name} ({class_uri})\n"
                    f"  Source: {source}\n"
                    f"  Role: label_column\n"
                    + (f"  Available columns: {suggested}\n" if suggested else "")
                    + f"  Fix: Update the label column mapping for '{local_name}' to use a valid column name."
                )
            for pred_uri, pred_info in mapping.get("predicates", {}).items():
                if pred_info.get("column") == bad_column:
                    attr_name = extract_local_name(pred_uri)
                    return (
                        f"Column '{bad_column}' not found in source for entity '{local_name}'.\n"
                        f"  Entity: {local_name} ({class_uri})\n"
                        f"  Source: {source}\n"
                        f"  Attribute: {attr_name}\n"
                        + (f"  Available columns: {suggested}\n" if suggested else "")
                        + f"  Fix: Update the attribute mapping '{attr_name}' for '{local_name}' to use a valid column name."
                    )

        for rel in relationship_mappings or []:
            rel_name = rel.get("property", "unknown")
            source = (rel.get("sql_query") or "unknown").strip()
            for key in ("source_column", "target_column"):
                if rel.get(key) == bad_column:
                    return (
                        f"Column '{bad_column}' not found in source for relationship '{rel_name}'.\n"
                        f"  Relationship: {rel_name}\n"
                        f"  Source: {source}\n"
                        f"  Role: {key}\n"
                        + (f"  Available columns: {suggested}\n" if suggested else "")
                        + f"  Fix: Update the {key} for relationship '{rel_name}' to use a valid column name."
                    )

        logger.warning(
            "diagnose_view_error: column '%s' not found in mappings; raw DB message: %s",
            bad_column,
            error_msg,
        )
        return (
            f"Column '{bad_column}' not found in any source table.\n"
            + (f"  Available columns: {suggested}\n" if suggested else "")
            + "  See server logs for the full database error message."
        )

    # ------------------------------------------------------------------
    # R2RML mapping augmentation (static -- pure transforms)
    # ------------------------------------------------------------------

    @staticmethod
    def augment_mappings_from_config(
        entity_mappings, mapping_config, base_uri, ontology_config=None
    ):
        """Augment R2RML mappings with data from mapping_config to ensure all attributes are included.

        Args:
            entity_mappings: dict of entity class URIs to mapping info
            mapping_config: mapping configuration from session
            base_uri: base URI for the ontology
            ontology_config: ontology configuration (used to skip excluded classes)

        Returns:
            dict: Augmented entity mappings
        """
        base_uri = TwinMapping._normalize_base_uri(base_uri)

        if not mapping_config:
            return entity_mappings

        ontology_config = ontology_config or {}
        all_dsm = (mapping_config or {}).get(
            "entities", (mapping_config or {}).get("data_source_mappings", [])
        )
        excluded_class_uris = {
            m.get("ontology_class") for m in all_dsm if m.get("excluded")
        }

        data_source_mappings = mapping_config.get(
            "entities", mapping_config.get("data_source_mappings", [])
        )

        for dsm in data_source_mappings:
            class_uri = dsm.get("ontology_class", "")
            class_label = dsm.get("ontology_class_label", "")
            sql_query = dsm.get("sql_query", "").strip()
            id_column = dsm.get("id_column", "")
            label_column = dsm.get("label_column", "")
            attribute_mappings = dsm.get("attribute_mappings", {})

            if not class_uri or not sql_query:
                continue

            if class_uri in excluded_class_uris:
                continue

            full_class_uri = (
                class_uri if class_uri.startswith("http") else f"{base_uri}{class_uri}"
            )

            sanitized_label = TwinMapping._safe_class_label(class_label, class_uri)

            if full_class_uri not in entity_mappings:
                entity_mappings[full_class_uri] = {
                    "table": None,
                    "id_column": id_column,
                    "label_column": label_column,
                    "uri_template": f"{base_uri}{sanitized_label}/{{{id_column}}}",
                    "sql_query": sql_query,
                    "predicates": {},
                }

            mapping = entity_mappings[full_class_uri]

            if not mapping.get("sql_query") and sql_query:
                mapping["sql_query"] = sql_query

            if label_column and not mapping.get("label_column"):
                mapping["label_column"] = label_column

            if (
                label_column
                and "http://www.w3.org/2000/01/rdf-schema#label"
                not in mapping.get("predicates", {})
            ):
                mapping.setdefault("predicates", {})[
                    "http://www.w3.org/2000/01/rdf-schema#label"
                ] = {"type": "column", "column": label_column}

            available_cols = TwinMapping._extract_select_columns(sql_query)

            for attr_name, column_name in attribute_mappings.items():
                if not column_name:
                    continue
                if available_cols and column_name not in available_cols:
                    logger.warning(
                        "Entity '%s': skipping attribute '%s' — column '%s' "
                        "is not in the source output columns %s. "
                        "Likely aliased away in the SQL query.",
                        class_label or class_uri,
                        attr_name,
                        column_name,
                        sorted(available_cols),
                    )
                    continue
                pred_uri = f"{base_uri}{attr_name.replace(' ', '_')}"
                mapping.setdefault("predicates", {})[pred_uri] = {
                    "type": "column",
                    "column": column_name,
                }

        # Final pass: remove ALL predicate columns (including R2RML-sourced)
        # that reference raw columns not visible through the CTE aliases.
        for class_uri, mapping in entity_mappings.items():
            src_sql = (mapping.get("sql_query") or "").strip()
            avail = TwinMapping._extract_select_columns(src_sql)
            if not avail:
                continue

            local_name = extract_local_name(class_uri)
            bad_preds = [
                pred_uri
                for pred_uri, info in mapping.get("predicates", {}).items()
                if info.get("type") == "column"
                and info.get("column")
                and info["column"] not in avail
            ]
            for pred_uri in bad_preds:
                col = mapping["predicates"][pred_uri]["column"]
                attr = extract_local_name(pred_uri)
                logger.warning(
                    "Entity '%s': removing predicate '%s' — column '%s' "
                    "is not available in source output columns %s.",
                    local_name,
                    attr,
                    col,
                    sorted(avail),
                )
                del mapping["predicates"][pred_uri]

            all_columns = set()
            if mapping.get("id_column"):
                all_columns.add(mapping["id_column"])
            if mapping.get("label_column"):
                all_columns.add(mapping["label_column"])
            for pred_info in mapping.get("predicates", {}).values():
                if pred_info.get("type") == "column" and pred_info.get("column"):
                    all_columns.add(pred_info["column"])
            source = (src_sql or mapping.get("table") or "unknown").strip()
            logger.info(
                "Entity '%s' mapped columns: [%s] from source: %s",
                local_name,
                ", ".join(sorted(all_columns)),
                source,
            )

        return entity_mappings

    @staticmethod
    def augment_relationships_from_config(
        relationship_mappings, mapping_config, base_uri, ontology_config=None
    ):
        """Augment relationship mappings from mapping_config.

        Args:
            relationship_mappings: list of relationship mappings
            mapping_config: mapping configuration from session
            base_uri: base URI for the ontology
            ontology_config: ontology configuration for fallback class lookup

        Returns:
            list: Augmented relationship mappings
        """
        base_uri = TwinMapping._normalize_base_uri(base_uri)

        ontology_config = ontology_config or {}
        if not mapping_config:
            return relationship_mappings

        all_dsm = (mapping_config or {}).get(
            "entities", (mapping_config or {}).get("data_source_mappings", [])
        )
        excluded_entity_uris = {
            m.get("ontology_class") for m in all_dsm if m.get("excluded")
        }
        excluded_class_names = set()
        for c in ontology_config.get("classes", []):
            if c.get("uri") in excluded_entity_uris:
                excluded_class_names.add(c.get("name") or c.get("localName") or "")

        all_rm = (mapping_config or {}).get(
            "relationships", (mapping_config or {}).get("relationship_mappings", [])
        )
        excluded_prop_uris = {m.get("property") for m in all_rm if m.get("excluded")}
        for p in ontology_config.get("properties", []):
            if (
                p.get("domain") in excluded_class_names
                or p.get("range") in excluded_class_names
            ):
                if p.get("uri"):
                    excluded_prop_uris.add(p["uri"])

        rel_configs = mapping_config.get(
            "relationships", mapping_config.get("relationship_mappings", [])
        )
        data_source_mappings = mapping_config.get(
            "entities", mapping_config.get("data_source_mappings", [])
        )

        entity_lookup = {}
        for dsm in data_source_mappings:
            class_uri = dsm.get("ontology_class", "")
            class_label = dsm.get("ontology_class_label", "")
            id_column = dsm.get("id_column", "")

            full_uri = (
                class_uri if class_uri.startswith("http") else f"{base_uri}{class_uri}"
            )

            sanitized_label = TwinMapping._safe_class_label(class_label, class_uri)
            entity_info = {
                "uri_base": f"{base_uri}{sanitized_label}/",
                "id_column": id_column,
            }

            entity_lookup[class_label] = entity_info
            entity_lookup[class_label.lower()] = entity_info
            entity_lookup[sanitized_label] = entity_info
            entity_lookup[class_uri] = entity_info
            entity_lookup[full_uri] = entity_info

            local_name = extract_local_name(class_uri)
            if local_name:
                entity_lookup[local_name] = entity_info

        ontology_property_lookup = {}
        ontology_classes = ontology_config.get("classes", [])
        for prop in ontology_config.get("properties", []) or ontology_config.get(
            "object_properties", []
        ):
            prop_uri = prop.get("uri", "")
            prop_label = prop.get("label", "") or prop.get("name", "")
            domain = prop.get("domain", "") or prop.get("source", "")
            range_val = prop.get("range", "") or prop.get("target", "")

            domain_label = ""
            for cls in ontology_classes:
                if (
                    cls.get("uri") == domain
                    or cls.get("name") == domain
                    or cls.get("label") == domain
                ):
                    domain_label = cls.get("label", "") or cls.get("name", "")
                    break

            range_label = ""
            for cls in ontology_classes:
                if (
                    cls.get("uri") == range_val
                    or cls.get("name") == range_val
                    or cls.get("label") == range_val
                ):
                    range_label = cls.get("label", "") or cls.get("name", "")
                    break

            prop_info = {"domain_label": domain_label, "range_label": range_label}

            if prop_uri:
                ontology_property_lookup[prop_uri] = prop_info
            if prop_label:
                ontology_property_lookup[prop_label] = prop_info

        for rel in rel_configs:
            sql_query = rel.get("sql_query", "").strip()
            predicate_uri = rel.get("property", "")
            predicate_label = rel.get("property_label", "")
            source_class = rel.get("source_class", "")
            target_class = rel.get("target_class", "")
            source_class_label = rel.get("source_class_label", "")
            target_class_label = rel.get("target_class_label", "")
            source_column = rel.get("source_id_column", "")
            target_column = rel.get("target_id_column", "")

            if not sql_query or not source_column or not target_column:
                continue

            if predicate_uri in excluded_prop_uris:
                continue

            if predicate_uri and predicate_uri.startswith(("http://", "https://")):
                if not predicate_uri.startswith(base_uri):
                    local = extract_local_name(predicate_uri)
                    predicate_uri = f"{base_uri}{local.replace(' ', '_')}"
            elif predicate_uri:
                predicate_uri = f"{base_uri}{predicate_uri.replace(' ', '_')}"
            elif predicate_label:
                predicate_uri = f"{base_uri}{predicate_label.replace(' ', '_')}"
            else:
                predicate_uri = f"{base_uri}relatesTo"

            rel_domain = rel.get("domain", "")
            rel_range = rel.get("range", "")
            direction = rel.get("direction", "forward")

            source_label = source_class_label or extract_local_name(source_class) or ""
            target_label = target_class_label or extract_local_name(target_class) or ""

            if not source_label:
                source_label = extract_local_name(
                    rel_range if direction == "reverse" else rel_domain
                )
            if not target_label:
                target_label = extract_local_name(
                    rel_domain if direction == "reverse" else rel_range
                )

            if not source_label or not target_label:
                prop_info = (
                    ontology_property_lookup.get(predicate_uri)
                    or ontology_property_lookup.get(predicate_label)
                    or {}
                )
                if not source_label:
                    source_label = prop_info.get("domain_label", "")
                if not target_label:
                    target_label = prop_info.get("range_label", "")

            source_local = extract_local_name(source_class)
            source_info = (
                entity_lookup.get(source_class)
                or entity_lookup.get(source_label)
                or entity_lookup.get(source_label.lower() if source_label else "")
                or entity_lookup.get(
                    source_label.replace(" ", "_") if source_label else ""
                )
                or (entity_lookup.get(source_local) if source_local else None)
                or (entity_lookup.get(source_local.lower()) if source_local else None)
                or {
                    "uri_base": f"{base_uri}{source_label.replace(' ', '_') if source_label else 'Entity'}/",
                    "id_column": source_column,
                }
            )

            target_local = extract_local_name(target_class)
            target_info = (
                entity_lookup.get(target_class)
                or entity_lookup.get(target_label)
                or entity_lookup.get(target_label.lower() if target_label else "")
                or entity_lookup.get(
                    target_label.replace(" ", "_") if target_label else ""
                )
                or (entity_lookup.get(target_local) if target_local else None)
                or (entity_lookup.get(target_local.lower()) if target_local else None)
                or {
                    "uri_base": f"{base_uri}{target_label.replace(' ', '_') if target_label else 'Entity'}/",
                    "id_column": target_column,
                }
            )

            subject_template = source_info["uri_base"] + "{" + source_column + "}"
            object_template = target_info["uri_base"] + "{" + target_column + "}"

            existing_rel = None
            for r in relationship_mappings:
                if (
                    r.get("predicate") == predicate_uri
                    and r.get("sql_query") == sql_query
                ):
                    existing_rel = r
                    break

            if existing_rel:
                old_subj = existing_rel.get("subject_template", "")
                old_obj = existing_rel.get("object_template", "")
                if (
                    "/Source/" in old_subj
                    or "/Target/" in old_subj
                    or "/Entity/" in old_subj
                    or "/UnknownEntity/" in old_subj
                ):
                    existing_rel["subject_template"] = subject_template
                if (
                    "/Source/" in old_obj
                    or "/Target/" in old_obj
                    or "/Entity/" in old_obj
                    or "/UnknownEntity/" in old_obj
                ):
                    existing_rel["object_template"] = object_template
            else:
                relationship_mappings.append(
                    {
                        "predicate": predicate_uri,
                        "sql_query": sql_query,
                        "subject_template": subject_template,
                        "object_template": object_template,
                        "subject_column": source_column,
                        "object_column": target_column,
                    }
                )

        return relationship_mappings

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
        from back.core.helpers import get_data_plane_client, run_blocking

        domain = self._domain
        try:
            # OBO: SPARQL compiles to SQL on the triplestore VIEW / source
            # tables in Unity Catalog. Run it as the signed-in user so UC
            # governs what is readable (fail-closed when no user token).
            client = get_data_plane_client(domain, settings)

            if not client:
                raise ValidationError(
                    "Databricks is not configured. Please configure your Databricks connection in Settings."
                )

            if not client.warehouse_id:
                raise ValidationError(
                    "No SQL warehouse configured. Please configure your Databricks connection in Settings."
                )

            if not client.host or not client.warehouse_id:
                missing = []
                if not client.host:
                    missing.append("host")
                if not client.warehouse_id:
                    missing.append("warehouse_id")
                raise ValidationError(
                    f'Databricks configuration incomplete. Missing: {", ".join(missing)}.'
                )

            if not client.has_valid_auth():
                raise ValidationError("Databricks authentication not configured.")

            entity_mappings, relationship_mappings = sparql.extract_r2rml_mappings(
                r2rml_content
            )
            base_uri = domain.ontology.get("base_uri", DEFAULT_BASE_URI)

            entity_mappings = _dt().augment_mappings_from_config(
                entity_mappings, domain.assignment, base_uri, domain.ontology
            )
            relationship_mappings = _dt().augment_relationships_from_config(
                relationship_mappings, domain.assignment, base_uri, domain.ontology
            )

            if not entity_mappings and not relationship_mappings:
                raise ValidationError("No valid R2RML TriplesMap found.")

            result = sparql.translate_sparql_to_spark(
                sparql_query, entity_mappings, limit, relationship_mappings
            )
            if not result.get("success"):
                raise ValidationError(
                    result.get("message") or "SPARQL translation failed."
                )

            spark_sql = result["sql"]
            select_vars = result["variables"]

            try:
                results = await run_blocking(client.execute_query, spark_sql)
            except Exception as e:
                logger.exception("Databricks query execution failed: %s", e)
                error_msg = str(e)
                if "NoneType" in error_msg or "request" in error_msg:
                    raise InfrastructureError(
                        "Databricks connection failed. Please verify your configuration.",
                        detail=error_msg,
                    ) from e
                raise InfrastructureError(
                    "Spark SQL execution failed.",
                    detail=error_msg,
                ) from e

            if results:
                columns = select_vars if select_vars else list(results[0].keys())
                return {
                    "success": True,
                    "results": results,
                    "columns": columns,
                    "count": len(results),
                    "engine": "spark",
                    "generated_sql": spark_sql,
                    "tables_queried": list(
                        set(
                            m.get("table", "")
                            for m in entity_mappings.values()
                            if m.get("table")
                        )
                    ),
                }
            else:
                return {
                    "success": True,
                    "results": [],
                    "columns": select_vars,
                    "count": 0,
                    "engine": "spark",
                    "generated_sql": spark_sql,
                }

        except ValidationError:
            raise
        except InfrastructureError:
            raise
        except ValueError as e:
            logger.exception("Spark query ValueError: %s", e)
            raise ValidationError(
                "The query or mapping configuration is invalid.",
                detail=str(e),
            ) from e
        except Exception as e:
            logger.exception("Spark query error: %s", e)
            raise InfrastructureError(
                "An unexpected error occurred while running the Spark query.",
                detail=str(e),
            ) from e

