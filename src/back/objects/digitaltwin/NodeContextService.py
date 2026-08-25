"""Resolve ontology class context and invoke UC function actions for a node.

Extracted from the external digitaltwin router so routes stay thin
(``.cursor/05``, ``src/.coding_rules.md §1``). Hard failures raise
``OntoBricksError`` subclasses; soft dataset/bridge fetch failures stay on
the success payload as ``message``.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from back.core.errors import InfrastructureError, NotFoundError, ValidationError
from back.core.graphdb import get_graphdb
from back.core.helpers import (
    effective_graph_query_table,
    extract_local_name,
    get_databricks_client,
    run_blocking,
    sql_escape,
)
from back.core.logging import get_logger
from back.objects.digitaltwin.DigitalTwin import DigitalTwin

logger = get_logger(__name__)

_SAFE_SQL_IDENT = re.compile(r"^[A-Za-z0-9_.]+$")
_SAFE_COL_IDENT = re.compile(r"^[A-Za-z0-9_]+$")
_RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


class NodeContextService:
    """Ontology-backed node context resolution and UC action invocation."""

    SAFE_SQL_IDENT = _SAFE_SQL_IDENT
    SAFE_COL_IDENT = _SAFE_COL_IDENT
    RDF_TYPE = _RDF_TYPE

    @staticmethod
    def match_ontology_class(
        entity_uri: str, classes: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """Resolve an entity instance URI to its ontology class definition.

        Prefers exact class-URI prefix match (``classUri/instance``), then falls
        back to matching the class local name / display name as a path or hash
        segment. Needed because R2RML often mints path-based instance URIs while
        OWL classes use ``base#ClassName``.
        """
        if not entity_uri or not classes:
            return None

        for cls in classes:
            cls_uri = (cls.get("uri") or "").rstrip("/")
            if not cls_uri:
                continue
            if entity_uri.startswith(cls_uri + "/") or entity_uri.startswith(
                cls_uri + "#"
            ):
                return cls

        for cls in classes:
            tokens = {
                t
                for t in (
                    extract_local_name(cls.get("uri") or ""),
                    (cls.get("name") or "").strip(),
                )
                if t
            }
            for token in tokens:
                if (
                    f"/{token}/" in entity_uri
                    or f"#{token}/" in entity_uri
                    or f"#{token}_" in entity_uri
                ):
                    return cls
        return None

    @staticmethod
    def class_action_entries(cls: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Return the class's UC function actions, keeping only usable entries.

        Each action binds a Unity Catalog function that takes exactly one
        parameter: the ID of the entity being acted on. Entries without a
        syntactically valid ``fullName`` are dropped rather than raising, so a
        single bad entry never breaks the whole node context.
        """
        entries: List[Dict[str, Any]] = []
        for action in cls.get("actions") or []:
            if not isinstance(action, dict):
                continue
            full_name = str(action.get("fullName") or "").strip()
            if not full_name or not _SAFE_SQL_IDENT.match(full_name):
                logger.warning("Skipping action with invalid fullName: %r", full_name)
                continue
            entries.append(
                {
                    "fullName": full_name,
                    "function": str(
                        action.get("function") or extract_local_name(full_name) or ""
                    ),
                    "description": (
                        str(action.get("description") or "").strip() or None
                    ),
                    "returns_table": bool(action.get("returns_table")),
                }
            )
        return entries

    @staticmethod
    def class_bridge_entries(cls: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Return the class's bridges with ``target_domain`` normalized.

        Ontology authoring historically used ``target_project``; downstream
        formatters (``agents/tools/graph_formatting.py``, the MCP server) and
        the front-end all read ``target_domain``. This mirrors the alias
        resolution :meth:`_resolve_bridges` already does so a shallow, static
        listing (no traversal) stays consistent with the traversal path.

        Description enrichment / MCP-visibility filtering are opt-in via
        :meth:`enrich_bridge_targets` — the raw shape here stays authoring-
        friendly (UI callers keep seeing every bridge).
        """
        entries: List[Dict[str, Any]] = []
        for b in cls.get("bridges") or []:
            if not isinstance(b, dict):
                continue
            entries.append(
                {
                    "target_domain": b.get("target_domain") or b.get("target_project", ""),
                    "target_class_name": b.get("target_class_name", ""),
                    "target_class_uri": b.get("target_class_uri", ""),
                    "label": b.get("label", ""),
                }
            )
        return entries

    @staticmethod
    def enrich_bridge_targets(
        bridges: List[Dict[str, Any]],
        *,
        session_mgr: Any,
        settings: Any,
        registry_catalog: Optional[str] = None,
        registry_schema: Optional[str] = None,
        registry_volume: Optional[str] = None,
        drop_unavailable: bool = True,
    ) -> List[Dict[str, Any]]:
        """Attach ``target_domain_description`` to each bridge.

        Fetches the MCP-visible domain map from
        :meth:`RegistryService.list_mcp_domains` once and looks each bridge's
        ``target_domain`` up (accepts the historical ``target_project`` alias).

        When *drop_unavailable* is True (default — the MCP / external REST
        contract), bridges whose target is not MCP-visible are omitted so an
        interrogating agent can only see targets it can actually hop to via
        ``select_domain``. Internal UI callers (``/dtwin/classes``) pass
        ``drop_unavailable=False`` so the ontology designer keeps seeing every
        authored bridge.

        Registry lookup errors soft-fail: the input bridges pass through
        with an empty description and without filtering. This matches the
        soft-fail contract of the rest of ``NodeContextService`` — a broken
        registry never breaks a node-context response.
        """
        normalized: List[Dict[str, Any]] = []
        for b in bridges:
            if not isinstance(b, dict):
                continue
            entry = dict(b)
            entry["target_domain"] = b.get("target_domain") or b.get("target_project", "")
            normalized.append(entry)
        if not normalized:
            return []

        descriptions = NodeContextService._load_mcp_target_descriptions(
            session_mgr=session_mgr,
            settings=settings,
            registry_catalog=registry_catalog,
            registry_schema=registry_schema,
            registry_volume=registry_volume,
        )
        if descriptions is None:
            for entry in normalized:
                entry.setdefault("target_domain_description", "")
            return normalized

        enriched: List[Dict[str, Any]] = []
        for entry in normalized:
            target = entry["target_domain"]
            if target in descriptions:
                entry["target_domain_description"] = descriptions[target] or ""
                enriched.append(entry)
            elif not drop_unavailable:
                entry["target_domain_description"] = ""
                enriched.append(entry)
        return enriched

    @staticmethod
    def _load_mcp_target_descriptions(
        *,
        session_mgr: Any,
        settings: Any,
        registry_catalog: Optional[str],
        registry_schema: Optional[str],
        registry_volume: Optional[str],
    ) -> Optional[Dict[str, str]]:
        """Return ``{domain_name: description}`` for MCP-visible domains.

        Returns ``None`` on any failure so :meth:`enrich_bridge_targets` can
        soft-fail. Kept private because the exact registry wiring may evolve
        (e.g. request-scoped caching) without affecting the enricher contract.
        """
        try:
            from back.objects.registry import RegistryCfg, RegistryService
            from back.objects.session import get_domain

            if session_mgr is None:
                return None
            base_cfg = RegistryCfg.from_session(session_mgr, settings)
            cfg = RegistryCfg(
                catalog=registry_catalog or base_cfg.catalog,
                schema=registry_schema or base_cfg.schema,
                volume=registry_volume or base_cfg.volume,
                lakebase_schema=base_cfg.lakebase_schema,
                lakebase_database=base_cfg.lakebase_database,
            )
            domain = get_domain(session_mgr)
            svc = RegistryService(cfg, DigitalTwin.uc_from_domain(domain, settings))
            ok, items, _ = svc.list_mcp_domains()
            if not ok:
                return None
            return {p["name"]: (p.get("description") or "") for p in items}
        except Exception as exc:  # noqa: BLE001 — soft-fail is the contract
            logger.debug(
                "enrich_bridge_targets: registry lookup failed — %s", exc
            )
            return None

    @staticmethod
    async def resolve_context(
        domain: Any,
        settings: Any,
        *,
        entity_uri: str,
        session_mgr: Any = None,
        fetch_dataset_rows: bool = False,
        dataset_row_limit: int = 5,
        follow_bridges: bool = False,
        bridge_depth: int = 1,
        registry_catalog: Optional[str] = None,
        registry_schema: Optional[str] = None,
        registry_volume: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build the node-context payload for *entity_uri*.

        Raises:
            ValidationError: invalid dataset identifiers or bridge_depth.
        """
        if bridge_depth < 1:
            raise ValidationError("bridge_depth must be >= 1")

        local_id = DigitalTwin.extract_local_id(entity_uri)
        dname = domain.domain_folder or (domain.info or {}).get("name", "")

        raw_classes = domain.get_classes() or []
        matched_cls = NodeContextService.match_ontology_class(entity_uri, raw_classes)

        if matched_cls is None:
            logger.info(
                "nodes/context: no class match for entity=%s domain=%s classes=%d",
                local_id,
                dname,
                len(raw_classes),
            )
            return {
                "success": True,
                "entity_uri": entity_uri,
                "entity_local_id": local_id,
            }

        class_name = matched_cls.get("name", "")
        raw_dataset = matched_cls.get("dataset") or None
        raw_bridges = matched_cls.get("bridges") or []
        actions_out = NodeContextService.class_action_entries(matched_cls)

        dataset_out, fetch_error = await NodeContextService._resolve_dataset(
            domain,
            settings,
            raw_dataset=raw_dataset,
            local_id=local_id,
            entity_uri=entity_uri,
            fetch_dataset_rows=fetch_dataset_rows,
            dataset_row_limit=dataset_row_limit,
        )

        bridges_out = await NodeContextService._resolve_bridges(
            matched_cls=matched_cls,
            raw_bridges=raw_bridges,
            local_id=local_id,
            follow_bridges=follow_bridges,
            bridge_depth=bridge_depth,
            session_mgr=session_mgr,
            settings=settings,
            registry_catalog=registry_catalog,
            registry_schema=registry_schema,
            registry_volume=registry_volume,
        )

        logger.info(
            "nodes/context: entity=%s class=%s domain=%s dataset=%s bridges=%d actions=%d",
            local_id,
            class_name,
            dname,
            bool(dataset_out),
            len(bridges_out),
            len(actions_out),
        )

        return {
            "success": True,
            "entity_uri": entity_uri,
            "entity_local_id": local_id,
            "class_name": class_name,
            "dataset": dataset_out,
            "bridges": bridges_out or None,
            "actions": actions_out or None,
            "message": fetch_error,
        }

    @staticmethod
    async def invoke_action(
        domain: Any,
        settings: Any,
        *,
        entity_uri: str,
        action_full_name: str,
    ) -> Dict[str, Any]:
        """Invoke a class-declared Unity Catalog function for *entity_uri*.

        Raises:
            NotFoundError: no ontology class matches the entity.
            ValidationError: action not on the class allow-list.
            InfrastructureError: Databricks client missing or SQL execution failed.
        """
        local_id = DigitalTwin.extract_local_id(entity_uri)
        dname = domain.domain_folder or (domain.info or {}).get("name", "")

        raw_classes = domain.get_classes() or []
        matched_cls = NodeContextService.match_ontology_class(entity_uri, raw_classes)
        if matched_cls is None:
            raise NotFoundError("No ontology class matches this entity URI")

        class_name = matched_cls.get("name", "")
        requested = (action_full_name or "").strip()
        action = next(
            (
                a
                for a in NodeContextService.class_action_entries(matched_cls)
                if a["fullName"] == requested
            ),
            None,
        )
        if action is None:
            logger.warning(
                "nodes/action: %r is not declared on class %s — rejected",
                requested,
                class_name,
            )
            raise ValidationError(
                f"Action {requested!r} is not configured on class {class_name!r}"
            )

        full_name = action["fullName"]
        returns_table = action["returns_table"]

        client_db = get_databricks_client(domain, settings)
        if client_db is None:
            raise InfrastructureError("Databricks client is not configured")

        arg = f"'{sql_escape(local_id)}'"
        sql = (
            f"SELECT * FROM {full_name}({arg})"
            if returns_table
            else f"SELECT {full_name}({arg}) AS result"
        )
        try:
            result = await run_blocking(client_db.execute_query, sql)
            rows = result if isinstance(result, list) else []
        except Exception as exc:
            logger.warning(
                "nodes/action: %s failed for %s: %s", full_name, entity_uri, exc
            )
            raise InfrastructureError(
                str(exc) or "Action invocation failed", detail=str(exc)
            ) from exc

        logger.info(
            "nodes/action: entity=%s class=%s domain=%s action=%s rows=%d",
            local_id,
            class_name,
            dname,
            full_name,
            len(rows),
        )

        return {
            "success": True,
            "entity_uri": entity_uri,
            "entity_local_id": local_id,
            "class_name": class_name,
            "action": full_name,
            "returns_table": returns_table,
            "rows": rows,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _resolve_dataset(
        domain: Any,
        settings: Any,
        *,
        raw_dataset: Optional[Dict[str, Any]],
        local_id: str,
        entity_uri: str,
        fetch_dataset_rows: bool,
        dataset_row_limit: int,
    ) -> Any:
        if not raw_dataset or not raw_dataset.get("fullName"):
            return None, None

        full_name = raw_dataset.get("fullName", "")
        if not full_name or not _SAFE_SQL_IDENT.match(full_name):
            raise ValidationError(
                f"Invalid dataset fullName in class config: {full_name!r}"
            )

        key_col = raw_dataset.get("key_column")
        if key_col and not _SAFE_COL_IDENT.match(key_col):
            raise ValidationError(
                f"Invalid key_column in class config: {key_col!r}"
            )

        rows = None
        key_col_missing = None
        fetch_error: Optional[str] = None

        if fetch_dataset_rows:
            if not key_col:
                key_col_missing = True
            else:
                try:
                    client_db = get_databricks_client(domain, settings)
                    if client_db is None:
                        fetch_error = "Databricks client is not configured"
                        rows = []
                    else:
                        sql = (
                            f"SELECT * FROM {raw_dataset['fullName']} "
                            f"WHERE {key_col} = '{sql_escape(local_id)}' "
                            f"LIMIT {dataset_row_limit}"
                        )
                        result = await run_blocking(client_db.execute_query, sql)
                        rows = result if isinstance(result, list) else []
                except Exception as exc:
                    fetch_error = str(exc)
                    rows = []
                    logger.warning(
                        "nodes/context: dataset row fetch failed for %s: %s",
                        entity_uri,
                        exc,
                    )

        dataset_out = {
            "fullName": raw_dataset["fullName"],
            "key_column": key_col,
            "key_column_missing": key_col_missing,
            "description": (raw_dataset.get("description") or "").strip() or None,
            "rows": rows,
        }
        return dataset_out, fetch_error

    @staticmethod
    async def _resolve_bridges(
        *,
        matched_cls: Dict[str, Any],
        raw_bridges: List[Dict[str, Any]],
        local_id: str,
        follow_bridges: bool,
        bridge_depth: int,
        session_mgr: Any,
        settings: Any,
        registry_catalog: Optional[str],
        registry_schema: Optional[str],
        registry_volume: Optional[str],
    ) -> List[Dict[str, Any]]:
        del matched_cls  # reserved for future class-scoped bridge filters
        enriched = NodeContextService.enrich_bridge_targets(
            raw_bridges,
            session_mgr=session_mgr,
            settings=settings,
            registry_catalog=registry_catalog,
            registry_schema=registry_schema,
            registry_volume=registry_volume,
        )
        bridges_out: List[Dict[str, Any]] = []
        for b in enriched:
            target_domain = b.get("target_domain", "")
            target_class_name = b.get("target_class_name", "")
            target_class_uri = b.get("target_class_uri", "")
            label = b.get("label", "")
            target_domain_description = b.get("target_domain_description", "")

            bridge_entry: Dict[str, Any] = {
                "target_domain": target_domain,
                "target_domain_description": target_domain_description,
                "target_class_name": target_class_name,
                "target_class_uri": target_class_uri,
                "label": label,
                "entities": None,
            }

            if follow_bridges and target_domain and target_class_name:
                try:
                    target_dom = DigitalTwin.resolve_domain(
                        target_domain,
                        session_mgr,
                        settings,
                        registry_catalog,
                        registry_schema,
                        registry_volume,
                        read_only=True,
                    )
                    target_store = get_graphdb(target_dom, settings)
                    if target_store:
                        target_table = effective_graph_query_table(
                            target_dom, settings, store=target_store
                        )
                        if target_table:
                            if not _SAFE_SQL_IDENT.match(target_table):
                                logger.warning(
                                    "nodes/context: invalid target_table %r for bridge %s — skipping",
                                    target_table,
                                    target_class_name,
                                )
                                continue
                            esc_type = sql_escape(target_class_name).lower()
                            esc_id = sql_escape(local_id).lower()
                            seed_where = (
                                f" WHERE subject IN ("
                                f"SELECT subject FROM {target_table} "
                                f"WHERE predicate = '{_RDF_TYPE}' "
                                f"AND (LOWER(object) LIKE '%#{esc_type}' "
                                f"OR LOWER(object) LIKE '%/{esc_type}'))"
                                f" AND (LOWER(subject) LIKE '%/{esc_id}%' "
                                f"OR LOWER(subject) LIKE '%#{esc_id}%')"
                            )
                            rows_bridge = target_store.bfs_traversal(
                                target_table,
                                seed_where,
                                depth=bridge_depth,
                                search=local_id,
                                entity_type=target_class_name,
                            )
                            entities = [
                                {
                                    "uri": r.get("subject", ""),
                                    "predicate": r.get("predicate", ""),
                                    "object": r.get("object", ""),
                                }
                                for r in (rows_bridge or [])
                            ]
                            bridge_entry = {
                                "target_domain": target_domain,
                                "target_domain_description": target_domain_description,
                                "target_class_name": target_class_name,
                                "target_class_uri": target_class_uri,
                                "label": label,
                                "entities": entities,
                            }
                    bridges_out.append(bridge_entry)
                except Exception as exc:
                    logger.warning(
                        "nodes/context: bridge traversal to %s/%s failed — skipping: %s",
                        target_domain,
                        target_class_name,
                        exc,
                    )
            else:
                bridges_out.append(bridge_entry)
        return bridges_out
