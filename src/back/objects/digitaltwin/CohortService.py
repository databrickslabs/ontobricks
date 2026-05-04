"""CohortService -- domain class owning every Cohort Discovery operation.

Extracted from :class:`DigitalTwin` so the digital-twin module stays
focused on query/build/quality and the cohort feature can grow on its
own.  Constructed with a ``DomainSession`` for the instance methods
that need the loaded ontology / saved rules; the few stateless utilities
(``cohort_probe_uc_write``) are kept as ``@staticmethod``.

The public surface intentionally matches the names previously exposed
on :class:`DigitalTwin` (``list_rules``/``save_rule``/``delete_rule``/
``dry_run``/``materialize``/``class_stats``/``edge_count``/
``node_count``/``path_trace``/``sample_values``/``explain``/
``suggest_uc_target``/``probe_uc_write``) without the redundant
``cohort_`` prefix; ``DigitalTwin`` keeps thin delegating wrappers for
backward compatibility.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from back.core.errors import NotFoundError, ValidationError
from back.core.helpers import extract_local_name
from back.core.logging import get_logger

logger = get_logger(__name__)


class CohortService:
    """Cohort Discovery operations scoped to a single domain session."""

    def __init__(self, domain: Any) -> None:
        """Bind the service to a *domain* (typically a ``DomainSession``)."""
        self._domain = domain

    # ------------------------------------------------------------------
    # Saved rule CRUD (session-backed)
    # ------------------------------------------------------------------

    def list_rules(self) -> List[Dict[str, Any]]:
        """Return all saved cohort rules for the active domain."""
        rules = getattr(self._domain, "cohort_rules", []) or []
        return list(rules)

    def save_rule(self, rule_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and upsert *rule_dict* into ``domain.cohort_rules``."""
        from back.core.graph_analysis import CohortRule

        rule = CohortRule.from_dict(rule_dict)
        errors = rule.validate()
        if errors:
            raise ValidationError(
                "Cohort rule validation failed", detail="; ".join(errors)
            )
        existing = list(getattr(self._domain, "cohort_rules", []) or [])
        replaced = False
        for i, r in enumerate(existing):
            if r.get("id") == rule.id:
                existing[i] = rule.to_dict()
                replaced = True
                break
        if not replaced:
            existing.append(rule.to_dict())
        self._domain.cohort_rules = existing
        try:
            self._domain.save()
        except AttributeError:
            pass
        return rule.to_dict()

    def delete_rule(self, rule_id: str) -> bool:
        """Remove a cohort rule by id; returns ``True`` when something was deleted."""
        existing = list(getattr(self._domain, "cohort_rules", []) or [])
        new = [r for r in existing if r.get("id") != rule_id]
        if len(new) == len(existing):
            return False
        self._domain.cohort_rules = new
        try:
            self._domain.save()
        except AttributeError:
            pass
        return True

    # ------------------------------------------------------------------
    # Engine / preview helpers
    # ------------------------------------------------------------------

    def _builder(self, store: Any, graph_name: str) -> Any:
        from back.core.graph_analysis.CohortBuilder import CohortBuilder
        from shared.config.constants import DEFAULT_BASE_URI

        ontology = getattr(self._domain, "ontology", {}) or {}
        base_uri = ontology.get("base_uri") or DEFAULT_BASE_URI
        return CohortBuilder(store, graph_name, base_uri=base_uri)

    @staticmethod
    def _result_to_dict(result: Any) -> Dict[str, Any]:
        return {
            "rule_id": result.rule_id,
            "cohorts": [
                {
                    "id": c.id,
                    "idx": c.idx,
                    "size": c.size,
                    "members": c.members,
                }
                for c in result.cohorts
            ],
            "stats": {
                "rule_id": result.stats.rule_id,
                "class_member_count": result.stats.class_member_count,
                "survivor_count": result.stats.survivor_count,
                "edge_count": result.stats.edge_count,
                "cohort_count": result.stats.cohort_count,
                "grouped_member_count": result.stats.grouped_member_count,
                "elapsed_ms": result.stats.elapsed_ms,
            },
        }

    @staticmethod
    def _enrich_members(
        payload: Dict[str, Any], store: Any, graph_name: str
    ) -> None:
        """Replace the URI strings in ``cohorts[*].members`` with
        ``{uri, id, label}`` records.

        ``id`` is the local segment after the last ``/`` or ``#`` --
        what humans typically read as the entity's identifier within
        the domain. ``label`` is best-effort; missing labels degrade
        to an empty string and are rendered as the id by the UI.
        Failures in the metadata fetch fall back to the un-enriched
        URI list -- the preview should not crash on store errors.
        """
        cohorts = payload.get("cohorts") or []
        uris = sorted({
            m for c in cohorts
            for m in (c.get("members") or [])
            if isinstance(m, str)
        })
        if not uris:
            return
        label_by_uri: Dict[str, str] = {}
        try:
            meta = store.get_entity_metadata(graph_name, uris) or []
            for row in meta:
                uri = row.get("uri", "") if isinstance(row, dict) else ""
                if uri:
                    label_by_uri[uri] = row.get("label", "") or ""
        except Exception as exc:
            logger.debug("Cohort member metadata fetch failed: %s", exc)
            return

        for cohort in cohorts:
            members = cohort.get("members") or []
            cohort["members"] = [
                {
                    "uri": m,
                    "id": extract_local_name(m),
                    "label": label_by_uri.get(m, ""),
                }
                for m in members
                if isinstance(m, str)
            ]

    def dry_run(
        self,
        rule_dict: Dict[str, Any],
        store: Any,
        graph_name: str,
    ) -> Dict[str, Any]:
        """Run the cohort engine on *rule_dict* without writing anything.

        The returned ``cohorts[*].members`` list is enriched with each
        member's ``{uri, id, label}`` so the preview can show all three
        without an extra round-trip -- labels come from ``rdfs:label``
        via ``store.get_entity_metadata``.
        """
        from back.core.graph_analysis import CohortRule

        rule = CohortRule.from_dict(rule_dict)
        builder = self._builder(store, graph_name)
        result = builder.build(rule)
        payload = self._result_to_dict(result)
        self._enrich_members(payload, store, graph_name)
        return payload

    def materialize(
        self,
        rule_id: str,
        store: Any,
        graph_name: str,
        client: Any = None,
        domain_version: str = "",
        member_label_resolver: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Re-run the engine for a saved rule and write outputs as configured.

        ``client`` is required when the rule's ``output.uc_table`` is set.
        ``member_label_resolver`` is an optional callable
        ``(uris) -> Dict[uri, label]`` used to enrich the UC rows.
        """
        from back.core.graph_analysis import CohortRule

        rules = list(getattr(self._domain, "cohort_rules", []) or [])
        match = next((r for r in rules if r.get("id") == rule_id), None)
        if not match:
            raise NotFoundError(f"Cohort rule '{rule_id}' was not found")

        rule = CohortRule.from_dict(match)
        builder = self._builder(store, graph_name)
        result = builder.build(rule)

        out: Dict[str, Any] = {
            "rule_id": rule.id,
            "cohort_count": len(result.cohorts),
            "grouped_member_count": result.stats.grouped_member_count,
            "elapsed_ms": result.stats.elapsed_ms,
            "materialized_triples": 0,
            "uc_rows_written": 0,
            "uc_table": None,
        }

        if rule.output.graph:
            try:
                triple_count = builder.materialize_to_graph(rule, result)
                out["materialized_triples"] = max(int(triple_count or 0), 0)
            except Exception as exc:
                logger.exception(
                    "Cohort materialise to graph failed for %s: %s", rule.id, exc
                )
                out["materialize_graph_error"] = str(exc)

        target = rule.output.uc_table
        if target and target.table_name:
            if client is None:
                out["materialize_uc_error"] = (
                    "Databricks SQL client is not configured"
                )
            else:
                domain_name = (
                    (self._domain.info or {}).get("name", "")
                    if hasattr(self._domain, "info")
                    else ""
                )
                labels: Dict[str, str] = {}
                if member_label_resolver is not None:
                    try:
                        all_members = sorted(
                            {m for c in result.cohorts for m in c.members}
                        )
                        labels = (
                            member_label_resolver(all_members) or {}
                        )
                    except Exception as exc:
                        logger.warning(
                            "Cohort label resolver failed: %s", exc
                        )
                try:
                    uc_count = builder.materialize_to_uc(
                        rule,
                        result,
                        client,
                        target,
                        domain_name=domain_name,
                        domain_version=str(domain_version or ""),
                        member_labels=labels,
                    )
                    out["uc_rows_written"] = max(int(uc_count or 0), 0)
                    out["uc_table"] = target.to_dict()
                except Exception as exc:
                    logger.exception(
                        "Cohort materialise to UC failed for %s: %s",
                        rule.id,
                        exc,
                    )
                    out["materialize_uc_error"] = str(exc)
                    out["uc_table"] = target.to_dict()

        return out

    # ------------------------------------------------------------------
    # Live preview helpers
    # ------------------------------------------------------------------

    def class_stats(
        self,
        class_uri: str,
        store: Any,
        graph_name: str,
    ) -> Dict[str, Any]:
        """Return ``{instance_count}`` for *class_uri* in the live graph."""
        builder = self._builder(store, graph_name)
        return {"instance_count": builder.count_class_members(class_uri)}

    def edge_count(
        self,
        rule_dict: Dict[str, Any],
        store: Any,
        graph_name: str,
    ) -> Dict[str, Any]:
        from back.core.graph_analysis import CohortLink

        class_uri = rule_dict.get("class_uri", "")
        links = [CohortLink.from_dict(lk) for lk in rule_dict.get("links", []) or []]
        combine = rule_dict.get("links_combine", "any") or "any"
        builder = self._builder(store, graph_name)
        return {"edge_count": builder.count_link_edges(class_uri, links, combine)}

    def node_count(
        self,
        rule_dict: Dict[str, Any],
        store: Any,
        graph_name: str,
    ) -> Dict[str, Any]:
        from back.core.graph_analysis import CohortCompat

        class_uri = rule_dict.get("class_uri", "")
        compatibility = [
            CohortCompat.from_dict(cc)
            for cc in rule_dict.get("compatibility", []) or []
        ]
        builder = self._builder(store, graph_name)
        matching, total = builder.count_matching_nodes(class_uri, compatibility)
        return {"matching_count": matching, "total_count": total}

    def path_trace(
        self,
        rule_dict: Dict[str, Any],
        store: Any,
        graph_name: str,
    ) -> Dict[str, Any]:
        """Per-hop frontier diagnostic for the rule's ``links``.

        Honours the rule's ``compatibility`` so the starting frontier
        matches what *Preview cohorts* sees.  Returns the structure
        emitted by :meth:`CohortBuilder.trace_paths`.
        """
        from back.core.graph_analysis import CohortCompat, CohortLink

        class_uri = rule_dict.get("class_uri", "")
        links = [CohortLink.from_dict(lk) for lk in rule_dict.get("links", []) or []]
        compatibility = [
            CohortCompat.from_dict(cc)
            for cc in rule_dict.get("compatibility", []) or []
        ]
        builder = self._builder(store, graph_name)
        return builder.trace_paths(class_uri, links, compatibility)

    def sample_values(
        self,
        class_uri: str,
        property_uri: str,
        store: Any,
        graph_name: str,
        limit: int = 20,
    ) -> Dict[str, Any]:
        builder = self._builder(store, graph_name)
        values = builder.sample_property_values(class_uri, property_uri, limit=limit)
        return {"values": values}

    def explain(
        self,
        rule_dict: Dict[str, Any],
        target: str,
        store: Any,
        graph_name: str,
    ) -> Dict[str, Any]:
        from back.core.graph_analysis import CohortRule

        rule = CohortRule.from_dict(rule_dict)
        builder = self._builder(store, graph_name)
        return builder.explain_membership(rule, target)

    # ------------------------------------------------------------------
    # UC target helpers
    # ------------------------------------------------------------------

    def suggest_uc_target(self, settings: Any = None) -> Dict[str, Any]:
        """Return a suggested UC Delta target for the active domain.

        Priority for catalog/schema:
          1. ``domain.settings.databricks.{catalog,schema}``
          2. First source-table catalog/schema in ``domain.metadata.tables``
          3. Registry catalog/schema
          4. Literal ``cohorts`` for schema as a last resort.
        """
        domain = self._domain
        info = getattr(domain, "info", {}) or {}
        domain_name = info.get("name", "") or ""
        slug = re.sub(r"[^a-z0-9]+", "_", domain_name.lower()).strip("_") or "domain"
        table_name = f"cohorts_{slug}"

        catalog = ""
        schema = ""
        provenance: Dict[str, str] = {}

        domain_settings = getattr(domain, "settings", {}) or {}
        if isinstance(domain_settings, dict):
            db_cfg = domain_settings.get("databricks", {}) or {}
            if db_cfg.get("catalog"):
                catalog = db_cfg["catalog"]
                provenance["catalog"] = "domain.settings.databricks.catalog"
            if db_cfg.get("schema"):
                schema = db_cfg["schema"]
                provenance["schema"] = "domain.settings.databricks.schema"

        if not catalog or not schema:
            metadata = getattr(domain, "catalog_metadata", {}) or {}
            tables = metadata.get("tables") if isinstance(metadata, dict) else None
            if isinstance(tables, list) and tables:
                first = tables[0] if isinstance(tables[0], dict) else None
                if first:
                    if not catalog and first.get("catalog"):
                        catalog = first["catalog"]
                        provenance["catalog"] = "first source table"
                    if not schema and first.get("schema"):
                        schema = first["schema"]
                        provenance["schema"] = "first source table"

        if not catalog or not schema:
            try:
                from back.objects.registry.RegistryService import RegistryCfg

                cfg = RegistryCfg.from_domain(domain, settings)
                if not catalog and getattr(cfg, "catalog", ""):
                    catalog = cfg.catalog
                    provenance["catalog"] = "registry"
                if not schema and getattr(cfg, "schema", ""):
                    schema = cfg.schema
                    provenance["schema"] = "registry"
            except Exception as exc:
                logger.debug("RegistryCfg.from_domain failed: %s", exc)

        if not schema:
            schema = "cohorts"
            provenance["schema"] = "fallback"

        return {
            "catalog": catalog,
            "schema": schema,
            "table_name": table_name,
            "provenance": provenance,
        }

    @staticmethod
    def probe_uc_write(
        target_dict: Dict[str, Any], client: Any
    ) -> Dict[str, Any]:
        """Run a 3-step read-only permission probe for a UC Delta target.

        Returns ``{ok, checks: [{name, status, message}]}``.  Never writes
        any data.  Backed by the SQL Warehouse via ``client.execute_query``.
        """
        if client is None:
            return {
                "ok": False,
                "checks": [
                    {
                        "name": "client",
                        "status": "error",
                        "message": "Databricks SQL client is not configured.",
                    }
                ],
            }

        catalog = (target_dict.get("catalog") or "").strip()
        schema = (target_dict.get("schema") or "").strip()
        table_name = (target_dict.get("table_name") or "").strip()
        if not catalog or not schema or not table_name:
            return {
                "ok": False,
                "checks": [
                    {
                        "name": "input",
                        "status": "error",
                        "message": "catalog, schema and table_name are required.",
                    }
                ],
            }

        checks: List[Dict[str, str]] = []

        try:
            client.execute_query(f"DESCRIBE CATALOG `{catalog}`")
            checks.append(
                {"name": "catalog", "status": "ok", "message": "Catalog exists"}
            )
        except Exception as exc:
            checks.append(
                {"name": "catalog", "status": "error", "message": str(exc)}
            )
            return {"ok": False, "checks": checks}

        try:
            client.execute_query(f"DESCRIBE SCHEMA `{catalog}`.`{schema}`")
            checks.append(
                {"name": "schema", "status": "ok", "message": "Schema exists"}
            )
        except Exception as exc:
            checks.append(
                {"name": "schema", "status": "error", "message": str(exc)}
            )
            return {"ok": False, "checks": checks}

        try:
            describe = client.execute_query(
                f"DESCRIBE TABLE `{catalog}`.`{schema}`.`{table_name}`"
            )
            cols = {(row.get("col_name") or "").strip() for row in (describe or [])}
            required = {"rule_id", "cohort_uri", "member_uri", "cohort_size"}
            missing = sorted(required - cols)
            if missing:
                checks.append(
                    {
                        "name": "table",
                        "status": "warning",
                        "message": (
                            "Table exists but is missing expected columns: "
                            + ", ".join(missing)
                        ),
                    }
                )
            else:
                checks.append(
                    {
                        "name": "table",
                        "status": "ok",
                        "message": "Existing table is compatible",
                    }
                )
        except Exception:
            try:
                client.execute_query(
                    f"SHOW GRANTS ON SCHEMA `{catalog}`.`{schema}`"
                )
                checks.append(
                    {
                        "name": "table",
                        "status": "ok",
                        "message": (
                            "Table will be created on first materialise "
                            "(schema-level grants are visible)"
                        ),
                    }
                )
            except Exception as exc:
                checks.append(
                    {
                        "name": "table",
                        "status": "warning",
                        "message": (
                            "Table does not exist and grant introspection failed: "
                            + str(exc)
                        ),
                    }
                )

        ok = all(c["status"] != "error" for c in checks)
        return {"ok": ok, "checks": checks}
