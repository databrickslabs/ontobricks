"""Evaluate the SHACL shapes that relate to one entity's type, for that entity only.

A shape relates to an entity when it targets one of the entity's classes, an
ancestor of one of them, or no class at all. Every selected shape is checked
with the same SQL the bulk Data Quality run uses, filtered on the entity, and
reported with the values the entity actually carries on the constrained
property. See ``.planning/entity-shacl-facets/SPEC.md``.
"""

from __future__ import annotations

import json
import re
from contextlib import nullcontext
from typing import Any, Callable, Dict, List, Optional, Sequence, Set

from back.core.errors import InfrastructureError
from back.core.graphdb import get_graphdb
from back.core.graphdb.constants import RDF_TYPE
from back.core.helpers import run_blocking, sql_escape
from back.core.logging import get_logger
from back.core.w3c.rdf_utils import uri_local_name
from back.core.w3c.shacl import ShapeConditions
from back.core.w3c.shacl.constants import QUALITY_CATEGORIES, RDFS_LABEL
from back.core.w3c.shacl.SHACLService import SHACLService
from back.objects.digitaltwin.DigitalTwin import DigitalTwin
from back.objects.digitaltwin.quality.SqlQualityChecks import SqlQualityChecks
from back.objects.digitaltwin.TwinGraphAccess import TwinGraphAccess
from back.objects.ontology.Rules import Rules

logger = get_logger(__name__)

#: Observed values echoed per facet; the entity may carry many more.
MAX_VALUES = 20

#: Shape IDs are inlined as SQL literals, so anything else is refused.
SAFE_SHAPE_ID = re.compile(r"^[A-Za-z0-9_\-]+$")

SEVERITIES = ("sh:Violation", "sh:Warning", "sh:Info")

STATUSES = ("pass", "fail", "not_applicable", "not_evaluated", "error")

#: Facet display order: failures by severity, then what could not be trusted,
#: then what passed, then what did not apply.
_STATUS_RANK = {"error": 3, "not_evaluated": 4, "pass": 5, "not_applicable": 6}
_SEVERITY_RANK = {s: i for i, s in enumerate(SEVERITIES)}
_CATEGORY_RANK = {c: i for i, c in enumerate(QUALITY_CATEGORIES)}


class NodeShaclService:
    """Shape selection and entity-scoped SHACL evaluation."""

    @staticmethod
    def triplestore_table(domain: Any, settings: Any) -> str:
        return TwinGraphAccess.dataquality_table(domain, settings)

    # ------------------------------------------------------------------
    # Selection
    # ------------------------------------------------------------------

    @staticmethod
    def applicable_shapes(
        shapes: Sequence[Dict[str, Any]],
        classes: Sequence[Dict[str, Any]],
        class_names: Sequence[str],
    ) -> List[Dict[str, Any]]:
        """Enabled shapes targeting *class_names*, their ancestors, or nothing.

        Each returned shape is a copy carrying ``inherited_from``: the ancestor
        class name when selected through inheritance, else ``None``.
        """
        uri_by_name = {
            (c.get("name") or "").lower(): c.get("uri") or "" for c in classes
        }
        own: Set[str] = set()
        ancestors: Dict[str, str] = {}
        for name in class_names:
            if not name:
                continue
            lineage = Rules.class_lineage(list(classes), name)
            own.add(name.lower())
            own.add(uri_by_name.get(name.lower(), ""))
            for parent in lineage[1:]:
                ancestors.setdefault(parent.lower(), parent)
                parent_uri = uri_by_name.get(parent.lower(), "")
                if parent_uri:
                    ancestors.setdefault(parent_uri, parent)
        own.discard("")

        selected: List[Dict[str, Any]] = []
        for shape in shapes or []:
            if not isinstance(shape, dict) or shape.get("enabled", True) is False:
                continue
            target_uri = shape.get("target_class_uri") or ""
            target_name = (shape.get("target_class") or "").lower()
            if not target_uri and not target_name:
                selected.append({**shape, "inherited_from": None})
            elif target_uri in own or target_name in own:
                selected.append({**shape, "inherited_from": None})
            elif target_uri in ancestors or target_name in ancestors:
                selected.append(
                    {
                        **shape,
                        "inherited_from": ancestors.get(target_uri)
                        or ancestors.get(target_name),
                    }
                )
        return selected

    @staticmethod
    def count_applicable(domain: Any, class_name: str) -> int:
        """Number of shapes the facet check would report for *class_name*."""
        if not class_name:
            return 0
        shapes = getattr(domain, "shacl_shapes", None)
        if not isinstance(shapes, list):
            return 0
        classes = domain.get_classes() or []
        return len(NodeShaclService.applicable_shapes(shapes, classes, [class_name]))

    @staticmethod
    def evaluation_shape(shape: Dict[str, Any], own_class_uri: str) -> Dict[str, Any]:
        """Copy of *shape* targeting the entity's own class.

        The triple store only carries the asserted ``rdf:type``: a shape
        inherited from an ancestor would match no subject and silently pass.
        """
        if not shape.get("target_class_uri") or not own_class_uri:
            return dict(shape)
        return {**shape, "target_class_uri": own_class_uri}

    # ------------------------------------------------------------------
    # SQL
    # ------------------------------------------------------------------

    @staticmethod
    def _guard_sql(shape: Dict[str, Any], table: str) -> Optional[str]:
        conditions = ShapeConditions.get_conditions(shape)
        if not conditions:
            return None
        return ShapeConditions.subject_sql(
            conditions,
            ShapeConditions.get_logic(shape),
            shape.get("target_class_uri", ""),
            table,
            sql_escape,
            SHACLService._normalize_prop_uri,
        )

    @staticmethod
    def batch_sql(
        shapes: Sequence[Dict[str, Any]], table: str, entity_uri: str
    ) -> Optional[str]:
        """One ``UNION ALL`` query returning every violation row of *entity_uri*.

        Rows are ``(shape_id, kind, detail)``: ``kind = 'v'`` is a violation,
        ``kind = 'g'`` says the entity matches the shape's conditions.
        """
        uri = sql_escape(entity_uri)
        branches: List[str] = []
        for shape in shapes:
            sid = str(shape.get("id") or "")
            if not SAFE_SHAPE_ID.match(sid):
                continue
            base = SHACLService.shape_to_sql(shape, table)
            if not base:
                continue
            branches.append(
                f"SELECT '{sid}' AS shape_id, 'v' AS kind, "
                f"to_json(struct(v.*)) AS detail\n"
                f"FROM (\n{base}\n) v\nWHERE v.s = '{uri}'"
            )
            guard = NodeShaclService._guard_sql(shape, table)
            if guard:
                branches.append(
                    f"SELECT '{sid}' AS shape_id, 'g' AS kind, "
                    f"to_json(struct(g.*)) AS detail\n"
                    f"FROM (\n{guard}\n) g\nWHERE g.s = '{uri}'"
                )
        return "\nUNION ALL\n".join(branches) if branches else None

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------

    @staticmethod
    def _emit(progress: Optional[Callable[[int, str], None]], pct: int, message: str) -> None:
        if progress:
            progress(pct, message)

    @staticmethod
    async def evaluate(
        domain: Any,
        settings: Any,
        *,
        entity_uri: str,
        table: Optional[str] = None,
        progress: Optional[Callable[[int, str], None]] = None,
        snapshot_task_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Check every shape related to *entity_uri*'s type against that entity.

        Raises:
            InfrastructureError: no SQL warehouse to query, or the graph
                cannot be read (typically: never built).
        """
        table = table or NodeShaclService.triplestore_table(domain, settings)
        store = get_graphdb(domain, settings, engine="view")
        if store is None:
            raise InfrastructureError("Could not reach the SQL warehouse")

        NodeShaclService._emit(progress, 1, "Reading entity triples...")
        objects_by_pred = await NodeShaclService._entity_triples(
            store, table, entity_uri
        )
        classes = domain.get_classes() or []
        own, own_names, lineage, own_uri_by_lineage = NodeShaclService._lineage(
            entity_uri, objects_by_pred, classes
        )
        selected = NodeShaclService.applicable_shapes(
            getattr(domain, "shacl_shapes", None) or [], classes, own_names
        )
        labels = objects_by_pred.get(RDFS_LABEL) or []
        payload: Dict[str, Any] = {
            "success": True,
            "entity_uri": entity_uri,
            "entity_local_id": DigitalTwin.extract_local_id(entity_uri),
            "entity_label": labels[0] if labels else None,
            "classes": [{"name": c.get("name", ""), "uri": c.get("uri", "")} for c in own],
            "lineage": lineage,
        }
        if not selected:
            NodeShaclService._emit(progress, 100, "No SHACL shape applies to this entity")
            payload.update(summary=NodeShaclService._summary([]), facets=[])
            return payload

        predicates = set(objects_by_pred)
        eval_shapes = []
        for shape in selected:
            target = shape.get("inherited_from") or (own_names[0] if own_names else "")
            evaluated = NodeShaclService.evaluation_shape(
                shape, own_uri_by_lineage.get(target, "")
            )
            eval_shapes.append(
                SqlQualityChecks._resolve_shape_uri_for_sql(evaluated, predicates)
            )

        NodeShaclService._emit(
            progress, 5, f"Found {len(eval_shapes)} applicable shapes"
        )
        rows, errors = await NodeShaclService._collect_rows(
            store,
            eval_shapes,
            table,
            entity_uri,
            domain=domain,
            settings=settings,
            progress=progress,
            snapshot_task_id=snapshot_task_id,
        )
        facets = [
            NodeShaclService._facet(shape, evaluated, table, rows, errors, objects_by_pred)
            for shape, evaluated in zip(selected, eval_shapes)
        ]
        facets.sort(key=NodeShaclService._sort_key)
        payload.update(summary=NodeShaclService._summary(facets), facets=facets)
        logger.info(
            "nodes/shacl: entity=%s classes=%s shapes=%d fail=%d",
            payload["entity_local_id"],
            own_names,
            len(facets),
            payload["summary"]["fail"],
        )
        return payload

    @staticmethod
    async def _entity_triples(store, table: str, entity_uri: str) -> Dict[str, List[str]]:
        try:
            triples = await run_blocking(
                store.execute_query,
                f"SELECT predicate, object FROM {table} "
                f"WHERE subject = '{sql_escape(entity_uri)}'",
            ) or []
        except Exception as exc:
            logger.warning("nodes/shacl: cannot read %s: %s", table, exc)
            raise InfrastructureError(
                "Could not read the knowledge graph — is it built? "
                + SqlQualityChecks._sql_error_detail(exc)
            ) from exc
        objects_by_pred: Dict[str, List[str]] = {}
        for triple in triples:
            pred = triple.get("predicate") or ""
            if pred:
                objects_by_pred.setdefault(pred, []).append(triple.get("object"))
        return objects_by_pred

    @staticmethod
    def _lineage(entity_uri, objects_by_pred, classes):
        own = NodeShaclService._own_classes(
            entity_uri, objects_by_pred.get(RDF_TYPE, []), classes
        )
        own_names = [c.get("name", "") for c in own]
        lineage: List[str] = []
        own_uri_by_lineage: Dict[str, str] = {}
        for cls in own:
            for name in Rules.class_lineage(classes, cls.get("name", "")):
                if name not in lineage:
                    lineage.append(name)
                own_uri_by_lineage.setdefault(name, cls.get("uri", ""))
        return own, own_names, lineage, own_uri_by_lineage

    @staticmethod
    def _own_classes(
        entity_uri: str, types: List[str], classes: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        by_uri = {c.get("uri"): c for c in classes if c.get("uri")}
        own: List[Dict[str, Any]] = []
        for type_uri in types:
            cls = by_uri.get(type_uri)
            if cls is not None and cls not in own:
                own.append(cls)
        if own:
            return own
        from back.objects.digitaltwin.node.NodeContextService import NodeContextService

        matched = NodeContextService.match_ontology_class(entity_uri, classes)
        return [matched] if matched else []

    @staticmethod
    async def _collect_rows(
        store,
        eval_shapes,
        table: str,
        entity_uri: str,
        *,
        domain,
        settings,
        progress,
        snapshot_task_id,
    ):
        """Batch on the live VIEW; per-shape + log when a snapshot exists."""
        n = len(eval_shapes)
        run_each = bool(progress)
        check_table = table
        snap_cm = nullcontext()
        if progress and snapshot_task_id and n >= 2:
            from back.objects.digitaltwin.quality.DataQualitySnapshot import (
                DataQualitySnapshot,
            )

            NodeShaclService._emit(progress, 6, "Materializing DQ snapshot...")
            snap_cm = DataQualitySnapshot(
                domain, settings, table, snapshot_task_id, enabled=True
            )

        with snap_cm as snap:
            if snap is not None:
                check_table = snap.table
                if not snap.parallel:
                    run_each = False
            if run_each:
                return await NodeShaclService._run_each(
                    store, eval_shapes, check_table, entity_uri, progress
                )
            NodeShaclService._emit(
                progress, 10, f"Running {n} checks on the warehouse..."
            )
            return await NodeShaclService._run(
                store, eval_shapes, check_table, entity_uri
            )

    @staticmethod
    async def _run(store, shapes, table, entity_uri):
        """Run the batch; on failure, retry shape by shape to isolate the culprit."""
        rows: List[Dict[str, Any]] = []
        errors: Dict[str, str] = {}
        sql = NodeShaclService.batch_sql(shapes, table, entity_uri)
        if not sql:
            return rows, errors
        try:
            return (await run_blocking(store.execute_query, sql) or []), errors
        except Exception as exc:
            logger.warning("nodes/shacl: batch failed, retrying per shape: %s", exc)
        for shape in shapes:
            one = NodeShaclService.batch_sql([shape], table, entity_uri)
            if not one:
                continue
            try:
                rows.extend(await run_blocking(store.execute_query, one) or [])
            except Exception as exc:
                errors[shape.get("id", "")] = SqlQualityChecks._sql_error_detail(exc)
        return rows, errors

    @staticmethod
    async def _run_each(store, shapes, table, entity_uri, progress):
        """One warehouse query per shape so the activity log can move."""
        rows: List[Dict[str, Any]] = []
        errors: Dict[str, str] = {}
        n = len(shapes)
        for i, shape in enumerate(shapes, 1):
            label = shape.get("label") or shape.get("id") or "shape"
            pct = 10 + int(80 * i / max(n, 1))
            NodeShaclService._emit(
                progress, pct, f"Checking {label} ({i}/{n})..."
            )
            one = NodeShaclService.batch_sql([shape], table, entity_uri)
            if not one:
                continue
            try:
                rows.extend(await run_blocking(store.execute_query, one) or [])
            except Exception as exc:
                errors[str(shape.get("id") or "")] = SqlQualityChecks._sql_error_detail(
                    exc
                )
        return rows, errors

    @staticmethod
    def _facet(shape, evaluated, table, rows, errors, objects_by_pred) -> Dict[str, Any]:
        sid = str(shape.get("id") or "")
        details = [
            NodeShaclService._parse_detail(r.get("detail"))
            for r in rows
            if r.get("shape_id") == sid and r.get("kind") == "v"
        ]
        guard_matched = any(
            r.get("shape_id") == sid and r.get("kind") == "g" for r in rows
        )
        message = shape.get("message") or shape.get("label") or ""

        if not SAFE_SHAPE_ID.match(sid):
            status = "not_evaluated"
            message = "The shape ID contains characters that cannot be queried safely"
        elif not SHACLService.shape_to_sql(evaluated, table):
            status = "not_evaluated"
        elif sid in errors:
            status = "error"
            message = errors[sid]
        elif details:
            status = "fail"
        elif NodeShaclService._guard_sql(evaluated, table) and not guard_matched:
            status = "not_applicable"
        else:
            status = "pass"

        values = NodeShaclService._values(evaluated.get("property_uri", ""), objects_by_pred)
        return {
            "shape_id": sid,
            "label": shape.get("label") or sid,
            "category": shape.get("category", ""),
            "severity": shape.get("severity") or "sh:Violation",
            "target_class": shape.get("target_class") or "",
            "inherited_from": shape.get("inherited_from"),
            "property_path": shape.get("property_path") or "",
            "property_uri": shape.get("property_uri") or "",
            "shacl_type": shape.get("shacl_type", ""),
            "parameters": shape.get("parameters") or {},
            "constraint": NodeShaclService._constraint_text(shape),
            "status": status,
            "values": values[:MAX_VALUES],
            "values_truncated": len(values) > MAX_VALUES,
            "details": details if status == "fail" else [],
            "message": message,
        }

    @staticmethod
    def _values(prop_uri: str, objects_by_pred: Dict[str, List[str]]) -> List[str]:
        if not prop_uri:
            return []
        if prop_uri in objects_by_pred:
            return list(objects_by_pred[prop_uri])
        if "#" in prop_uri:
            base, local = prop_uri.rsplit("#", 1)
            swapped = base.rstrip("/") + "/" + local
            if swapped in objects_by_pred:
                return list(objects_by_pred[swapped])
        local = uri_local_name(prop_uri).lower()
        for pred, objs in objects_by_pred.items():
            if local and "w3.org" not in pred and uri_local_name(pred).lower() == local:
                return list(objs)
        return []

    @staticmethod
    def _parse_detail(raw: Any) -> Dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        try:
            parsed = json.loads(raw or "{}")
        except (TypeError, ValueError):
            return {"raw": raw}
        return parsed if isinstance(parsed, dict) else {"raw": parsed}

    @staticmethod
    def _constraint_text(shape: Dict[str, Any]) -> str:
        shacl_type = shape.get("shacl_type", "")
        if shacl_type == "sh:sparql":
            return shacl_type
        params = shape.get("parameters") or {}
        parts = [
            f"{key} {value}"
            for key, value in params.items()
            if str(key).startswith("sh:") and key != "sh:flags"
        ]
        return ", ".join(parts) or shacl_type

    @staticmethod
    def _sort_key(facet: Dict[str, Any]):
        if facet["status"] == "fail":
            rank = _SEVERITY_RANK.get(facet["severity"], len(SEVERITIES))
        else:
            rank = _STATUS_RANK[facet["status"]]
        category = _CATEGORY_RANK.get(facet["category"], len(QUALITY_CATEGORIES))
        return (rank, category, facet["label"].lower())

    @staticmethod
    def _summary(facets: List[Dict[str, Any]]) -> Dict[str, Any]:
        summary: Dict[str, Any] = {"total": len(facets)}
        summary.update({status: 0 for status in STATUSES})
        by_severity = {s: 0 for s in SEVERITIES}
        for facet in facets:
            summary[facet["status"]] += 1
            if facet["status"] == "fail" and facet["severity"] in by_severity:
                by_severity[facet["severity"]] += 1
        summary["fail_by_severity"] = by_severity
        return summary
