"""Declare and compute virtual attributes for an ontology class.

A virtual attribute is never fed by a mapping and never materialized in the
triple store. It is produced on demand by a Unity Catalog function bound to
the class, which takes exactly one parameter: the entity's local ID. One
function yields one virtual attribute per ``RETURNS TABLE`` column, or a
single one when it is scalar.

Because the declarations live in ``cls["virtualAttributes"]`` and never in
``dataProperties``, the mapping layer, R2RML, the build pipeline, GraphQL and
SHACL never see them — which is also why they are not queryable.

Kept out of :mod:`NodeContextService` so the declare/compute pair reads as one
unit; that module only adds the class matching in front of it.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.core.errors import InfrastructureError, ValidationError
from back.core.helpers import (
    SAFE_COL_IDENT,
    SAFE_SQL_IDENT,
    get_databricks_client,
    run_blocking,
    sql_escape,
)
from back.core.logging import get_logger
from back.objects.digitaltwin.DigitalTwin import DigitalTwin

logger = get_logger(__name__)

# Column alias used for a scalar function, whose single value has no name of
# its own. Mirrors the ``AS result`` alias used when invoking class actions.
SCALAR_RESULT_COLUMN = "result"

# Name of this element in the per-domain MCP context policy.
VIRTUAL_ATTRIBUTES_FEATURE = "virtual_attributes"


class VirtualAttributeService:
    """Virtual attribute declaration reading and on-demand computation."""

    @staticmethod
    def class_entries(cls: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Return the class's virtual attribute groups, dropping unusable ones.

        One entry per bound Unity Catalog function. Entries without a
        syntactically valid ``fullName``, or without a single usable attribute,
        are skipped rather than raising: one bad declaration must not cost the
        node its whole context.
        """
        entries: List[Dict[str, Any]] = []
        for group in cls.get("virtualAttributes") or []:
            if not isinstance(group, dict):
                continue
            full_name = str(group.get("fullName") or "").strip()
            if not full_name or not SAFE_SQL_IDENT.match(full_name):
                logger.warning(
                    "Skipping virtual attribute group with invalid fullName: %r",
                    full_name,
                )
                continue
            returns_table = bool(group.get("returns_table"))
            attributes = VirtualAttributeService._attribute_entries(
                group.get("attributes"), returns_table=returns_table
            )
            if not attributes:
                logger.warning(
                    "Skipping virtual attribute group %s: no usable attribute",
                    full_name,
                )
                continue
            entries.append(
                {
                    "fullName": full_name,
                    "function": str(
                        group.get("function") or full_name.rsplit(".", 1)[-1]
                    ),
                    "description": (
                        str(group.get("description") or "").strip() or None
                    ),
                    "returns_table": returns_table,
                    "attributes": attributes,
                }
            )
        return entries

    @staticmethod
    def _attribute_entries(
        raw: Any, *, returns_table: bool
    ) -> List[Dict[str, Any]]:
        """Normalize one group's attribute declarations.

        ``name`` is the attribute as the user sees it; ``column`` is where to
        read it in the function result. A scalar function has no result column
        name, so it falls back to :data:`SCALAR_RESULT_COLUMN`.
        """
        attributes: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for attr in raw or []:
            if not isinstance(attr, dict):
                continue
            name = str(attr.get("name") or "").strip()
            if not name or not SAFE_COL_IDENT.match(name) or name in seen:
                logger.warning("Skipping virtual attribute with invalid name: %r", name)
                continue
            column = str(attr.get("column") or "").strip()
            if not returns_table:
                column = SCALAR_RESULT_COLUMN
            elif not column or not SAFE_COL_IDENT.match(column):
                logger.warning(
                    "Skipping virtual attribute %r with invalid column: %r",
                    name,
                    column,
                )
                continue
            seen.add(name)
            attributes.append(
                {
                    "name": name,
                    "column": column,
                    "label": str(attr.get("label") or "").strip() or name,
                    "dataType": str(attr.get("dataType") or "").strip() or None,
                }
            )
        return attributes

    @staticmethod
    async def compute(
        domain: Any,
        settings: Any,
        *,
        entity_uri: str,
        matched_cls: Dict[str, Any],
        function_full_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Compute the virtual attributes declared on *matched_cls*.

        Returns one entry per function group, each carrying its declarations
        plus a ``values`` mapping. *function_full_name* restricts the run to a
        single group, which is what the Graph Explorer's per-group Compute
        button uses.

        A group whose function fails carries an ``error`` and leaves the others
        untouched: a broken UC function must not deny the user the attributes
        that still resolve.

        Raises:
            ValidationError: *function_full_name* is not declared on the class.
            InfrastructureError: the Databricks client is not configured.
        """
        groups = VirtualAttributeService.class_entries(matched_cls)
        requested = (function_full_name or "").strip()
        if requested:
            groups = [g for g in groups if g["fullName"] == requested]
            if not groups:
                class_name = matched_cls.get("name", "")
                logger.warning(
                    "virtual-attributes: %r is not declared on class %s — rejected",
                    requested,
                    class_name,
                )
                raise ValidationError(
                    f"Virtual attribute function {requested!r} is not configured "
                    f"on class {class_name!r}"
                )
        if not groups:
            return []

        client_db = get_databricks_client(domain, settings)
        if client_db is None:
            raise InfrastructureError("Databricks client is not configured")

        local_id = DigitalTwin.extract_local_id(entity_uri)
        results: List[Dict[str, Any]] = []
        for group in groups:
            results.append(
                await VirtualAttributeService._compute_group(
                    client_db, group, local_id=local_id, entity_uri=entity_uri
                )
            )
        return results

    @staticmethod
    async def _compute_group(
        client_db: Any,
        group: Dict[str, Any],
        *,
        local_id: str,
        entity_uri: str,
    ) -> Dict[str, Any]:
        """Run one function and map its first row onto the group's attributes."""
        full_name = group["fullName"]
        arg = f"'{sql_escape(local_id)}'"
        sql = (
            f"SELECT * FROM {full_name}({arg})"
            if group["returns_table"]
            else f"SELECT {full_name}({arg}) AS {SCALAR_RESULT_COLUMN}"
        )
        out = dict(group)
        try:
            result = await run_blocking(client_db.execute_query, sql)
        except Exception as exc:  # noqa: BLE001 — isolated per group by contract
            logger.warning(
                "virtual-attributes: %s failed for %s: %s",
                full_name,
                entity_uri,
                exc,
            )
            out["values"] = {}
            out["error"] = str(exc) or "Virtual attribute computation failed"
            return out

        rows = result if isinstance(result, list) else []
        if not rows:
            logger.info(
                "virtual-attributes: %s returned no row for %s", full_name, entity_uri
            )
            out["values"] = {}
            out["message"] = "No row returned for this entity"
            return out

        # Only the first row is used: a virtual attribute is single-valued per
        # entity, so a function returning several rows is a declaration error
        # rather than something to aggregate here.
        row = rows[0] if isinstance(rows[0], dict) else {}
        out["values"] = {
            attr["name"]: row.get(attr["column"]) for attr in group["attributes"]
        }
        if len(rows) > 1:
            out["message"] = (
                f"Function returned {len(rows)} rows; only the first one is used"
            )
        return out
