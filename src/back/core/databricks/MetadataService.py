"""Unity Catalog Metadata Management Service.

Handles loading, merging, and managing Unity Catalog metadata
(tables, columns, descriptions / comments).
"""

from typing import Any, Dict, List, Optional, Tuple

from back.core.logging import get_logger
from .DatabricksAuth import DatabricksAuth
from .UnityCatalog import UnityCatalog

logger = get_logger(__name__)


class MetadataService:
    """Service for managing Unity Catalog table metadata.

    Accepts either a ``UnityCatalog`` instance directly **or** the
    legacy ``(host, token, warehouse_id)`` signature for backward
    compatibility with existing call sites.
    """

    @staticmethod
    def build_metadata_dict(tables: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {"tables": tables, "table_count": len(tables)}

    @staticmethod
    def validate_metadata(metadata: Dict[str, Any]) -> Tuple[bool, str]:
        if not metadata:
            return False, "Metadata is empty"
        return True, ""

    @staticmethod
    def has_metadata(metadata: Dict[str, Any]) -> bool:
        return bool(
            metadata and metadata.get("tables") and len(metadata.get("tables", [])) > 0
        )

    @staticmethod
    def extract_catalog_schema_from_full_name(full_name: str) -> Tuple[str, str, str]:
        parts = full_name.split(".")
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        if len(parts) == 2:
            return "", parts[0], parts[1]
        return "", "", full_name

    @staticmethod
    def get_catalog_schema_from_metadata(metadata: Dict[str, Any]) -> Tuple[str, str]:
        tables = metadata.get("tables", [])
        if tables and tables[0].get("full_name"):
            cat, sch, _ = MetadataService.extract_catalog_schema_from_full_name(
                tables[0]["full_name"]
            )
            return cat, sch
        return "", ""

    def __init__(
        self,
        host: Optional[str] = None,
        token: Optional[str] = None,
        warehouse_id: Optional[str] = None,
        *,
        catalog_svc: Optional[UnityCatalog] = None,
    ) -> None:
        if catalog_svc is not None:
            self._catalog = catalog_svc
        else:
            auth = DatabricksAuth(host=host, token=token, warehouse_id=warehouse_id)
            self._catalog = UnityCatalog(auth)

    def load_schema_metadata(
        self,
        catalog: str,
        schema: str,
        existing_metadata: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """Load metadata for all tables in a schema, merging with *existing_metadata*."""
        try:
            existing_tables: list = []
            existing_names: set = set()

            if existing_metadata and existing_metadata.get("tables"):
                existing_tables = existing_metadata["tables"]
                existing_names = {t["name"] for t in existing_tables}

            uc_tables = self._catalog.get_tables(catalog, schema)
            if not uc_tables:
                return False, f"No tables found in {catalog}.{schema}", {}

            new_names = [t for t in uc_tables if t not in existing_names]
            new_tables = self._fetch_tables_metadata(catalog, schema, new_names)
            merged = existing_tables + new_tables

            metadata = {"tables": merged, "table_count": len(merged)}
            if not new_tables:
                msg = f"No new tables found. All {len(uc_tables)} tables already in metadata."
            else:
                msg = f"Added {len(new_tables)} new table(s). Total: {len(merged)} tables."
            return True, msg, metadata
        except Exception as exc:
            logger.exception("Failed to load schema metadata: %s", exc)
            return False, str(exc), {}

    def load_selected_tables(
        self,
        catalog: str,
        schema: str,
        table_names: List[str],
        existing_metadata: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """Load metadata for specific *table_names*, merging with existing."""
        try:
            if not table_names:
                return False, "No tables selected", {}

            existing_tables: list = []
            existing_names: set = set()

            if existing_metadata and existing_metadata.get("tables"):
                existing_tables = existing_metadata["tables"]
                existing_names = {t["name"] for t in existing_tables}

            new_names = [t for t in table_names if t not in existing_names]
            if not new_names:
                return (
                    True,
                    f"All {len(table_names)} selected tables already loaded.",
                    {"tables": existing_tables, "table_count": len(existing_tables)},
                )

            new_tables = self._fetch_tables_metadata(catalog, schema, new_names)
            merged = existing_tables + new_tables
            metadata = {"tables": merged, "table_count": len(merged)}
            msg = f"Added {len(new_tables)} new table(s). Total: {len(merged)} tables."
            return True, msg, metadata
        except Exception as exc:
            logger.exception("Failed to load schema metadata: %s", exc)
            return False, str(exc), {}

    def get_table_metadata(
        self, catalog: str, schema: str, table_name: str
    ) -> Dict[str, Any]:
        tables = self._fetch_tables_metadata(catalog, schema, [table_name])
        return tables[0] if tables else {}

    def refresh_table_metadata(
        self,
        catalog: str,
        schema: str,
        table_name: str,
        existing_metadata: Dict[str, Any],
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """Re-fetch metadata for *table_name* and update *existing_metadata* in place."""
        try:
            fresh = self.get_table_metadata(catalog, schema, table_name)
            if not fresh or fresh.get("error"):
                return (
                    False,
                    f"Failed to get metadata for {table_name}",
                    existing_metadata,
                )

            tables = existing_metadata.get("tables", [])
            updated = False
            for i, t in enumerate(tables):
                if t["name"] == table_name:
                    tables[i] = fresh
                    updated = True
                    break
            if not updated:
                tables.append(fresh)

            existing_metadata["tables"] = tables
            existing_metadata["table_count"] = len(tables)
            return True, f"Refreshed metadata for {table_name}", existing_metadata
        except Exception as exc:
            logger.exception("Failed to refresh table metadata: %s", exc)
            return False, str(exc), existing_metadata

    def _fetch_tables_metadata(
        self, catalog: str, schema: str, table_names: List[str]
    ) -> List[Dict[str, Any]]:
        tables: list = []
        for name in table_names:
            fqn = f"{catalog}.{schema}.{name}"
            try:
                columns = self._catalog.get_table_columns(catalog, schema, name)
                comment = self._catalog.get_table_comment(catalog, schema, name)
                tables.append(
                    {
                        "name": name,
                        "full_name": fqn,
                        "comment": comment,
                        "description": comment,
                        "columns": columns,
                    }
                )
            except Exception as exc:
                logger.warning("Could not get metadata for %s: %s", name, exc)
                tables.append(
                    {
                        "name": name,
                        "full_name": fqn,
                        "comment": "",
                        "description": "",
                        "columns": [],
                        "error": str(exc),
                    }
                )
        return tables
