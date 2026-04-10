"""Background thread targets for metadata load/update (used by Project)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.core.databricks import DatabricksClient, MetadataService
from back.core.logging import get_logger
from back.core.task_manager import get_task_manager

logger = get_logger(__name__)


def run_metadata_load_task(
    task_id: str,
    host: str,
    token: str,
    warehouse_id: str,
    catalog: str,
    schema: str,
    selected_tables: Optional[List[str]],
    existing_metadata: Dict[str, Any],
) -> None:
    tm = get_task_manager()
    try:
        tm.start_task(task_id, "Connecting to Unity Catalog...")
        service = MetadataService(host=host, token=token, warehouse_id=warehouse_id)
        tables_count = len(selected_tables) if selected_tables else "all"
        tm.advance_step(task_id, f"Fetching metadata for {tables_count} table(s)...")
        if selected_tables is not None:
            success, message, metadata = service.load_selected_tables(
                catalog=catalog,
                schema=schema,
                table_names=selected_tables,
                existing_metadata=existing_metadata,
            )
        else:
            success, message, metadata = service.load_schema_metadata(
                catalog=catalog,
                schema=schema,
                existing_metadata=existing_metadata,
            )
        if not success:
            tm.fail_task(task_id, message)
            return
        tm.advance_step(task_id, "Finalizing...")
        total_tables = len(metadata.get("tables", []))
        tm.complete_task(
            task_id,
            result={
                "message": message,
                "total_tables": total_tables,
                "metadata": metadata,
            },
            message=f"Loaded {total_tables} tables from {catalog}.{schema}",
        )
    except Exception as e:
        logger.exception("Metadata load task failed: %s", e)
        tm.fail_task(task_id, str(e))


def run_metadata_update_task(
    task_id: str,
    host: str,
    token: str,
    warehouse_id: str,
    catalog: str,
    schema: str,
    tables_to_update: List[str],
    existing_metadata: Dict[str, Any],
    existing_tables: Dict[str, Any],
) -> None:
    tm = get_task_manager()
    try:
        tm.start_task(task_id, "Connecting to Unity Catalog...")
        client = DatabricksClient(host=host, token=token, warehouse_id=warehouse_id)
        tm.advance_step(task_id, f"Updating 0/{len(tables_to_update)} tables...")
        updated_count = 0
        errors: List[str] = []
        from back.objects.project.project import merge_table_metadata

        for i, table_name in enumerate(tables_to_update):
            try:
                old_table = existing_tables[table_name]
                new_columns = client.get_table_columns(catalog, schema, table_name)
                table_comment = client.get_table_comment(catalog, schema, table_name)
                merge_table_metadata(old_table, new_columns, table_comment, catalog, schema, table_name)
                updated_count += 1
            except Exception as e:
                errors.append(f"{table_name}: {str(e)}")
            progress = int(((i + 1) / len(tables_to_update)) * 100)
            tm.update_progress(task_id, progress, f"Updated {updated_count}/{len(tables_to_update)} tables...")
        tm.advance_step(task_id, "Finalizing...")
        message = f"Updated {updated_count} of {len(tables_to_update)} tables"
        if errors:
            message += f'. Errors: {"; ".join(errors[:3])}'
        tm.complete_task(
            task_id,
            result={
                "message": message,
                "updated_count": updated_count,
                "total_count": len(tables_to_update),
                "errors": errors,
                "metadata": existing_metadata,
            },
            message=message,
        )
    except Exception as e:
        logger.exception("Metadata update task failed: %s", e)
        tm.fail_task(task_id, str(e))
