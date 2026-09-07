"""Unity Catalog DDL for Lakebase synced-table registration."""

from __future__ import annotations

import re
from typing import Any

from back.core.errors import InfrastructureError
from back.core.logging import get_logger

logger = get_logger(__name__)

_UC_SEGMENT_RE = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_-]*$")


def ensure_uc_schema_for_synced_table_fqn(
    client: Any,
    synced_table_uc_fqn: str,
    *,
    task_log_prefix: str = "",
) -> None:
    """Create the Unity Catalog schema for *synced_table_uc_fqn* if it does not exist.

    Synced-database-table registration requires ``catalog.schema`` to exist in
    Unity Catalog as a metastore object. A Postgres ``CREATE SCHEMA`` on
    Lakebase does **not** satisfy this — the failure

    ``Schema 'catalog.schema' does not exist``

    refers to **UC**, not PG.

    *synced_table_uc_fqn* must be exactly ``catalog.schema.table`` (three
    segments, no extra dots in segment names).
    """
    fqn = (synced_table_uc_fqn or "").strip()
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(
            "Synced table UC name must be catalog.schema.table; "
            f"got {synced_table_uc_fqn!r}"
        )
    catalog, schema_name, _table = parts
    for label, seg in (("catalog", catalog), ("schema", schema_name)):
        if not _UC_SEGMENT_RE.match(seg or ""):
            raise ValueError(f"Invalid UC {label} segment in FQN: {fqn!r}")

    # Two-part UC namespace (catalog.schema); identifiers are already validated.
    ddl = f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema_name}`"
    prefix = f"{task_log_prefix} " if task_log_prefix else ""
    logger.info("%sEnsuring UC schema exists: %s.%s", prefix, catalog, schema_name)
    try:
        client.execute_statement(ddl)
    except Exception as exc:  # noqa: BLE001
        raise InfrastructureError(
            "Could not create Unity Catalog schema (required before Lakebase "
            f"synced-table registration). Attempted SQL: {ddl!r}. "
            "Ensure the SQL Warehouse identity has CREATE SCHEMA on this catalog. "
            f"Underlying error: {exc}"
        ) from exc
