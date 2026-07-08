import re

from shared.config.constants import (
    DEFAULT_GRAPH_NAME,
    DEFAULT_GRAPH_VERSION,
    MSG_TABLE_NAME_REQUIRED,
)

from back.core.logging import get_logger

logger = get_logger(__name__)


class SQLHelpers:
    @staticmethod
    def sql_escape(value: str) -> str:
        """Escape a string value for safe embedding in SQL literals.

        Handles ``None`` gracefully and escapes both single-quotes and
        backslashes, which is the superset behaviour needed by all callers.
        """
        if value is None:
            return ""
        return str(value).replace("\\", "\\\\").replace("'", "''")

    @staticmethod
    def add_object_hash_column(select_sql: str) -> str:
        """Wrap a triple-producing SELECT so it also emits an ``object_hash`` column.

        ``managed_synced`` Lakebase tables key their primary key on
        ``object_hash`` (a SHA-256 of the literal ``object``) instead of the raw
        ``object`` column, so literals larger than the Postgres B-tree limit
        (2704 bytes) can be synced without aborting the pipeline.  ``sha2`` is a
        Spark SQL built-in; the extra column is ignored by the ``app_managed``
        read paths, which select ``subject`` / ``predicate`` / ``object``
        explicitly.

        The wrap uses a derived table so it composes with the CTE-based SQL the
        SPARQL→Spark translator emits.
        """
        inner = (select_sql or "").strip().rstrip(";")
        if not inner:
            return select_sql
        return (
            "SELECT _obh.*, "
            "sha2(CAST(_obh.object AS STRING), 256) AS object_hash\n"
            f"FROM (\n{inner}\n) _obh"
        )

    @staticmethod
    def validate_table_name(table_name: str) -> None:
        """Raise :class:`~back.core.errors.ValidationError` if *table_name* is empty.

        Centralises the guard that every triple-store method must perform.
        """
        if not table_name or not table_name.strip():
            from back.core.errors import ValidationError

            raise ValidationError(MSG_TABLE_NAME_REQUIRED)

    @staticmethod
    def effective_view_table(domain, settings=None) -> str:
        """Fully-qualified VIEW name derived from the registry location and the domain name.

        The Delta VIEW always lives in the registry's ``catalog.schema`` and
        its name is ``triplestore_<safe_name>_V<version>``.  When *settings*
        is provided and the composed name is empty, falls back to
        ``settings.databricks_triplestore_table``.
        """
        from back.core.errors import ValidationError

        delta = getattr(domain, "delta", None) or {}
        catalog = delta.get("catalog", "")
        schema = delta.get("schema", "")
        name = (getattr(domain, "info", None) or {}).get("name", "")
        version = (
            getattr(domain, "current_version", DEFAULT_GRAPH_VERSION)
            or DEFAULT_GRAPH_VERSION
        )
        if not name:
            raise ValidationError(
                "Domain name is required to derive the triple-store view name"
            )
        safe = re.sub(r"[^a-z0-9_]", "_", name.lower())
        view_name = f"triplestore_{safe}_V{version}"
        parts = [catalog, schema, view_name]
        table = ".".join(p for p in parts if p)
        if not table and settings:
            table = getattr(settings, "databricks_triplestore_table", "")
        return table

    @staticmethod
    def effective_graph_name(domain) -> str:
        """Graph table/name derived from the domain name and version."""
        name = (getattr(domain, "info", None) or {}).get("name", DEFAULT_GRAPH_NAME)
        version = (
            getattr(domain, "current_version", DEFAULT_GRAPH_VERSION)
            or DEFAULT_GRAPH_VERSION
        )
        return f"{name}_V{version}"
