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
    def validate_uc_identifier(name: str, *, role: str = "identifier") -> str:
        from back.core.databricks.uc.identifiers import validate_uc_identifier

        return validate_uc_identifier(name, role=role)

    @staticmethod
    def quote_uc_identifier(name: str, *, role: str = "identifier") -> str:
        from back.core.databricks.uc.identifiers import quote_uc_identifier

        return quote_uc_identifier(name, role=role)

    @staticmethod
    def quote_uc_fqn(
        catalog: str,
        schema: str,
        table: str | None = None,
    ) -> str:
        from back.core.databricks.uc.identifiers import quote_uc_fqn

        return quote_uc_fqn(catalog, schema, table)

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
    def effective_databricks_table(domain, settings=None) -> str:
        """Fully-qualified materialized Delta TABLE (``…_data``) for Databricks backend."""
        from back.core.graphdb.delta import _table_naming

        return _table_naming.data_table_fqn(domain, settings)

    @staticmethod
    def effective_graph_name(domain) -> str:
        """Graph table/name derived from the domain name and version."""
        name = (getattr(domain, "info", None) or {}).get("name", DEFAULT_GRAPH_NAME)
        version = (
            getattr(domain, "current_version", DEFAULT_GRAPH_VERSION)
            or DEFAULT_GRAPH_VERSION
        )
        return f"{name}_V{version}"

    @staticmethod
    def effective_graph_query_table(
        domain,
        settings=None,
        *,
        include_inferred: bool = True,
        store=None,
    ) -> str:
        """Physical table/view for graph read queries (Explorer, filter, stats, …).

        * **databricks** — union VIEW ``…_graph`` (``_data`` ∪ ``_inferred``) when
          *include_inferred* is true; otherwise the materialized ``…_data`` TABLE.
        * **lakebase** — union view (``Domain_V<n>``) when *include_inferred* is true,
          otherwise the synced bulk table via *store.synced_table_name*.
        """
        from back.core.errors import ValidationError
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        backend = GraphDBFactory._resolve_triple_store_backend(domain, settings)
        if backend == "databricks":
            if include_inferred:
                from back.core.graphdb.delta import _table_naming

                table = _table_naming.graph_view_fqn(domain, settings)
            else:
                table = SQLHelpers.effective_databricks_table(domain, settings)
            if not table:
                raise ValidationError("Delta triple-store table FQN could not be resolved")
            return table

        graph_name = SQLHelpers.effective_graph_name(domain)
        if include_inferred or store is None:
            return graph_name
        return store.synced_table_name(graph_name)
