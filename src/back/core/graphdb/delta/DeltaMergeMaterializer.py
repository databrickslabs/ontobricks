"""``…_data`` as a Delta TABLE kept in step with the gateway VIEW by ``MERGE``."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from back.core.errors import InfrastructureError, OntoBricksError
from back.core.graphdb.delta import materialize
from back.core.helpers import SQLHelpers, WarehouseStatements, validate_table_name
from back.core.logging import get_logger

logger = get_logger(__name__)

# Row tracking + deletion vectors keep a MERGE cheap and let Lakeflow sync the
# table incrementally; the change data feed is what ``table_changes`` reads.
_TABLE_PROPERTIES = materialize.CHANGE_FEED_PROPERTIES
#: The keys of those properties, each expected to read ``true``.
_PROPERTY_KEYS = (
    "delta.enableRowTracking",
    "delta.enableDeletionVectors",
    "delta.enableChangeDataFeed",
)


class DeltaMergeMaterializer:
    """Builds ``…_data`` by ``MERGE`` when serverless pipelines are unavailable.

    The incremental-without-serverless strategy: instead of replacing the
    table (CTAS), each build merges the gateway VIEW into it, so only the rows
    that changed are rewritten and the table's change feed carries exactly
    those changes. The only I/O is the ``client`` handed to :meth:`apply`.
    """

    @staticmethod
    def ensure_table_sql(data_fqn: str, *, with_object_hash: bool = False) -> str:
        """``CREATE TABLE IF NOT EXISTS`` for the merge target.

        *with_object_hash* adds the ``object_hash`` column a Lakeflow synced
        table keys on (as the CTAS does for a synced domain).
        """
        validate_table_name(data_fqn)
        columns = "subject STRING, predicate STRING, object STRING"
        if with_object_hash:
            columns += ", object_hash STRING"
        return (
            f"CREATE TABLE IF NOT EXISTS {data_fqn} ({columns}) USING DELTA "
            f"CLUSTER BY (predicate, subject) TBLPROPERTIES {_TABLE_PROPERTIES}"
        )

    @staticmethod
    def merge_sql(view_fqn: str, data_fqn: str, *, with_object_hash: bool = False) -> str:
        """``MERGE`` the deduplicated VIEW triples into *data_fqn*; delete what left the VIEW.

        Objects match null-safely (``<=>``). With *with_object_hash* new rows
        also get ``sha2(object, 256)``, the hash the MV and the wrapped VIEW use,
        and a matched row whose ``object_hash`` is ``NULL`` gets it too: rows
        merged before the column was added (a domain that became
        Lakeflow-synced) would otherwise keep a ``NULL`` in the synced table's
        primary key. In the same statement as the merge, so a failed backfill
        is retried by the next build; once filled no row matches.
        """
        validate_table_name(view_fqn)
        validate_table_name(data_fqn)
        columns, values = "subject, predicate, object", "s.subject, s.predicate, s.object"
        backfill = ""
        if with_object_hash:
            object_hash = SQLHelpers.object_hash_sql("s.object")
            columns += ", object_hash"
            values += f", {object_hash}"
            backfill = (
                "WHEN MATCHED AND t.object_hash IS NULL "
                f"THEN UPDATE SET object_hash = {object_hash}\n"
            )
        return (
            f"MERGE INTO {data_fqn} t\n"
            f"USING (SELECT subject, predicate, object FROM {view_fqn} "
            "GROUP BY subject, predicate, object) s\n"
            "  ON t.subject = s.subject AND t.predicate = s.predicate "
            "AND t.object <=> s.object\n"
            f"{backfill}"
            f"WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})\n"
            "WHEN NOT MATCHED BY SOURCE THEN DELETE"
        )

    @staticmethod
    def fingerprint(
        source_versions: dict, r2rml: str, base_uri: str, *, view_sql: str = ""
    ) -> str:
        """SHA-256 over the source table versions, the mapping and the generated SQL.

        *view_sql* is the gateway VIEW SQL: mappings augmented from the domain
        configuration can change it while the stored R2RML stays the same.
        Independent of the order of *source_versions*.
        """
        payload = json.dumps(
            {
                "sources": {str(k): int(v) for k, v in (source_versions or {}).items()},
                "r2rml": r2rml or "",
                "base_uri": base_uri or "",
                "view_sql": view_sql or "",
            },
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def apply(
        client: Any, view_fqn: str, data_fqn: str, *, with_object_hash: bool = False
    ) -> None:
        """Make *data_fqn* a merge-maintained TABLE and merge *view_fqn* into it.

        Every statement is built (and validated) first. Then: drop a stale
        ``…_data`` materialized view or view (best-effort; ``CREATE TABLE``
        cannot replace either), create the table if absent, turn its table
        properties on when ``SHOW TBLPROPERTIES`` lacks one (a table left by a
        CTAS has none; an unreadable answer counts as lacking), add the
        ``object_hash`` column when *with_object_hash* finds a table without
        it (a domain that became Lakeflow-synced; the ``MERGE`` fills it on
        the existing rows), and ``MERGE``. A table that
        already has everything gets no metadata commit. Driver errors become
        :class:`InfrastructureError`.
        """
        ensure_sql = DeltaMergeMaterializer.ensure_table_sql(
            data_fqn, with_object_hash=with_object_hash
        )
        merge_sql = DeltaMergeMaterializer.merge_sql(
            view_fqn, data_fqn, with_object_hash=with_object_hash
        )
        properties_sql = f"ALTER TABLE {data_fqn} SET TBLPROPERTIES {_TABLE_PROPERTIES}"
        hash_column_sql = f"ALTER TABLE {data_fqn} ADD COLUMNS (object_hash STRING)"
        materialize.drop_relation(client, data_fqn, kind="materialized_view")
        materialize.drop_relation(client, data_fqn, kind="view")
        logger.info("Merging %s into %s", view_fqn, data_fqn)
        try:
            materialize.run_sql(client, ensure_sql)
            if DeltaMergeMaterializer._lacks_properties(client, data_fqn):
                materialize.run_sql(client, properties_sql)
            if with_object_hash and DeltaMergeMaterializer._lacks_object_hash(client, data_fqn):
                materialize.run_sql(client, hash_column_sql)
            materialize.run_sql(client, merge_sql)
        except OntoBricksError:
            raise
        except Exception as exc:  # noqa: BLE001 — translated, not swallowed
            raise InfrastructureError(
                f"Could not merge {view_fqn} into {data_fqn}: {exc}", detail=str(exc)
            ) from exc

    @staticmethod
    def _lacks_properties(client: Any, data_fqn: str) -> bool:
        """True unless ``SHOW TBLPROPERTIES`` reads every merge property as ``true``."""
        try:
            rows = WarehouseStatements.query(
                client, f"SHOW TBLPROPERTIES {data_fqn}", f"read the properties of {data_fqn}"
            )
        except InfrastructureError as exc:
            logger.debug("Properties of %s unreadable; setting them: %s", data_fqn, exc)
            return True
        values = {
            str(row.get("key", "")): str(row.get("value", "")).strip().lower()
            for row in rows
            if isinstance(row, dict)
        }
        return any(values.get(key) != "true" for key in _PROPERTY_KEYS)

    @staticmethod
    def _lacks_object_hash(client: Any, data_fqn: str) -> bool:
        """True when ``SHOW COLUMNS`` lists no ``object_hash`` (unreadable: ``False``)."""
        try:
            rows = WarehouseStatements.query(
                client, f"SHOW COLUMNS IN {data_fqn}", f"read the columns of {data_fqn}"
            )
        except InfrastructureError as exc:
            logger.debug("Columns of %s unreadable; not adding object_hash: %s", data_fqn, exc)
            return False
        names = {
            str(row.get("col_name", "")).strip().lower() for row in rows if isinstance(row, dict)
        }
        return "object_hash" not in names
