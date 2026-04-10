"""Delta (Databricks SQL Warehouse) triple store backend."""
from typing import Any, Callable, Dict, List, Optional

from back.core.logging import get_logger
from back.core.triplestore.TripleStoreBackend import TripleStoreBackend
from back.core.helpers import sql_escape as _escape_sql_string, validate_table_name

logger = get_logger(__name__)


class DeltaTripleStore(TripleStoreBackend):
    """Triple store backend using Databricks Delta tables."""

    def __init__(self, client: Any) -> None:
        """Initialize with a DatabricksClient instance."""
        self.client = client

    def create_table(self, table_name: str) -> None:
        """Create the (subject, predicate, object) Delta table with Liquid Clustering."""
        validate_table_name(table_name)
        query = (
            f"CREATE TABLE IF NOT EXISTS {table_name} "
            "(subject STRING, predicate STRING, object STRING) USING DELTA "
            "CLUSTER BY (predicate, subject)"
        )
        logger.info("Creating Delta table: %s", table_name)
        self.client.execute_statement(query)

    def optimize_table(self, table_name: str) -> None:
        """Run OPTIMIZE to trigger Liquid Clustering compaction."""
        validate_table_name(table_name)
        logger.info("Optimizing Delta table: %s", table_name)
        self.client.execute_statement(f"OPTIMIZE {table_name}")

    def drop_table(self, table_name: str) -> None:
        """Drop table if exists."""
        validate_table_name(table_name)
        query = f"DROP TABLE IF EXISTS {table_name}"
        logger.info("Dropping Delta table: %s", table_name)
        self.client.execute_statement(query)

    def insert_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Batch insert triples using a single persistent connection."""
        validate_table_name(table_name)
        if not triples:
            return 0

        from databricks import sql

        total = 0
        conn_params = self.client._get_sql_connection_params()
        with sql.connect(**conn_params) as connection:
            with connection.cursor() as cursor:
                for i in range(0, len(triples), batch_size):
                    batch = triples[i : i + batch_size]
                    values_list = []
                    for t in batch:
                        s = _escape_sql_string(t.get("subject", "") or "")
                        p = _escape_sql_string(t.get("predicate", "") or "")
                        o = _escape_sql_string(t.get("object", "") or "")
                        values_list.append(f"('{s}', '{p}', '{o}')")
                    values_sql = ",\n".join(values_list)
                    query = f"INSERT INTO {table_name} (subject, predicate, object) VALUES\n{values_sql}"
                    cursor.execute(query)
                    total += len(batch)
                    if on_progress:
                        on_progress(total, len(triples))
                    logger.debug("Inserted batch %d-%d of %d", i + 1, i + len(batch), len(triples))

        logger.info("Inserted %d triples into %s", total, table_name)
        return total

    def query_triples(self, table_name: str) -> List[Dict[str, str]]:
        """SELECT all triples."""
        validate_table_name(table_name)
        query = f"SELECT subject, predicate, object FROM {table_name}"
        return self.client.execute_query(query)

    def count_triples(self, table_name: str) -> int:
        """Count triples."""
        validate_table_name(table_name)
        query = f"SELECT COUNT(*) AS cnt FROM {table_name}"
        rows = self.client.execute_query(query)
        if rows and len(rows) > 0:
            return int(rows[0].get("cnt", 0))
        return 0

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists by trying count, catch TABLE_OR_VIEW_NOT_FOUND."""
        if not table_name or not table_name.strip():
            return False
        try:
            self.count_triples(table_name)
            return True
        except Exception as e:
            error_msg = str(e)
            if "TABLE_OR_VIEW_NOT_FOUND" in error_msg or "does not exist" in error_msg.lower():
                return False
            raise

    def get_status(self, table_name: str) -> Dict[str, Any]:
        """Return dict with count, last_modified, etc."""
        validate_table_name(table_name)
        count = self.count_triples(table_name)
        status: Dict[str, Any] = {"count": count, "last_modified": None}
        try:
            detail = self.client.execute_query(f"DESCRIBE DETAIL {table_name}")
            if detail and len(detail) > 0:
                row = detail[0]
                status["last_modified"] = row.get("lastModified")
                status["path"] = row.get("path")
                status["format"] = row.get("format")
        except Exception as e:
            logger.debug("Could not get DESCRIBE DETAIL for %s: %s", table_name, e)
        return status

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute arbitrary SQL and return results."""
        return self.client.execute_query(query)
