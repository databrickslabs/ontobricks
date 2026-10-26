"""Serverless job that inserts one fake contract for ``entity_id`` (customer).

Bind as a class Action on Customer (or similar). The Job must declare
``entity_id`` so OntoBricks can list and ``run_now`` it.
"""

from __future__ import annotations

import argparse
import logging
from typing import List, Optional

try:
    from contract_demo import ContractDemoTable
except ImportError:  # pytest: src/ is on the path, script dir is not
    from jobs.contract_demo import ContractDemoTable

logger = logging.getLogger("ontobricks.jobs.create_fake_contract")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Insert a fake demo contract")
    parser.add_argument("--entity-id", default="", help="Customer / parent entity id")
    parser.add_argument(
        "--contract-table",
        required=True,
        help="Delta table catalog.schema.table for demo contracts",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    args = parse_args(argv)
    store = ContractDemoTable(args.contract_table)
    row = store.fake_row(args.entity_id)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.sql(store.ensure_table_sql())
    spark.createDataFrame([row]).write.mode("append").saveAsTable(store.table)
    logger.info(
        "Inserted fake contract %s for customer %s into %s",
        row["contract_id"],
        row["customer_id"],
        store.table,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
