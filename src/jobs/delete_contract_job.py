"""Serverless job that deletes a contract (or fake contracts for a customer).

Bind as a class Action on Contract (``entity_id`` = contract id) or on
Customer (deletes ``FAKE-*`` rows whose ``customer_id`` matches).
"""

from __future__ import annotations

import argparse
import logging
from typing import List, Optional

try:
    from contract_demo import ContractDemoTable
except ImportError:  # pytest: src/ is on the path, script dir is not
    from jobs.contract_demo import ContractDemoTable

logger = logging.getLogger("ontobricks.jobs.delete_contract")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delete a demo contract")
    parser.add_argument(
        "--entity-id",
        default="",
        help="Contract id to delete, or customer id for FAKE-* rows",
    )
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
    stmt = store.delete_sql(args.entity_id)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.sql(store.ensure_table_sql())
    spark.sql(stmt)
    logger.info("Deleted demo contracts matching %s from %s", args.entity_id, store.table)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
