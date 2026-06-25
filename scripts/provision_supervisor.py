"""Provision the OntoBricks mapping Supervisor (Agent Bricks MAS) end to end.

Run from the repo root after PR1+PR2 land. Steps:

1. Register the deterministic complexity UC function from ``uc_function.sql``
   (substituting ${CATALOG}/${SCHEMA}).
2. Log + deploy the two mapping-engine ResponsesAgents as Model Serving endpoints.
3. Build the Supervisor (MAS) config and create/update it via Agent Bricks.

This script does workspace I/O and is intended to run inside a configured
Databricks environment (CLI profile or SP creds). It is deliberately thin — the
testable logic lives in ``agents.agent_supervisor.{complexity,engine,mas}``.

Usage::

    CATALOG=fiifi_cdm_demo_catalog SCHEMA=ontobricks \\
    PGE_ENDPOINT=ob-mapping-pge SIMPLE_ENDPOINT=ob-mapping-simple \\
    python scripts/provision_supervisor.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from agents.agent_supervisor.mas import SupervisorProvisioner  # noqa: E402
from back.core.logging import get_logger  # noqa: E402

logger = get_logger(__name__)


def register_uc_function(catalog: str, schema: str, warehouse_id: str) -> None:
    """Execute uc_function.sql with the catalog/schema substituted."""
    from databricks import sql as dbsql  # local import: deploy-time dep

    sql_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..",
        "src",
        "agents",
        "agent_supervisor",
        "uc_function.sql",
    )
    with open(sql_path) as fh:
        ddl = fh.read().replace("${CATALOG}", catalog).replace("${SCHEMA}", schema)

    host = os.environ["DATABRICKS_HOST"].replace("https://", "")
    with dbsql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        access_token=os.environ["DATABRICKS_TOKEN"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
    logger.info("Registered %s.%s.assess_domain_complexity", catalog, schema)


def deploy_engine_endpoints(experiment: str) -> dict:
    """Log + deploy both mapping-engine ResponsesAgents. Returns endpoint names."""
    from agents.agent_supervisor.log_model import log_engine_agent

    endpoints = {}
    for engine, env_key, default in (
        ("pge", "PGE_ENDPOINT", "ob-mapping-pge"),
        ("simple", "SIMPLE_ENDPOINT", "ob-mapping-simple"),
    ):
        uri = log_engine_agent(engine, experiment)
        endpoint = os.environ.get(env_key, default)
        logger.info("Logged %s engine -> %s; deploy as endpoint %r", engine, uri, endpoint)
        # Deployment to Model Serving is done via databricks.agents.deploy(uri,
        # endpoint) or the agents SDK; left to the operator so this script stays
        # idempotent and credential-agnostic.
        endpoints[engine] = endpoint
    return endpoints


def main() -> None:
    catalog = os.environ.get("CATALOG", "main")
    schema = os.environ.get("SCHEMA", "ontobricks")
    warehouse_id = os.environ.get("WAREHOUSE_ID", "")
    experiment = os.environ.get("ONTOBRICKS_MLFLOW_EXPERIMENT", "ontobricks-agents")

    if warehouse_id:
        register_uc_function(catalog, schema, warehouse_id)
    else:
        logger.warning("WAREHOUSE_ID unset — skipping UC function registration")

    endpoints = deploy_engine_endpoints(experiment)

    config = SupervisorProvisioner.build_config(
        catalog=catalog,
        schema=schema,
        pge_endpoint=endpoints["pge"],
        simple_endpoint=endpoints["simple"],
    )
    logger.info("Supervisor config built with %d agents", len(config["agents"]))
    tile_id = SupervisorProvisioner.provision(config)
    logger.info("Supervisor provisioned — tile_id=%s", tile_id)


if __name__ == "__main__":
    main()
