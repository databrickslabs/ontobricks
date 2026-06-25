"""Log the mapping-engine ResponsesAgents to MLflow + deploy as endpoints.

Logs ``MappingEngineResponsesAgent`` twice - once per engine - so the Agent
Bricks Supervisor can route between two Model Serving endpoints.

Usage::

    # From the OntoBricks repository root
    python -m agents.agent_supervisor.log_model

    ONTOBRICKS_MLFLOW_EXPERIMENT=my-exp python -m agents.agent_supervisor.log_model
"""

import os
import sys

from back.core.logging import get_logger

logger = get_logger(__name__)

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

_INPUT_EXAMPLE = {
    "input": [{"role": "user", "content": "Map this domain."}],
    "custom_inputs": {
        "mode": "assess",
        "metadata": {"tables": [{"name": "patients", "columns": ["id", "name"]}]},
        "ontology": {"classes": [{"name": "Patient"}], "properties": []},
    },
}


def log_engine_agent(engine: str, experiment_name: str = "ontobricks-agents") -> str:
    """Log a mapping-engine ResponsesAgent for ``engine`` ('pge' | 'simple').

    Returns the model URI ``runs:/<run_id>/mapping-<engine>``.
    """
    import mlflow

    if engine not in ("pge", "simple"):
        raise ValueError(f"engine must be 'pge' or 'simple', got {engine!r}")

    mlflow.set_experiment(experiment_name)
    artifact = f"mapping-{engine}"
    with mlflow.start_run(run_name=f"supervisor-mapping-{engine}") as run:
        mlflow.pyfunc.log_model(
            python_model="agents/agent_supervisor/responses_agent.py",
            name=artifact,
            model_config={"engine": engine},
            input_example=_INPUT_EXAMPLE,
        )
        model_uri = f"runs:/{run.info.run_id}/{artifact}"
        logger.info("Logged %s engine agent - URI: %s", engine, model_uri)
        return model_uri


if __name__ == "__main__":
    experiment = os.getenv("ONTOBRICKS_MLFLOW_EXPERIMENT", "ontobricks-agents")
    for eng in ("pge", "simple"):
        uri = log_engine_agent(eng, experiment)
        logger.info("Done %s -> %s", eng, uri)
