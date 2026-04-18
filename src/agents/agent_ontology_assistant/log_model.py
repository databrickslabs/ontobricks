"""
Log the Ontology Assistant ResponsesAgent to MLflow.

Run this script to register the agent as an MLflow model that can be
served via Databricks Model Serving or evaluated with Agent Evaluation.

Usage::

    # From the OntoBricks repository root
    python -m agents.agent_ontology_assistant.log_model

    # Or with a custom experiment name
    ONTOBRICKS_MLFLOW_EXPERIMENT=my-experiment python -m agents.agent_ontology_assistant.log_model
"""

import os
import sys

from back.core.logging import get_logger

logger = get_logger(__name__)

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)


def log_agent(experiment_name: str = "ontobricks-agents") -> str:
    """Log the OntologyAssistantResponsesAgent to MLflow.

    Returns the model URI (``runs:/<run_id>/ontology-assistant``).
    """
    import mlflow

    mlflow.set_experiment(experiment_name)

    input_example = {
        "input": [
            {
                "role": "user",
                "content": "Add an entity called Vehicle with attributes vin and color",
            }
        ],
        "custom_inputs": {
            "host": "https://example.cloud.databricks.com",
            "token": "dapi...",
            "endpoint_name": "databricks-meta-llama-3-3-70b-instruct",
            "classes": [],
            "properties": [],
            "base_uri": "http://example.org/ontology#",
        },
    }

    with mlflow.start_run(run_name="ontology-assistant-agent") as run:
        mlflow.pyfunc.log_model(
            python_model="agents/agent_ontology_assistant/responses_agent.py",
            name="ontology-assistant",
            input_example=input_example,
        )
        model_uri = f"runs:/{run.info.run_id}/ontology-assistant"
        logger.info("Model logged — run_id=%s", run.info.run_id)
        logger.info("Model URI: %s", model_uri)
        return model_uri


if __name__ == "__main__":
    experiment = os.getenv("ONTOBRICKS_MLFLOW_EXPERIMENT", "ontobricks-agents")
    uri = log_agent(experiment)
    logger.info(
        "Done. Use this URI to load the model: mlflow.pyfunc.load_model('%s')", uri
    )
