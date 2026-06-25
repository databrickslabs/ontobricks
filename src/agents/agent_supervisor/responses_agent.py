"""Mapping-engine MLflow ResponsesAgent wrappers for Model Serving.

Wraps the mapping engines as ``mlflow.pyfunc.ResponsesAgent`` models so each can
be logged and served as its own Model Serving endpoint. The Agent Bricks
Supervisor then references the endpoints by name and routes between them using
the complexity assessor's recommendation (see ``mas.py`` / ``uc_function.sql``).

Two endpoints are produced from the SAME class, parameterised by ``engine``:

* ``engine="pge"``    -> ``agent_mapping_pge`` (planner/generator/evaluator/critic)
* ``engine="simple"`` -> ``agent_auto_assignment`` (original single-agent engine)

The mapping run is long (minutes for a large domain). A serving endpoint must
not block indefinitely, so this wrapper supports two modes via
``custom_inputs.mode``:

* ``"assess"`` (default when no SQL client is supplied) - run only the
  deterministic complexity assessment and return the recommendation. Cheap,
  always fast; lets a caller preview routing without running an engine.
* ``"run"`` - execute the wrapped engine and return the mapping result. Intended
  for callers that drive it as a background task.

The heavy lifting lives unchanged in the engine packages; this is a thin,
serving-friendly adapter.
"""

import copy
from typing import Generator, Optional
from uuid import uuid4

import mlflow
from mlflow.models import ModelConfig
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

from agents.agent_supervisor.complexity import assess
from agents.agent_supervisor.engine import SupervisorEngine
from back.core.logging import get_logger

logger = get_logger(__name__)


class MappingEngineResponsesAgent(ResponsesAgent):
    """Serve one mapping engine (``pge`` or ``simple``) behind Model Serving.

    The engine is fixed per deployed endpoint via the model config key
    ``engine`` (defaults to ``"pge"``), so the same code logs two endpoints.
    """

    def __init__(self) -> None:
        try:
            cfg = ModelConfig(development_config={"engine": "pge"})
            self._engine = cfg.get("engine") or "pge"
        except Exception:  # no config bound (e.g. unit test) -> default
            self._engine = "pge"

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs, custom_outputs = [], {}
        for event in self.predict_stream(request):
            if event.type == "response.output_item.done":
                outputs.append(event.item)
            if getattr(event, "custom_outputs", None):
                custom_outputs.update(event.custom_outputs)
        return ResponsesAgentResponse(output=outputs, custom_outputs=custom_outputs)

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        ci = request.custom_inputs or {}
        metadata = copy.deepcopy(ci.get("metadata", {}))
        ontology = copy.deepcopy(ci.get("ontology", {}))
        mode = ci.get("mode") or ("run" if ci.get("client") is not None else "assess")

        if mode == "assess":
            report = assess(metadata, ontology)
            text = (
                f"Complexity {report.score:.2f} ({report.tier}). "
                f"Recommended engine: {report.recommended_engine}. {report.rationale}"
            )
            yield self._text_event(text, custom_outputs={"complexity": report.to_dict()})
            return

        if not ci.get("host") or not ci.get("token") or not ci.get("endpoint_name"):
            yield self._text_event(
                "Error: 'run' mode needs host, token, and endpoint_name in custom_inputs."
            )
            return

        result = SupervisorEngine.run(
            task="mapping",
            host=ci["host"],
            token=ci["token"],
            endpoint_name=ci["endpoint_name"],
            metadata=metadata,
            ontology=ontology,
            engine_override=ci.get("engine_override") or self._engine,
            client=ci.get("client"),
            entity_mappings=ci.get("entity_mappings"),
            relationship_mappings=ci.get("relationship_mappings"),
            documents=ci.get("documents"),
        )
        yield self._text_event(
            f"Mapping run via '{result.engine_used}' engine - success={result.success}.",
            custom_outputs=result.to_dict(),
        )

    def _text_event(
        self, text: str, custom_outputs: Optional[dict] = None
    ) -> ResponsesAgentStreamEvent:
        return ResponsesAgentStreamEvent(
            type="response.output_item.done",
            item=self.create_text_output_item(text=text, id=f"msg_{uuid4().hex[:8]}"),
            custom_outputs=custom_outputs or {},
        )


agent = MappingEngineResponsesAgent()
mlflow.models.set_model(agent)
