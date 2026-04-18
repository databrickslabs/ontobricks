"""
OntoBricks Ontology Assistant — MLflow ResponsesAgent wrapper.

Wraps the existing agentic loop as an ``mlflow.pyfunc.ResponsesAgent`` so it
can be logged, served, and evaluated through the Databricks Agent Framework
while the core tool-calling logic remains unchanged.

Usage (local / in-process)::

    agent = OntologyAssistantResponsesAgent()
    response = agent.predict({
        "input": [{"role": "user", "content": "Add an entity called Vehicle"}],
        "custom_inputs": {
            "host": "https://...",
            "token": "dapi...",
            "endpoint_name": "databricks-meta-llama...",
            "classes": [...],
            "properties": [...],
            "base_uri": "http://example.org/ontology#",
        },
    })

Usage (log to MLflow)::

    import mlflow
    with mlflow.start_run():
        mlflow.pyfunc.log_model(
            python_model="agents/agent_ontology_assistant/responses_agent.py",
            name="ontology-assistant",
        )
"""

import json
import copy
from typing import Generator, Optional
from uuid import uuid4

import mlflow
from mlflow.entities import SpanType
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

from back.core.logging import get_logger
from agents.agent_ontology_assistant.tools import TOOL_DEFINITIONS, TOOL_HANDLERS
from agents.tools.context import ToolContext
from agents.engine_base import call_serving_endpoint, dispatch_tool

logger = get_logger(__name__)

MAX_ITERATIONS = 20
LLM_TIMEOUT = 120


class OntologyAssistantResponsesAgent(ResponsesAgent):
    """MLflow ResponsesAgent that delegates to the OntoBricks ontology-editing
    agentic loop.

    The caller supplies domain context (classes, properties, Databricks
    credentials) via ``custom_inputs``.  The mutated ontology is returned
    in ``custom_outputs``.
    """

    # ------------------------------------------------------------------
    # ResponsesAgent interface
    # ------------------------------------------------------------------

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = []
        custom_outputs = {}

        for event in self.predict_stream(request):
            if event.type == "response.output_item.done":
                outputs.append(event.item)
            if hasattr(event, "custom_outputs") and event.custom_outputs:
                custom_outputs.update(event.custom_outputs)

        if not custom_outputs:
            custom_outputs = self._run_and_collect(request)

        return ResponsesAgentResponse(output=outputs, custom_outputs=custom_outputs)

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        ci = request.custom_inputs or {}
        host = ci.get("host", "")
        token = ci.get("token", "")
        endpoint_name = ci.get("endpoint_name", "")
        classes = copy.deepcopy(ci.get("classes", []))
        properties = copy.deepcopy(ci.get("properties", []))
        base_uri = ci.get("base_uri", "")

        user_message = self._extract_user_message(request)
        conversation_history = self._extract_history(request)

        if not user_message:
            yield self._error_event("No user message provided.")
            return

        if not host or not token or not endpoint_name:
            yield self._error_event(
                "Missing host, token, or endpoint_name in custom_inputs."
            )
            return

        ctx = ToolContext(
            host=host.rstrip("/"),
            token=token,
            ontology_classes=classes,
            ontology_properties=properties,
            ontology_base_uri=base_uri,
        )

        from agents.agent_ontology_assistant.engine import SYSTEM_PROMPT

        messages = [{"role": "system", "content": SYSTEM_PROMPT}]
        if conversation_history:
            messages.extend(conversation_history)
        messages.append({"role": "user", "content": user_message})

        for iteration in range(MAX_ITERATIONS):
            is_last = iteration == MAX_ITERATIONS - 1
            send_tools = TOOL_DEFINITIONS if not is_last else None

            llm_response = self._call_llm(
                host, token, endpoint_name, messages, send_tools
            )
            if llm_response is None:
                yield self._error_event("LLM request failed.")
                return

            choices = llm_response.get("choices", [])
            if not choices:
                yield self._error_event("Empty LLM response.")
                return

            message = choices[0].get("message", {})
            tool_calls = message.get("tool_calls")
            content = message.get("content", "") or ""

            if tool_calls:
                messages.append(message)
                for tc in tool_calls:
                    func = tc.get("function", {})
                    tool_name = func.get("name", "")
                    tool_id = tc.get("id", "")
                    raw_args = func.get("arguments", "{}")
                    try:
                        arguments = (
                            json.loads(raw_args)
                            if isinstance(raw_args, str)
                            else raw_args
                        )
                    except json.JSONDecodeError:
                        arguments = {}

                    yield ResponsesAgentStreamEvent(
                        type="response.output_item.done",
                        item=self.create_function_call_item(
                            id=f"fc_{uuid4().hex[:8]}",
                            call_id=tool_id,
                            name=tool_name,
                            arguments=json.dumps(arguments, default=str),
                        ),
                    )

                    tool_result = self._execute_tool(ctx, tool_name, arguments)

                    yield ResponsesAgentStreamEvent(
                        type="response.output_item.done",
                        item=self.create_function_call_output_item(
                            call_id=tool_id,
                            output=tool_result[:2000],
                        ),
                    )

                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_id,
                            "content": tool_result,
                        }
                    )
            else:
                msg_id = f"msg_{uuid4().hex[:8]}"
                yield ResponsesAgentStreamEvent(
                    type="response.output_item.done",
                    item=self.create_text_output_item(text=content, id=msg_id),
                )
                return

        yield self._error_event("Max iterations reached.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_and_collect(self, request: ResponsesAgentRequest) -> dict:
        """Fallback: run the original engine and return custom_outputs."""
        ci = request.custom_inputs or {}
        from agents.agent_ontology_assistant.engine import run_agent

        user_message = self._extract_user_message(request)
        result = run_agent(
            host=ci.get("host", ""),
            token=ci.get("token", ""),
            endpoint_name=ci.get("endpoint_name", ""),
            classes=copy.deepcopy(ci.get("classes", [])),
            properties=copy.deepcopy(ci.get("properties", [])),
            base_uri=ci.get("base_uri", ""),
            user_message=user_message,
            conversation_history=self._extract_history(request),
        )
        return {
            "success": result.success,
            "ontology_changed": result.ontology_changed,
            "classes": result.classes,
            "properties": result.properties,
        }

    @mlflow.trace(span_type=SpanType.LLM)
    def _call_llm(
        self,
        host: str,
        token: str,
        endpoint_name: str,
        messages: list,
        tools: Optional[list],
    ) -> Optional[dict]:
        try:
            return call_serving_endpoint(
                host,
                token,
                endpoint_name,
                messages,
                tools=tools,
                max_tokens=2048,
                temperature=0.2,
                timeout=LLM_TIMEOUT,
                trace_name="responses_agent:llm",
            )
        except Exception as exc:
            logger.error("ResponsesAgent _call_llm failed: %s", exc)
            return None

    @mlflow.trace(span_type=SpanType.TOOL)
    def _execute_tool(self, ctx: ToolContext, tool_name: str, arguments: dict) -> str:
        return dispatch_tool(
            TOOL_HANDLERS,
            ctx,
            tool_name,
            arguments,
            trace_name="responses_agent:tool",
        )

    @staticmethod
    def _extract_user_message(request: ResponsesAgentRequest) -> str:
        for item in reversed(request.input):
            dumped = item.model_dump() if hasattr(item, "model_dump") else item
            if isinstance(dumped, dict) and dumped.get("role") == "user":
                content = dumped.get("content", "")
                if isinstance(content, str):
                    return content.strip()
                if isinstance(content, list):
                    return " ".join(
                        c.get("text", "") for c in content if isinstance(c, dict)
                    ).strip()
        return ""

    @staticmethod
    def _extract_history(request: ResponsesAgentRequest) -> list:
        history = []
        items = list(request.input)
        if not items:
            return history
        for item in items[:-1]:
            dumped = item.model_dump() if hasattr(item, "model_dump") else item
            if isinstance(dumped, dict) and dumped.get("role") in ("user", "assistant"):
                history.append(
                    {
                        "role": dumped["role"],
                        "content": dumped.get("content", ""),
                    }
                )
        return history

    def _error_event(self, text: str) -> ResponsesAgentStreamEvent:
        return ResponsesAgentStreamEvent(
            type="response.output_item.done",
            item=self.create_text_output_item(
                text=f"Error: {text}",
                id=f"err_{uuid4().hex[:8]}",
            ),
        )


agent = OntologyAssistantResponsesAgent()
mlflow.models.set_model(agent)
