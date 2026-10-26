"""Contract tests for strict Graph-route LLM gating."""

import inspect

from api.routers.internal import dtwin


def test_graph_llm_routes_require_saved_domain_target():
    for route in [
        dtwin.interpret_graph_metrics,
        dtwin.dtwin_assistant_chat,
        dtwin.dtwin_assistant_chat_stream,
    ]:
        source = inspect.getsource(route)
        assert "require_domain_llm(" in source
        assert "_auto_discover_llm_endpoint(" not in source
