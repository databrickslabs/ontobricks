"""Source-order contract for domain-level LLM route guards."""

import inspect

from api.routers.internal import mapping, ontology

ROUTES = [
    ontology.generate_business_rules_async,
    ontology.start_generate_detection,
    ontology.start_generate_completion,
    ontology.auto_assign_icons,
    ontology.ontology_assistant_chat,
    ontology.ontology_assistant_invoke,
    mapping.generate_sql_from_prompt,
    mapping.start_auto_assign,
    mapping.single_auto_assign,
]


def test_every_ontology_and_mapping_llm_route_uses_domain_guard():
    for route in ROUTES:
        source = inspect.getsource(route)
        assert "require_domain_llm(" in source, route.__name__
        if "create_task(" in source:
            assert source.index("require_domain_llm(") < source.index("create_task(")


def test_legacy_one_shot_generate_route_never_calls_require_domain_llm():
    """``/ontology/wizard/generate-async`` is gone (410), not merely
    guarded — it must never reach ``require_domain_llm``/``create_task``
    at all, staged detect/complete routes are the only LLM entry points."""
    source = inspect.getsource(ontology.generate_ontology_async)
    assert "require_domain_llm(" not in source
    assert "create_task(" not in source
    # Typed 410 subclass (§4 minor closure), not a raw base-error status_code kwarg.
    assert "GoneError(" in source
    assert ontology.GoneError.__name__ == "GoneError" and ontology.GoneError(
        "x"
    ).status_code == 410
