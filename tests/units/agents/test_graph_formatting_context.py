from agents.tools.graph_formatting import (
    format_class_context_block,
    format_find_response,
    format_node_action_response,
    format_node_context_response,
)


def test_class_context_includes_dataset_bridges_actions():
    text = format_class_context_block(
        "CUST1",
        {
            "name": "Customer",
            "dataset": {"fullName": "main.crm.customers", "key_column": "id"},
            "bridges": [{"target_domain": "Finance", "target_class_name": "Contract", "label": "Owns"}],
            "actions": [{"fullName": "main.ops.recompute_risk", "description": "Risk"}],
        },
        action_invoke_hint="call request_entity_action(entity_uri, action) to propose one",
    )
    assert "Dataset: main.crm.customers" in text
    assert "Finance / Contract" in text
    assert "main.ops.recompute_risk" in text
    assert "request_entity_action" in text


def test_class_context_empty_when_no_actions():
    assert format_class_context_block("x", {"name": "A"}) == ""


def test_format_find_appends_context_for_typed_entity():
    data = {
        "success": True,
        "seed_count": 1,
        "depth": 1,
        "total": 1,
        "triples": [
            {
                "subject": "https://ex/Customer/CUST1",
                "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "object": "https://ex/Customer",
            },
            {
                "subject": "https://ex/Customer/CUST1",
                "predicate": "http://www.w3.org/2000/01/rdf-schema#label",
                "object": "Cust One",
            },
        ],
    }
    class_actions = {
        "https://ex/Customer": {
            "name": "Customer",
            "dataset": {"fullName": "main.crm.customers", "key_column": "id"},
            "bridges": [],
            "actions": [],
        }
    }
    text = format_find_response(data, class_actions=class_actions)
    assert "[Context — class: Customer]" in text
    assert "main.crm.customers" in text


def test_node_context_and_action_formatters():
    ctx = format_node_context_response(
        {
            "success": True,
            "entity_uri": "https://ex/Customer/CUST1",
            "entity_local_id": "CUST1",
            "class_name": "Customer",
            "dataset": {"fullName": "main.crm.customers", "key_column": "id", "rows": [{"id": "CUST1"}]},
            "bridges": None,
            "actions": [{"fullName": "main.ops.recompute_risk", "description": "Risk"}],
        },
        action_invoke_hint="call request_entity_action(...)",
    )
    assert "Rows (1):" in ctx
    assert "request_entity_action" in ctx
    act = format_node_action_response(
        {"success": True, "action": "main.ops.recompute_risk", "entity_local_id": "CUST1",
         "class_name": "Customer", "rows": [{"result": 1}]}
    )
    assert "main.ops.recompute_risk" in act
    assert "result: 1" in act
