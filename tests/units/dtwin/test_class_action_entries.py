"""Allow-list shape for UC function vs Job class actions."""

from back.objects.digitaltwin.node.NodeContextService import NodeContextService


def test_uc_function_entries_keep_full_name():
    entries = NodeContextService.class_action_entries(
        {
            "actions": [
                {
                    "fullName": "main.ops.recompute_risk",
                    "function": "recompute_risk",
                    "returns_table": False,
                }
            ]
        }
    )
    assert entries == [
        {
            "kind": "uc_function",
            "fullName": "main.ops.recompute_risk",
            "function": "recompute_risk",
            "description": None,
            "returns_table": False,
        }
    ]


def test_job_entries_use_job_prefix_and_positive_id():
    entries = NodeContextService.class_action_entries(
        {
            "actions": [
                {
                    "kind": "job",
                    "job_id": 42,
                    "job_name": "enrich-customer",
                    "description": "Ticket",
                }
            ]
        }
    )
    assert entries[0]["kind"] == "job"
    assert entries[0]["job_id"] == 42
    assert entries[0]["fullName"] == "job:42"
    assert entries[0]["function"] == "enrich-customer"


def test_job_inferred_from_job_id_without_kind():
    entries = NodeContextService.class_action_entries(
        {"actions": [{"job_id": "99", "job_name": "x"}]}
    )
    assert entries[0]["fullName"] == "job:99"


def test_invalid_job_id_and_notebook_path_are_dropped():
    entries = NodeContextService.class_action_entries(
        {
            "actions": [
                {"kind": "job", "job_id": 0},
                {"kind": "job", "job_id": -3},
                {"kind": "job", "job_id": 7, "notebook_path": "/Users/me/nb"},
                {"kind": "job", "job_id": 8},
            ]
        }
    )
    assert [e["job_id"] for e in entries] == [8]


def test_uc_and_job_entries_coexist():
    entries = NodeContextService.class_action_entries(
        {
            "actions": [
                {"fullName": "a.b.c", "function": "c"},
                {"kind": "job", "job_id": 1, "job_name": "j"},
            ]
        }
    )
    assert [e["kind"] for e in entries] == ["uc_function", "job"]
