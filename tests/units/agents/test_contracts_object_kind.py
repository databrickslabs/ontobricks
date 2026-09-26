"""TableRole carries object_kind through the planner slice."""

from agents.agent_mapping_pge.contracts import TableRole


def test_table_role_object_kind_roundtrip():
    tr = TableRole(table="c.s.mv", object_kind="metric_view")
    assert TableRole.from_dict(tr.to_dict()).object_kind == "metric_view"


def test_table_role_default_kind():
    assert TableRole(table="c.s.t").object_kind == "table"


def test_table_role_from_dict_defaults_when_absent():
    # Legacy serialized TableRole without object_kind stays valid.
    assert TableRole.from_dict({"table": "c.s.t"}).object_kind == "table"
