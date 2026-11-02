"""SourceReadiness: the row-tracking probe on Delta sources."""

from __future__ import annotations

import pytest

from back.core.errors import InfrastructureError, ValidationError
from back.core.streaming import SourceReadiness

pytestmark = pytest.mark.unit

_SRC = "main.sales.customers"
_OTHER = "main.sales.orders"


class _Client:
    """Answers SHOW TBLPROPERTIES / DESCRIBE HISTORY from canned rows; records SQL."""

    def __init__(self, *, props=None, history=None, fail: bool = False):
        self.calls: list[str] = []
        self._props = props or {}
        self._history = history or []
        self._fail = fail

    def execute_query(self, sql: str):
        self.calls.append(sql)
        if self._fail:
            raise RuntimeError("PERMISSION_DENIED on table")
        if sql.startswith("SHOW TBLPROPERTIES"):
            table = sql.split()[2]
            value = self._props.get(table)
            if value is None:
                return [
                    {
                        "key": "delta.enableRowTracking",
                        "value": f"Table {table} does not have property: "
                        "delta.enableRowTracking",
                    }
                ]
            return [{"key": "delta.enableRowTracking", "value": value}]
        if sql.startswith("DESCRIBE HISTORY"):
            return list(self._history)
        return []


class TestRowTracking:
    def test_the_probe_reads_the_one_property(self):
        client = _Client(props={_SRC: "true"})

        SourceReadiness.row_tracking_enabled(client, _SRC)

        assert client.calls == [f"SHOW TBLPROPERTIES {_SRC} ('delta.enableRowTracking')"]

    @pytest.mark.parametrize(("value", "expected"), [("true", True), ("TRUE", True), ("false", False)])
    def test_the_property_value_decides(self, value, expected):
        client = _Client(props={_SRC: value})

        assert SourceReadiness.row_tracking_enabled(client, _SRC) is expected

    def test_a_missing_property_is_not_tracked(self):
        assert SourceReadiness.row_tracking_enabled(_Client(), _SRC) is False

    def test_check_reports_every_source(self):
        client = _Client(props={_SRC: "true", _OTHER: "false"})

        assert SourceReadiness.check(client, [_SRC, _OTHER]) == {_SRC: True, _OTHER: False}

    def test_check_of_no_sources_is_empty(self):
        assert SourceReadiness.check(_Client(), []) == {}

    def test_a_failed_probe_is_an_infrastructure_error(self):
        with pytest.raises(InfrastructureError, match="row tracking"):
            SourceReadiness.row_tracking_enabled(_Client(fail=True), _SRC)

    @pytest.mark.parametrize("name", ["", "main.sales.t; DROP TABLE x", "a.b.c'"])
    def test_an_unsafe_name_is_refused_before_any_sql(self, name):
        client = _Client()

        with pytest.raises(ValidationError):
            SourceReadiness.row_tracking_enabled(client, name)
        assert client.calls == []
