"""``materialize.relation_kind``: what kind of relation a name is, from the catalog."""

from __future__ import annotations

import pytest

from back.core.errors import InfrastructureError, ValidationError
from back.core.graphdb.delta import materialize

pytestmark = pytest.mark.unit

DATA = "main.Dom.T_V3_data"


class _Client:
    def __init__(self, rows=(), error=None):
        self.queries: list[str] = []
        self._rows, self._error = list(rows), error

    def execute_query(self, sql):
        self.queries.append(sql)
        if self._error is not None:
            raise self._error
        return self._rows


@pytest.mark.parametrize(
    ("table_type", "kind"),
    [
        ("MATERIALIZED_VIEW", "mv"),
        ("VIEW", "view"),
        ("STREAMING_TABLE", "streaming_table"),
        ("MANAGED", "table"),
        ("EXTERNAL", "table"),
    ],
)
def test_the_catalog_table_type_names_the_kind(table_type, kind):
    assert materialize.relation_kind(_Client([{"table_type": table_type}]), DATA) == kind


def test_the_lookup_reads_the_information_schema_lower_cased():
    client = _Client()

    materialize.relation_kind(client, DATA)

    assert client.queries == [
        "SELECT table_type FROM main.information_schema.tables "
        "WHERE table_schema = 'dom' AND table_name = 't_v3_data'"
    ]


def test_no_row_is_absent():
    assert materialize.relation_kind(_Client(), DATA) == "absent"


def test_a_failed_lookup_is_not_absent():
    with pytest.raises(InfrastructureError):
        materialize.relation_kind(_Client(error=RuntimeError("no USE CATALOG")), DATA)


@pytest.mark.parametrize("name", ["", "main.dom", "main.dom.x'", "a.b.c.d"])
def test_an_unusable_name_is_refused_before_any_query(name):
    client = _Client()

    with pytest.raises(ValidationError):
        materialize.relation_kind(client, name)
    assert client.queries == []
