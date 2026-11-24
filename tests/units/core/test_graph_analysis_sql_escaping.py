"""Spark SQL literals built from user input double backslashes before quotes.

Spark treats ``\\`` as an escape, so quote-doubling alone lets ``\\'`` close
the literal early (``pattern_id`` comes straight from a URL path).
"""

import importlib.util
import sys
from pathlib import Path

import pytest

from back.core.databricks.uc.metric_sql import build_metric_view_entity_sql
from back.core.graph_analysis.community.CommunityMaterializer import (
    CommunityMaterializer,
)
from back.core.graph_analysis.community.JobCommunities import membership_batch_query
from back.core.graph_analysis.pattern.JobPatterns import (
    members_query,
    pattern_support_query,
    shapes_bulk_query,
)

pytestmark = pytest.mark.unit

PAYLOAD = "x\\' OR 1=1 --"
SPARK_LITERAL = "'x\\\\'' OR 1=1 --'"

SRC = Path(__file__).resolve().parents[3] / "src"


def test_pattern_queries_escape_backslash_and_quote():
    assert SPARK_LITERAL in pattern_support_query("cat.sch.t", PAYLOAD)
    assert SPARK_LITERAL in members_query("cat.sch.t", PAYLOAD, 0, 10)
    assert SPARK_LITERAL in shapes_bulk_query("cat.sch.t", [PAYLOAD])


def test_community_keyset_query_escapes_backslash_and_quote():
    assert f"node_uri > {SPARK_LITERAL}" in membership_batch_query("cat.sch.t", PAYLOAD, 5)


def test_metric_view_lookup_escapes_backslash_and_quote():
    entry = {"full_name": "c.s.mv", "columns": [{"name": "id"}]}
    sql = build_metric_view_entity_sql(entry, key_column="id", key_value=PAYLOAD)
    assert SPARK_LITERAL in sql


class _Store:
    def __init__(self, flavor):
        self._flavor = flavor

    def sql_flavor(self):
        return self._flavor


@pytest.mark.parametrize(
    "flavor,expected",
    [("spark", "x\\\\'' OR 1=1 --"), (None, "x\\\\'' OR 1=1 --"), ("postgres", "x\\'' OR 1=1 --")],
)
def test_community_materializer_escapes_per_store_dialect(flavor, expected):
    mat = CommunityMaterializer.__new__(CommunityMaterializer)
    mat._store = _Store(flavor)
    assert mat._esc(PAYLOAD) == expected


@pytest.mark.parametrize("script", ["graph_analytics_job.py", "contract_demo.py"])
def test_standalone_job_scripts_escape_backslash(script, monkeypatch):
    name = f"_job_{script[:-3]}"
    spec = importlib.util.spec_from_file_location(name, SRC / "jobs" / script)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    assert module.sql_escape(PAYLOAD) == "x\\\\'' OR 1=1 --"
