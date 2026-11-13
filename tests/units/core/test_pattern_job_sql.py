"""Egocentric pattern signatures in the Lakeflow job, run against SQLite.

Same harness as the community job: the exact SQL the Spark driver runs,
executed on SQLite. Spark-only functions (``collect_list``, ``sort_array``,
``concat_ws``) are shimmed in :class:`SqliteRunner`.
"""

from typing import Dict, List

import pytest

from jobs.graph_analytics_job import (
    PATTERN_ALGORITHM,
    RDF_TYPE,
    RDFS_LABEL,
    PatternSQL,
    run_patterns,
)
from tests.units.core.test_graph_analytics_job_sql import SqliteRunner

pytestmark = pytest.mark.unit

NS = "http://ex.org/"
CUSTOMER = NS + "Customer"
ORDER = NS + "Order"
PRODUCT = NS + "Product"
HAS_ORDER = NS + "hasOrder"
HAS_PRODUCT = NS + "hasProduct"
COUNTRY = NS + "country"
ORDER_COUNT = NS + "orderCount"


def _t(s, p, o):
    return {"subject": s, "predicate": p, "object": o}


def _entity(uri, typ, label, **attrs):
    rows = [_t(uri, RDF_TYPE, typ), _t(uri, RDFS_LABEL, label)]
    for pred, val in attrs.items():
        rows.append(_t(uri, pred, val))
    return rows


def alice_bob_carol() -> List[Dict[str, str]]:
    alice, bob, carol = NS + "Alice", NS + "Bob", NS + "Carol"
    o1, o2, o3, o4, o5 = [NS + f"O{i}" for i in range(1, 6)]
    phone, case, laptop = NS + "Phone", NS + "Case", NS + "Laptop"
    rows = []
    rows += _entity(alice, CUSTOMER, "Alice", **{COUNTRY: "FR", ORDER_COUNT: "2"})
    rows += _entity(bob, CUSTOMER, "Bob", **{COUNTRY: "FR", ORDER_COUNT: "2"})
    rows += _entity(carol, CUSTOMER, "Carol", **{COUNTRY: "US", ORDER_COUNT: "1"})
    for o, owner in ((o1, alice), (o2, alice), (o3, bob), (o4, bob), (o5, carol)):
        rows += _entity(o, ORDER, o.rsplit("/", 1)[-1])
        rows.append(_t(owner, HAS_ORDER, o))
    for o, prod in ((o1, phone), (o2, case), (o3, phone), (o4, case), (o5, laptop)):
        rows += _entity(prod, PRODUCT, prod.rsplit("/", 1)[-1])
        rows.append(_t(o, HAS_PRODUCT, prod))
    return rows


def multiplicity_fixture() -> List[Dict[str, str]]:
    """Two customers with the same typed shape, 2 vs 5 Orders all -> Phone.

    No numeric attribute on purpose: a bin on ``orderCount`` would put 2 and 5
    in different buckets and the refine could never merge them.
    """
    rows = []
    phone = NS + "Phone"
    rows += _entity(phone, PRODUCT, "Phone")
    for name, n_orders in (("Dana", 2), ("Eve", 5)):
        cust = NS + name
        rows += _entity(cust, CUSTOMER, name, **{COUNTRY: "FR"})
        for i in range(n_orders):
            o = NS + f"{name}O{i}"
            rows += _entity(o, ORDER, o.rsplit("/", 1)[-1])
            rows.append(_t(cust, HAS_ORDER, o))
            rows.append(_t(o, HAS_PRODUCT, phone))
    return rows


def _builder(**kwargs) -> PatternSQL:
    return PatternSQL(
        source_table="triples",
        work_prefix="w",
        output_table="out",
        class_filter=[CUSTOMER],
        hops=kwargs.pop("hops", 2),
        bins=kwargs.pop("bins", 8),
        min_support=kwargs.pop("min_support", 1),
        refine_top=kwargs.pop("refine_top", 0),
        **kwargs,
    )


def _run(triples, **kwargs):
    runner = SqliteRunner(triples)
    builder = _builder(**kwargs)
    stats = run_patterns(
        runner.execute, runner.scalar, builder, cleanup=False, computed_at="t0"
    )
    return runner, builder, stats


class TestAliceBobCarol:
    def test_alice_and_bob_share_a_pattern(self):
        runner, _, stats = _run(alice_bob_carol())
        rows = {
            r["anchor_uri"]: r["pattern_id"]
            for r in runner.rows("SELECT anchor_uri, pattern_id FROM out")
        }
        assert rows[NS + "Alice"] == rows[NS + "Bob"]
        assert rows[NS + "Alice"] != rows[NS + "Carol"]
        assert stats["pattern_count"] == 2
        assert stats["anchor_count"] == 3
        assert stats["algorithm"] == PATTERN_ALGORITHM

    def test_hashes_are_stable_across_runs(self):
        first, _, _ = _run(alice_bob_carol())
        second, _, _ = _run(alice_bob_carol())
        q = "SELECT anchor_uri, pattern_id FROM out ORDER BY anchor_uri"
        assert first.rows(q) == second.rows(q)

    def test_min_support_drops_carol(self):
        runner, _, stats = _run(alice_bob_carol(), min_support=2)
        uris = {r["anchor_uri"] for r in runner.rows("SELECT anchor_uri FROM out")}
        assert uris == {NS + "Alice", NS + "Bob"}
        assert stats["pattern_count"] == 1

    def test_shapes_table_counts_members(self):
        runner, _, _ = _run(alice_bob_carol())
        sizes = sorted(
            r["support"] for r in runner.rows("SELECT support FROM out_shapes")
        )
        assert sizes == [1, 2]

    def test_missing_class_filter_raises(self):
        runner = SqliteRunner(alice_bob_carol())
        builder = PatternSQL(
            source_table="triples", work_prefix="w", output_table="out", class_filter=[]
        )
        with pytest.raises(ValueError, match="class_filter"):
            run_patterns(runner.execute, runner.scalar, builder)


class TestRefine:
    def test_refine_off_keeps_multiplicity_patterns_apart(self):
        runner, _, stats = _run(multiplicity_fixture(), hops=2, min_support=1, refine_top=0)
        assert stats["pattern_count"] == 2
        assert stats["refined"] == 0

    def test_refine_merges_multiplicity_near_duplicates(self):
        runner, _, stats = _run(multiplicity_fixture(), hops=2, min_support=1, refine_top=20)
        ids = {r["pattern_id"] for r in runner.rows("SELECT pattern_id FROM out")}
        assert len(ids) == 1
        assert stats["pattern_count"] == 1
        assert stats["refined"] == 1
        shape = runner.rows("SELECT refined, near_duplicate_count, support FROM out_shapes")
        assert shape[0]["refined"] == 1
        assert shape[0]["near_duplicate_count"] == 2
        assert shape[0]["support"] == 2
