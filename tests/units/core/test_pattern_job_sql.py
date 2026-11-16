"""Egocentric pattern signatures in the Lakeflow job, run against SQLite.

Same harness as the community job: the exact SQL the Spark driver runs,
executed on SQLite. Spark-only functions (``collect_list``, ``sort_array``,
``concat_ws``) are shimmed in :class:`SqliteRunner`.
"""

import json
from typing import Dict, List

import pytest

from jobs.graph_analytics_job import (
    DEFAULT_PATTERN_HUB_DEGREE,
    PATTERN_ALGORITHM,
    RDF_TYPE,
    RDFS_LABEL,
    PatternSQL,
    _portable_is_numeric_sql,
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


COUNTRY_T = NS + "Country"
REGION_T = NS + "Region"
IN_COUNTRY = NS + "inCountry"
HAS_REGION = NS + "hasRegion"
FRANCE = NS + "France"


def hub_country_fixture(n_customers: int = 6, n_regions: int = 6) -> List[Dict[str, str]]:
    """Customers point at one Country, which lists many Regions.

    Walks are forward-only, so the hub is France's *outgoing* ``hasRegion``
    fan-out: expanding through it from every customer would pull in every
    region.
    """
    rows = _entity(FRANCE, COUNTRY_T, "France")
    for k in range(n_regions):
        region = NS + f"Region{k}"
        rows += _entity(region, REGION_T, f"Region{k}")
        rows.append(_t(FRANCE, HAS_REGION, region))
    for i in range(n_customers):
        cust = NS + f"Cust{i}"
        rows += _entity(cust, CUSTOMER, f"Cust{i}", **{TIER: "gold"})
        rows.append(_t(cust, IN_COUNTRY, FRANCE))
        order = NS + f"Ord{i}"
        rows += _entity(order, ORDER, f"Ord{i}")
        rows.append(_t(cust, HAS_ORDER, order))
    return rows


class TestHubPruning:
    def test_threshold_zero_walks_through_the_hub(self):
        runner, _, stats = _run(
            hub_country_fixture(6), hops=2, min_support=1, hub_degree=0
        )
        assert int(stats["hub_count"]) == 0
        f2 = {
            r["node"]
            for r in runner.rows(
                f"SELECT node FROM w_frontier_2 WHERE anchor = '{NS}Cust0'"
            )
        }
        assert any(n.startswith(NS + "Region") for n in f2)

    def test_low_threshold_stops_at_the_hub(self):
        runner, builder, stats = _run(
            hub_country_fixture(6), hops=2, min_support=1, hub_degree=3
        )
        assert int(stats["hub_count"]) == 1
        assert int(stats["hub_degree"]) == 3
        f2 = {
            r["node"]
            for r in runner.rows(
                f"SELECT node FROM w_frontier_2 WHERE anchor = '{NS}Cust0'"
            )
        }
        assert not any(n.startswith(NS + "Region") for n in f2)
        tokens = runner.rows(
            f"SELECT token FROM {builder.tokens} "
            f"WHERE anchor = '{NS}Cust0' AND kind = 's' AND hop = 1"
        )
        assert any(
            IN_COUNTRY in t["token"] and COUNTRY_T in t["token"] for t in tokens
        )

    def test_anchor_above_threshold_still_expands_at_hop_1(self):
        rows = _entity(NS + "Star", CUSTOMER, "Star")
        for i in range(5):
            o = NS + f"O{i}"
            rows += _entity(o, ORDER, f"O{i}")
            rows.append(_t(NS + "Star", HAS_ORDER, o))
        runner, _, _ = _run(rows, hops=1, min_support=1, hub_degree=2)
        f1 = {
            r["node"]
            for r in runner.rows(
                f"SELECT node FROM w_frontier_1 WHERE anchor = '{NS}Star'"
            )
        }
        assert len(f1) == 5


class TestRepresentativeShapes:
    def test_non_refined_shape_matches_member_union(self):
        runner, _, _ = _run(alice_bob_carol(), min_support=1)
        for row in runner.rows("SELECT pattern_id, shape_json, hop_count FROM out_shapes"):
            assert row["shape_json"].startswith("[")
            assert row["hop_count"] >= 0
        shared = runner.rows(
            "SELECT shape_json FROM out_shapes WHERE support = 2"
        )[0]["shape_json"]
        assert HAS_ORDER in shared
        assert HAS_PRODUCT in shared

    def test_refined_shape_still_merges(self):
        runner, _, stats = _run(
            multiplicity_fixture(), hops=2, min_support=1, refine_top=20
        )
        assert stats["refined"] == 1
        assert runner.rows("SELECT support FROM out_shapes")[0]["support"] == 2

    def test_reps_table_has_one_row_per_pattern(self):
        runner, builder, _ = _run(alice_bob_carol(), min_support=1)
        n_patterns = runner.scalar("SELECT COUNT(*) FROM out_shapes")
        n_reps = runner.scalar(f"SELECT COUNT(*) FROM {builder.reps}")
        assert n_reps == n_patterns


class TestDenormalizedTypes:
    def test_directed_edges_carry_endpoint_types(self):
        runner, builder, _ = _run(alice_bob_carol(), hops=2, min_support=1)
        rows = runner.rows(
            f"SELECT src, dst, src_type, dst_type FROM {builder.directed} "
            f"WHERE via = '{HAS_ORDER}' LIMIT 1"
        )
        assert rows[0]["src_type"] == CUSTOMER
        assert rows[0]["dst_type"] == ORDER

    def test_shape_edges_unchanged_after_denorm(self):
        runner, _, _ = _run(alice_bob_carol())
        pid = runner.scalar(
            f"SELECT pattern_id FROM out WHERE anchor_uri = '{NS}Alice'"
        )
        edges = runner.rows(
            "SELECT hop, parent_type, via, child_type "
            f"FROM out_shape_edges WHERE pattern_id = '{pid}' ORDER BY hop, via"
        )
        assert [
            (e["hop"], e["parent_type"], e["via"], e["child_type"])
            for e in edges
        ] == [
            (1, CUSTOMER, HAS_ORDER, ORDER),
            (2, ORDER, HAS_PRODUCT, PRODUCT),
        ]


class TestSinglePassSignatures:
    def test_alice_bob_carol_ids_unchanged(self):
        runner, _, stats = _run(alice_bob_carol(), min_support=1)
        rows = {
            r["anchor_uri"]: r["pattern_id"]
            for r in runner.rows("SELECT anchor_uri, pattern_id FROM out")
        }
        assert rows[NS + "Alice"] == rows[NS + "Bob"]
        assert rows[NS + "Alice"] != rows[NS + "Carol"]
        assert stats["detail_level"] == "full"

    def test_detail_level_fallback_still_picks_structure(self):
        triples = _customers_with_one_order(12, **{TIER: lambda i: f"t{i}"})
        runner, _, stats = _run(triples, min_support=3)
        assert stats["detail_level"] == "structure"
        assert stats["pattern_count"] == 1

    def test_sigs_table_has_all_level_columns(self):
        runner, builder, _ = _run(alice_bob_carol(), min_support=1)
        cols = {
            r[1]
            for r in runner.conn.execute(
                f"PRAGMA table_info({builder.signatures})"
            ).fetchall()
        }
        assert cols >= {
            "anchor_uri",
            "sig_full",
            "sig_neighbour",
            "sig_own",
            "sig_structure",
        }


class TestStageInstrumentation:
    def test_summary_carries_stage_ms_and_hub_columns(self):
        runner, _, stats = _run(alice_bob_carol())
        summary = _summary(runner)
        stage_ms = json.loads(summary["stage_ms"])
        assert "prep" in stage_ms
        assert "bins" in stage_ms
        assert "signatures" in stage_ms
        assert "output" in stage_ms
        assert "shapes" in stage_ms
        assert all(isinstance(v, int) and v >= 0 for v in stage_ms.values())
        assert int(summary["hub_degree"]) == DEFAULT_PATTERN_HUB_DEGREE
        assert int(summary["hub_count"]) == 0
        assert "stage_ms" in stats

    def test_instrumentation_survives_a_failed_row_count(self):
        # Only instrumentation frontier COUNTs must fail; algorithm COUNTs stay.
        runner = SqliteRunner(alice_bob_carol())
        builder = _builder()
        real_scalar = runner.scalar

        def flaky(query: str):
            if "frontier" in query.lower() and "count" in query.lower():
                raise RuntimeError("boom")
            return real_scalar(query)

        runner.scalar = flaky
        stats = run_patterns(
            runner.execute, runner.scalar, builder, cleanup=False, computed_at="t0"
        )
        assert stats["pattern_count"] >= 1


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


class TestPatternCli:
    def test_parse_args_patterns_task(self):
        from jobs.graph_analytics_job import parse_args

        ns = parse_args(
            [
                "--task",
                "patterns",
                "--source-table",
                "c.s.t",
                "--output-table",
                "c.s.o",
                "--class-filter",
                CUSTOMER,
                "--pattern-hops",
                "2",
                "--refine-top",
                "10",
            ]
        )
        assert ns.task == "patterns"
        assert ns.pattern_hops == 2
        assert ns.refine_top == 10

    def test_parse_args_pattern_hub_degree(self):
        from jobs.graph_analytics_job import parse_args

        ns = parse_args(
            [
                "--task",
                "patterns",
                "--source-table",
                "c.s.t",
                "--output-table",
                "c.s.o",
                "--class-filter",
                CUSTOMER,
            ]
        )
        assert ns.pattern_hub_degree == DEFAULT_PATTERN_HUB_DEGREE


AGENT_ID = NS + "agentId"
SIGNUP_DATE = NS + "signupDate"
VERSION = NS + "version"


def mixed_literals_fixture() -> List[Dict[str, str]]:
    """Customers whose literals include IDs, dates and dotted versions.

    A real numeric attribute (``orderCount``) sits next to values that merely
    *contain* digits, ``.`` and ``-`` — the shapes that Spark's ANSI ``CAST``
    rejects (``'AGT138'``, ``'2026-11-13'``, ``'1.2.3'``).
    """
    rows = []
    specs = (
        ("Ann", "AGT138", "2026-11-13", "1.2.3", "2"),
        ("Ben", "AGT7", "2026-11-14", "1.2.4", "5"),
        ("Cy", "AGT138", "2026-11-13", "1.2.3", "9"),
        ("Dee", "AGT9", "2026-12-01", "2.0.0", "11"),
    )
    for name, agent, signup, version, count in specs:
        rows += _entity(
            NS + name,
            CUSTOMER,
            name,
            **{
                AGENT_ID: agent,
                SIGNUP_DATE: signup,
                VERSION: version,
                ORDER_COUNT: count,
            },
        )
    return rows


def _ansi_runner(triples) -> SqliteRunner:
    """A SqliteRunner whose numeric ordering cast fails like Spark ANSI mode.

    SQLite casts ``'AGT138'`` to 0.0; Spark serverless raises
    ``CAST_INVALID_INPUT``. Rewriting the cast to ``float()`` makes the harness
    reproduce that failure instead of hiding it.
    """
    runner = SqliteRunner(triples)
    runner.conn.create_function("ansi_double", 1, lambda value: float(value))
    raw_execute = runner.execute

    def execute(stmt: str) -> None:
        raw_execute(stmt.replace("CAST(value AS DOUBLE)", "ansi_double(value)"))

    runner.execute = execute
    return runner


class TestStrictCastSafety:
    def _bins(self):
        runner = _ansi_runner(mixed_literals_fixture())
        builder = _builder()
        run_patterns(
            runner.execute, runner.scalar, builder, cleanup=False, computed_at="t0"
        )
        rows = runner.rows(f"SELECT n, prop, bin_label FROM {builder.bins_table}")
        by_prop: Dict[str, set] = {}
        for r in rows:
            by_prop.setdefault(r["prop"], set()).add(r["bin_label"])
        return by_prop

    def test_non_numeric_literals_never_reach_the_numeric_cast(self):
        # Raises sqlite3.OperationalError (ansi_double) before the fix.
        self._bins()

    def test_a_real_numeric_attribute_is_still_binned(self):
        labels = self._bins()[ORDER_COUNT]
        assert labels and all(label.startswith("q") for label in labels)

    @pytest.mark.parametrize("prop", [AGENT_ID, SIGNUP_DATE, VERSION])
    def test_ids_dates_and_versions_stay_categorical(self, prop):
        labels = self._bins()[prop]
        assert not any(
            label.startswith("q") and label[1:].isdigit() for label in labels
        ), f"{prop} was binned as numeric: {labels}"


class TestPortableIsNumeric:
    """The gate in front of ``CAST(value AS DOUBLE)`` must never admit text that
    Spark's ANSI cast rejects."""

    @staticmethod
    def _is_numeric(value: str) -> bool:
        conn = SqliteRunner([]).conn
        sql = _portable_is_numeric_sql("value")
        row = conn.execute(
            f"SELECT CASE WHEN {sql} THEN 1 ELSE 0 END FROM (SELECT ? AS value)",
            (value,),
        ).fetchone()
        return bool(row[0])

    @pytest.mark.parametrize(
        "value", ["0", "42", "-5", "3.14", "-0.5", ".5", "5.", "-.5", "007"]
    )
    def test_plain_decimals_are_numeric(self, value):
        assert self._is_numeric(value)

    @pytest.mark.parametrize(
        "value",
        [
            "", "-", ".", "-.", "--", "..", "1.2.3", "5-3",
            "2026-11-13", "AGT138", "1e5", "+5", " 5", "5 ",
        ],
    )
    def test_everything_else_is_not(self, value):
        assert not self._is_numeric(value)


TIER = NS + "tier"
CODE = NS + "code"


def _customers_with_one_order(n: int, **per_customer) -> List[Dict[str, str]]:
    """*n* customers, each with exactly one Order; *per_customer* maps an
    attribute predicate to ``lambda i: value``."""
    rows: List[Dict[str, str]] = []
    for i in range(n):
        cust = NS + f"Cust{i}"
        attrs = {pred: fn(i) for pred, fn in per_customer.items()}
        rows += _entity(cust, CUSTOMER, f"Cust{i}", **attrs)
        order = NS + f"Ord{i}"
        rows += _entity(order, ORDER, f"Ord{i}")
        rows.append(_t(cust, HAS_ORDER, order))
    return rows


def _summary(runner) -> Dict[str, object]:
    return runner.rows("SELECT * FROM out_summary")[0]


class TestDetailLevels:
    """Hashing every token makes rich neighbourhoods unique; discovery falls
    back to coarser signatures until the patterns explain enough anchors."""

    def test_identifier_like_attribute_does_not_split_patterns(self):
        # 24 distinct codes: more than CATEGORICAL_MAX_VALUES, so left out.
        triples = _customers_with_one_order(24, **{CODE: lambda i: f"C{i:03d}"})
        runner, _, stats = _run(triples, min_support=3)
        assert stats["detail_level"] == "full"
        assert stats["pattern_count"] == 1
        assert stats["anchor_count"] == 24

    def test_falls_back_to_structure_when_attributes_are_noise(self):
        # 12 distinct tiers (kept as an attribute): unique at every level that
        # includes attributes, identical once only the structure is hashed.
        triples = _customers_with_one_order(12, **{TIER: lambda i: f"t{i}"})
        runner, _, stats = _run(triples, min_support=3)
        assert stats["detail_level"] == "structure"
        assert stats["pattern_count"] == 1
        assert stats["anchor_count"] == 12
        assert _summary(runner)["detail_level"] == "structure"

    def test_shape_only_lists_tokens_of_the_chosen_level(self):
        triples = _customers_with_one_order(12, **{TIER: lambda i: f"t{i}"})
        runner, _, _ = _run(triples, min_support=3)
        shape = runner.rows("SELECT shape_json FROM out_shapes")[0]["shape_json"]
        assert "tier" not in shape
        assert HAS_ORDER in shape

    def test_stays_exact_when_exact_patterns_explain_enough(self):
        runner, _, stats = _run(alice_bob_carol(), min_support=2)
        assert stats["detail_level"] == "full"
        assert stats["anchor_count"] == 2

    def test_coarser_levels_ignore_multiplicity(self):
        # Dana has 2 orders, Eve has 5: different multisets, same set.
        runner, _, stats = _run(multiplicity_fixture(), min_support=2)
        assert stats["detail_level"] == "neighbour"
        assert stats["pattern_count"] == 1

    def test_anchors_without_relations_form_an_isolated_pattern(self):
        triples: List[Dict[str, str]] = []
        for i in range(4):
            triples += _entity(
                NS + f"Lone{i}", CUSTOMER, f"Lone{i}", **{TIER: f"t{i}"}
            )
        runner, _, stats = _run(triples, min_support=3)
        assert stats["detail_level"] == "structure"
        assert stats["pattern_count"] == 1
        assert runner.rows("SELECT hop_count FROM out_shapes")[0]["hop_count"] == 0

    def test_refine_is_skipped_below_the_finest_level(self):
        runner, _, stats = _run(
            multiplicity_fixture(), min_support=2, refine_top=5
        )
        assert stats["detail_level"] == "neighbour"
        assert stats["refined"] == 0


class TestShapeEdges:
    """Typed edges per pattern feed the diagram on the Patterns page."""

    def _edges(self, runner, anchor_name):
        pid = runner.scalar(
            f"SELECT pattern_id FROM out WHERE anchor_uri = '{NS}{anchor_name}'"
        )
        return runner.rows(
            "SELECT hop, parent_type, via, child_type, member_count, "
            f"edge_total FROM out_shape_edges WHERE pattern_id = '{pid}' "
            "ORDER BY hop, via"
        )

    def test_edges_link_parent_and_child_types_per_hop(self):
        runner, _, _ = _run(alice_bob_carol())
        edges = self._edges(runner, "Alice")
        assert [(e["hop"], e["parent_type"], e["via"], e["child_type"])
                for e in edges] == [
            (1, CUSTOMER, HAS_ORDER, ORDER),
            (2, ORDER, HAS_PRODUCT, PRODUCT),
        ]

    def test_counts_members_and_neighbours(self):
        runner, _, _ = _run(alice_bob_carol())
        hop1 = self._edges(runner, "Alice")[0]
        # Alice and Bob share the pattern, two orders each.
        assert hop1["member_count"] == 2
        assert hop1["edge_total"] == 4

    def test_walk_never_steps_back_onto_the_anchor(self):
        runner, _, _ = _run(alice_bob_carol(), hops=3)
        back = runner.rows(
            f"SELECT 1 FROM out_shape_edges WHERE child_type = '{CUSTOMER}'"
        )
        assert back == []


HAS_CONTRACT = NS + "hasContract"
CONTRACT = NS + "Contract"


class TestForwardOnlyWalk:
    """Relations are followed subject -> object and never in reverse."""

    def test_inbound_relation_is_not_walked(self):
        # Only Order -> Customer (hasCustomer) exists: the customer has no
        # outgoing relation, so its neighbourhood is empty.
        triples = _entity(NS + "Cust0", CUSTOMER, "Cust0", **{TIER: "gold"})
        triples += _entity(NS + "Ord0", ORDER, "Ord0")
        triples.append(_t(NS + "Ord0", NS + "hasCustomer", NS + "Cust0"))
        runner, _, stats = _run(triples, hops=2, min_support=1)
        assert runner.rows(
            f"SELECT node FROM w_frontier_1 WHERE anchor = '{NS}Cust0'"
        ) == []
        assert runner.rows("SELECT hop_count FROM out_shapes")[0]["hop_count"] == 0

    def test_shared_contract_does_not_invent_reverse_hasContract(self):
        """Addison-Martin bug: two customers share one Contract.

        Old bidirectional walks did ``Customer -hasContract-> Contract
        -hasContract(in)-> other Customer``, which drew a phantom
        ``Contract → hasContract → Customer`` edge. Forward-only must keep
        hop 2 empty for that path.
        """
        contract = NS + "Contract1"
        triples = _entity(contract, CONTRACT, "Contract1")
        for name in ("Addison", "Martin"):
            cust = NS + name
            triples += _entity(cust, CUSTOMER, name)
            triples.append(_t(cust, HAS_CONTRACT, contract))
        runner, _, _ = _run(triples, hops=2, min_support=1)

        # Hop 1 reaches the shared contract; hop 2 must not walk back.
        f1 = {r["node"] for r in runner.rows(
            f"SELECT node FROM w_frontier_1 WHERE anchor = '{NS}Addison'")}
        f2 = runner.rows(
            f"SELECT node FROM w_frontier_2 WHERE anchor = '{NS}Addison'")
        assert f1 == {contract}
        assert f2 == []

        edges = runner.rows(
            "SELECT hop, parent_type, via, child_type FROM out_shape_edges "
            "ORDER BY hop, via"
        )
        assert [
            (e["hop"], e["parent_type"], e["via"], e["child_type"]) for e in edges
        ] == [
            (1, CUSTOMER, HAS_CONTRACT, CONTRACT),
        ]
        assert not any(
            e["parent_type"] == CONTRACT and e["child_type"] == CUSTOMER
            for e in edges
        )

    def test_chain_hangs_off_the_predicate_that_reached_it(self):
        """Addison-Martin, second shape: K0 is reached by hasContract with no
        outgoing edge; Y0 is reached by hasPolicy and has hasContract -> Martin.

        The hop-2 edge must hang off the hasPolicy Contract only. It must not
        be drawn under the hasContract Contract, which never had that edge.
        """
        hp = NS + "hasPolicy"
        k0, y0 = NS + "K0", NS + "Y0"
        triples = _entity(NS + "Addison", CUSTOMER, "Addison")
        triples += _entity(NS + "Martin", CUSTOMER, "Martin")
        triples += _entity(k0, CONTRACT, "K0")
        triples += _entity(y0, CONTRACT, "Y0")
        triples.append(_t(NS + "Addison", HAS_CONTRACT, k0))
        triples.append(_t(NS + "Addison", hp, y0))
        triples.append(_t(y0, HAS_CONTRACT, NS + "Martin"))
        runner, _, _ = _run(triples, hops=2, min_support=1)
        pid = runner.scalar(
            f"SELECT pattern_id FROM out WHERE anchor_uri = '{NS}Addison'"
        )
        edges = runner.rows(
            "SELECT hop, parent_via, parent_type, via, child_type "
            f"FROM out_shape_edges WHERE pattern_id = '{pid}' "
            "ORDER BY hop, parent_via, via"
        )
        assert [
            (e["hop"], e["parent_via"], e["parent_type"], e["via"], e["child_type"])
            for e in edges
        ] == [
            (1, "", CUSTOMER, HAS_CONTRACT, CONTRACT),
            (1, "", CUSTOMER, hp, CONTRACT),
            (2, hp, CONTRACT, HAS_CONTRACT, CUSTOMER),
        ]

    def test_walk_stops_at_dead_ends(self):
        # Alice -> O1 -> Phone -> (nothing): hop 3 is empty, nothing is
        # reached by walking an edge backwards (Phone has an inbound Order).
        runner, _, _ = _run(alice_bob_carol(), hops=3)
        frontier_1 = {r["node"] for r in runner.rows(
            f"SELECT node FROM w_frontier_1 WHERE anchor = '{NS}Alice'")}
        frontier_2 = {r["node"] for r in runner.rows(
            f"SELECT node FROM w_frontier_2 WHERE anchor = '{NS}Alice'")}
        frontier_3 = runner.rows(
            f"SELECT node FROM w_frontier_3 WHERE anchor = '{NS}Alice'")
        assert frontier_1 == {NS + "O1", NS + "O2"}
        assert frontier_2 == {NS + "Phone", NS + "Case"}
        assert frontier_3 == []


class TestShortestDistanceShells:
    """Hop k only holds nodes not already reached at hops k-1 or k-2."""

    def test_cycle_does_not_step_back_to_an_earlier_hop(self):
        # Alice -> O1 -> Phone -> O1 (cycle): O1 is already hop 1, so hop 3
        # must not revisit it.
        triples = _entity(NS + "Alice", CUSTOMER, "Alice")
        triples += _entity(NS + "O1", ORDER, "O1")
        triples += _entity(NS + "Phone", PRODUCT, "Phone")
        triples.append(_t(NS + "Alice", HAS_ORDER, NS + "O1"))
        triples.append(_t(NS + "O1", HAS_PRODUCT, NS + "Phone"))
        triples.append(_t(NS + "Phone", NS + "soldIn", NS + "O1"))
        runner, _, _ = _run(triples, hops=3, min_support=1)

        def frontier(hop):
            return {r["node"] for r in runner.rows(
                f"SELECT node FROM w_frontier_{hop} WHERE anchor = '{NS}Alice'")}

        assert frontier(1) == {NS + "O1"}
        assert frontier(2) == {NS + "Phone"}
        assert frontier(3) == set()
