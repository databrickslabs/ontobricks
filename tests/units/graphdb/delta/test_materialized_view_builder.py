"""Unit tests for ``MaterializedViewBuilder`` (Layer 2: ``_data`` / ``_graph`` MVs)."""

from __future__ import annotations

import hashlib
import sqlite3
import sys

import pytest

from back.core.errors import ValidationError
from back.core.graphdb.delta import MaterializedViewBuilder as ExportedBuilder
from back.core.graphdb.delta import materialize
from back.core.graphdb.delta.MaterializedViewBuilder import MaterializedViewBuilder

pytestmark = pytest.mark.unit

# The module itself: the package re-exports the class under the module's name.
_BUILDER_MODULE = sys.modules[MaterializedViewBuilder.__module__]

DATA = "main.ob.triplestore_d_V1_data"
GRAPH = "main.ob.triplestore_d_V1_graph"
INFERRED = "main.ob.triplestore_d_V1_inferred"
SLOTS = "main.ob.triplestore_d_V1_slots"
BATCH = "SELECT subject, predicate, object FROM batch_src"
BATCH_WITH_CTE = (
    "WITH src_0 AS (\n    SELECT * FROM people\n  )\n"
    "SELECT subject, predicate, object FROM src_0"
)

ROW_TRACKING = "(delta.enableRowTracking = true, delta.enableDeletionVectors = true)"


def _header(fqn: str, policy: str = "INCREMENTAL", sha: str | None = None) -> str:
    extra = f", 'ontobricks.mv_sql_sha256' = '{sha}'" if sha else ""
    return (
        f"CREATE OR REPLACE MATERIALIZED VIEW {fqn}\n"
        "  CLUSTER BY (predicate, subject)\n"
        f"  REFRESH POLICY {policy}\n"
        "  TBLPROPERTIES ('pipelines.channel' = 'preview', "
        f"'delta.enableRowTracking' = 'true'{extra})\n"
    )


def _sha(body: str, policy: str = "INCREMENTAL") -> str:
    """The definition fingerprint: header template, refresh policy and ``AS SELECT`` body."""
    header = _BUILDER_MODULE._MV_HEADER
    return hashlib.sha256(f"{header}\n{policy}\n{body}".encode()).hexdigest()


def _data_mv(body: str, policy: str = "INCREMENTAL") -> str:
    return _header(DATA, policy, sha=_sha(body, policy)) + body


class RecordingClient:
    """Warehouse double: records DDL, serves canned query rows."""

    def __init__(self, rows=None, fail_when=(), query_error=None):
        self.statements: list[str] = []
        self.queries: list[str] = []
        self._rows = rows if rows is not None else []
        self._fail_when = fail_when
        self._query_error = query_error

    def execute_statement(self, sql):
        self.statements.append(sql)
        if any(marker in sql for marker in self._fail_when):
            raise RuntimeError("boom")
        return True

    def execute_query(self, sql):
        self.queries.append(sql)
        if self._query_error is not None:
            raise self._query_error
        return self._rows


class TestPackageExport:
    def test_is_reexported_from_the_delta_package(self):
        assert ExportedBuilder is MaterializedViewBuilder


class TestDataMvSql:
    def test_slots_and_batch_exact_statement(self):
        sql = MaterializedViewBuilder.data_mv_sql(
            DATA, slots_fqn=SLOTS, batch_branch_sql=BATCH
        )
        assert sql == _data_mv(
            "AS SELECT subject, predicate, object, sha2(object, 256) AS object_hash\n"
            "FROM (\n"
            f"  SELECT subject, predicate, object FROM {SLOTS}\n"
            "  WHERE object IS NOT NULL AND TRIM(object) != ''\n"
            "  UNION ALL\n"
            f"  ({BATCH})\n"
            ") t\n"
            "GROUP BY subject, predicate, object"
        )

    def test_slots_only(self):
        sql = MaterializedViewBuilder.data_mv_sql(DATA, slots_fqn=SLOTS)
        assert sql == _data_mv(
            "AS SELECT subject, predicate, object, sha2(object, 256) AS object_hash\n"
            "FROM (\n"
            f"  SELECT subject, predicate, object FROM {SLOTS}\n"
            "  WHERE object IS NOT NULL AND TRIM(object) != ''\n"
            ") t\n"
            "GROUP BY subject, predicate, object"
        )

    def test_batch_only(self):
        sql = MaterializedViewBuilder.data_mv_sql(DATA, batch_branch_sql=BATCH)
        assert sql == _data_mv(
            "AS SELECT subject, predicate, object, sha2(object, 256) AS object_hash\n"
            "FROM (\n"
            f"  {BATCH}\n"
            ") t\n"
            "GROUP BY subject, predicate, object"
        )

    def test_the_definition_sha_matches_the_statement(self):
        kwargs = dict(slots_fqn=SLOTS, batch_branch_sql=BATCH, refresh_policy="AUTO")
        sql = MaterializedViewBuilder.data_mv_sql(DATA, **kwargs)
        sha = MaterializedViewBuilder.data_mv_sha(**kwargs)
        assert f"'ontobricks.mv_sql_sha256' = '{sha}'" in sql
        assert len(sha) == 64

    def test_the_sha_changes_with_the_branches_and_the_policy(self):
        base = MaterializedViewBuilder.data_mv_sha(slots_fqn=SLOTS, batch_branch_sql=BATCH)
        assert base != MaterializedViewBuilder.data_mv_sha(slots_fqn=SLOTS)
        assert base != MaterializedViewBuilder.data_mv_sha(
            slots_fqn=SLOTS, batch_branch_sql=BATCH + " WHERE 1 = 1"
        )
        assert base != MaterializedViewBuilder.data_mv_sha(
            slots_fqn=SLOTS, batch_branch_sql=BATCH, refresh_policy="AUTO"
        )
        assert base == MaterializedViewBuilder.data_mv_sha(
            slots_fqn=SLOTS, batch_branch_sql=BATCH, refresh_policy=" incremental "
        )

    def test_the_sha_changes_with_the_view_header(self, monkeypatch):
        """A new CLUSTER BY or table property must replace existing views too."""
        before = MaterializedViewBuilder.data_mv_sha(slots_fqn=SLOTS)
        header = _BUILDER_MODULE._MV_HEADER.replace("(predicate, subject)", "(subject)")
        monkeypatch.setattr(_BUILDER_MODULE, "_MV_HEADER", header)
        assert MaterializedViewBuilder.data_mv_sha(slots_fqn=SLOTS) != before

    def test_both_sides_missing_is_a_validation_error(self):
        with pytest.raises(ValidationError):
            MaterializedViewBuilder.data_mv_sql(DATA)

    def test_blank_batch_counts_as_missing(self):
        with pytest.raises(ValidationError):
            MaterializedViewBuilder.data_mv_sql(DATA, batch_branch_sql="   ")

    def test_batch_with_a_cte_is_parenthesised_after_union_all(self):
        sql = MaterializedViewBuilder.data_mv_sql(
            DATA, slots_fqn=SLOTS, batch_branch_sql=BATCH_WITH_CTE
        )
        assert f"UNION ALL\n  ({BATCH_WITH_CTE})\n" in sql
        assert "UNION ALL WITH" not in sql

    def test_policy_is_rendered_and_normalised(self):
        sql = MaterializedViewBuilder.data_mv_sql(
            DATA, slots_fqn=SLOTS, refresh_policy="incremental strict"
        )
        assert "  REFRESH POLICY INCREMENTAL STRICT\n" in sql

    @pytest.mark.parametrize("policy", ["AUTO", "INCREMENTAL", "INCREMENTAL STRICT", "FULL"])
    def test_every_documented_policy_is_accepted(self, policy):
        sql = MaterializedViewBuilder.data_mv_sql(DATA, slots_fqn=SLOTS, refresh_policy=policy)
        assert f"  REFRESH POLICY {policy}\n" in sql

    @pytest.mark.parametrize("policy", ["", "FAST", "INCREMENTAL; DROP TABLE x"])
    def test_unknown_policy_is_rejected(self, policy):
        with pytest.raises(ValidationError):
            MaterializedViewBuilder.data_mv_sql(DATA, slots_fqn=SLOTS, refresh_policy=policy)

    @pytest.mark.parametrize("kwargs", [{"data_fqn": ""}, {"slots_fqn": " "}])
    def test_empty_fqn_is_rejected(self, kwargs):
        args = {"data_fqn": DATA, "slots_fqn": SLOTS}
        args.update(kwargs)
        with pytest.raises(ValidationError):
            MaterializedViewBuilder.data_mv_sql(**args)


class TestDataMvAbsorbsTheSlotShape:
    """Two facts about the slots table that the ``_data`` MV must absorb."""

    SLOT_ROWS = [
        ("s1", "p", "x"),
        ("s1", "p", "x"),  # same attribute slot emitted by a second class on one TriplesMap
        ("s1", "q", ""),  # attribute helper wraps NULL in COALESCE(..., '')
        ("s2", "p", "   "),  # blank object
        ("s2", "p", None),
        ("s3", "p", "y"),
    ]

    def _run(self, rows):
        sql = MaterializedViewBuilder.data_mv_sql(DATA, slots_fqn="slots")
        body = sql.split("\nAS ", 1)[1].replace("sha2(object, 256)", "object")
        con = sqlite3.connect(":memory:")
        con.execute("CREATE TABLE slots (subject TEXT, predicate TEXT, object TEXT)")
        con.executemany("INSERT INTO slots VALUES (?, ?, ?)", rows)
        return sorted(con.execute(body).fetchall())

    def test_group_by_dedups_the_copies_of_a_shared_triples_map(self):
        result = self._run(self.SLOT_ROWS)
        assert result.count(("s1", "p", "x", "x")) == 1

    def test_blank_and_null_objects_are_dropped(self):
        result = self._run(self.SLOT_ROWS)
        assert result == [("s1", "p", "x", "x"), ("s3", "p", "y", "y")]

    def test_group_by_is_outermost_and_the_filter_is_in_the_slots_select(self):
        sql = MaterializedViewBuilder.data_mv_sql(
            DATA, slots_fqn=SLOTS, batch_branch_sql=BATCH
        )
        slots_select = sql.index(f"FROM {SLOTS}")
        blank_filter = sql.index("TRIM(object) != ''")
        union = sql.index("UNION ALL")
        group_by = sql.rindex("GROUP BY subject, predicate, object")
        assert slots_select < blank_filter < union < group_by
        assert sql.endswith("GROUP BY subject, predicate, object")


_TRIGGER = "TRIGGER ON UPDATE"
_EVERY_15_MINUTES = "SCHEDULE CRON '0 0/15 * * * ?' AT TIME ZONE 'UTC'"


class TestMicrobatchRefreshClause:
    """A microbatch ``_data`` MV refreshes between builds (CREATE MATERIALIZED VIEW grammar)."""

    @pytest.mark.parametrize("count", [1, 10])
    def test_up_to_ten_sources_refresh_on_update(self, count):
        assert MaterializedViewBuilder.microbatch_refresh_clause(count) == _TRIGGER

    @pytest.mark.parametrize("count", [11, 40, None], ids=["11", "40", "unknown"])
    def test_more_or_unknown_sources_refresh_every_fifteen_minutes(self, count):
        assert MaterializedViewBuilder.microbatch_refresh_clause(count) == _EVERY_15_MINUTES

    @pytest.mark.parametrize("clause", [_TRIGGER, _EVERY_15_MINUTES])
    def test_the_clause_sits_between_the_properties_and_the_query(self, clause):
        sql = MaterializedViewBuilder.data_mv_sql(DATA, slots_fqn=SLOTS, refresh_clause=clause)

        header, body = sql.split("AS SELECT", 1)
        assert header.rstrip("\n").endswith(f")\n  {clause}")
        assert "GROUP BY subject, predicate, object" in body

    def test_the_clause_is_part_of_the_definition_fingerprint(self):
        plain = MaterializedViewBuilder.data_mv_sha(slots_fqn=SLOTS)
        triggered = MaterializedViewBuilder.data_mv_sha(slots_fqn=SLOTS, refresh_clause=_TRIGGER)
        scheduled = MaterializedViewBuilder.data_mv_sha(
            slots_fqn=SLOTS, refresh_clause=_EVERY_15_MINUTES
        )

        assert len({plain, triggered, scheduled}) == 3
        sql = MaterializedViewBuilder.data_mv_sql(DATA, slots_fqn=SLOTS, refresh_clause=_TRIGGER)
        assert f"'ontobricks.mv_sql_sha256' = '{triggered}'" in sql

    def test_without_a_clause_the_statement_and_fingerprint_are_unchanged(self):
        body = (
            "AS SELECT subject, predicate, object, sha2(object, 256) AS object_hash\n"
            "FROM (\n"
            f"  SELECT subject, predicate, object FROM {SLOTS}\n"
            "  WHERE object IS NOT NULL AND TRIM(object) != ''\n"
            ") t\n"
            "GROUP BY subject, predicate, object"
        )

        assert MaterializedViewBuilder.data_mv_sql(DATA, slots_fqn=SLOTS) == _data_mv(body)

    def test_any_other_clause_is_refused(self):
        with pytest.raises(ValidationError):
            MaterializedViewBuilder.data_mv_sql(
                DATA, slots_fqn=SLOTS, refresh_clause="SCHEDULE EVERY 15 MINUTES"
            )

    def test_apply_creates_the_view_with_the_clause(self):
        client = RecordingClient()

        MaterializedViewBuilder.apply(
            client,
            data_fqn=DATA,
            graph_fqn=GRAPH,
            inferred_fqn=INFERRED,
            slots_fqn=SLOTS,
            batch_branch_sql=None,
            drop_existing_table=False,
            refresh_clause=_TRIGGER,
        )

        create = next(
            s for s in client.statements if s.startswith("CREATE OR REPLACE MATERIALIZED")
        )
        assert f"\n  {_TRIGGER}\n" in create


class TestGraphMvSql:
    def test_exact_statement(self):
        sha = MaterializedViewBuilder.graph_mv_sha(DATA, INFERRED)
        assert MaterializedViewBuilder.graph_mv_sql(GRAPH, DATA, INFERRED) == (
            _header(GRAPH, sha=sha)
            + f"AS SELECT subject, predicate, object FROM {DATA}\n"
            "UNION ALL\n"
            f"SELECT subject, predicate, object FROM {INFERRED}"
        )

    def test_the_fingerprint_follows_the_definition(self):
        base = MaterializedViewBuilder.graph_mv_sha(DATA, INFERRED)
        assert base == MaterializedViewBuilder.graph_mv_sha(DATA, INFERRED)
        assert base != MaterializedViewBuilder.graph_mv_sha(DATA, INFERRED, refresh_clause=_TRIGGER)
        assert base != MaterializedViewBuilder.graph_mv_sha(DATA, f"{INFERRED}_x")
        assert base != MaterializedViewBuilder.data_mv_sha(slots_fqn=SLOTS)

    def test_a_self_refreshing_graph_view_carries_its_trigger(self):
        sql = MaterializedViewBuilder.graph_mv_sql(GRAPH, DATA, INFERRED, refresh_clause=_TRIGGER)
        assert f"\n  {_TRIGGER}\nAS SELECT" in sql

    def test_policy_keyword(self):
        sql = MaterializedViewBuilder.graph_mv_sql(
            GRAPH, DATA, INFERRED, refresh_policy="AUTO"
        )
        assert "  REFRESH POLICY AUTO\n" in sql

    def test_unknown_policy_is_rejected(self):
        with pytest.raises(ValidationError):
            MaterializedViewBuilder.graph_mv_sql(
                GRAPH, DATA, INFERRED, refresh_policy="NOPE"
            )

    @pytest.mark.parametrize("position", [0, 1, 2])
    def test_empty_fqn_is_rejected(self, position):
        args = [GRAPH, DATA, INFERRED]
        args[position] = ""
        with pytest.raises(ValidationError):
            MaterializedViewBuilder.graph_mv_sql(*args)


class TestSmallStatements:
    def test_explain_prefixes_the_statement(self):
        assert MaterializedViewBuilder.explain_sql("CREATE X") == "EXPLAIN CREATE X"

    def test_explain_drops_or_replace_from_the_data_view(self):
        sql = MaterializedViewBuilder.data_mv_sql(DATA, slots_fqn="main.dom.t_slots")

        explain = MaterializedViewBuilder.explain_sql(sql)

        assert explain.startswith(f"EXPLAIN CREATE MATERIALIZED VIEW {DATA}")
        assert explain[len("EXPLAIN ") :] == sql.replace("CREATE OR REPLACE", "CREATE", 1)

    def test_refresh(self):
        assert MaterializedViewBuilder.refresh_sql(DATA) == (
            f"REFRESH MATERIALIZED VIEW {DATA}"
        )

    def test_full_refresh(self):
        assert MaterializedViewBuilder.refresh_sql(DATA, full=True) == (
            f"REFRESH MATERIALIZED VIEW {DATA} FULL"
        )

    def test_refresh_requires_a_name(self):
        with pytest.raises(ValidationError):
            MaterializedViewBuilder.refresh_sql("")

    def test_refresh_technique_query(self):
        assert MaterializedViewBuilder.refresh_technique_sql(DATA) == (
            "SELECT timestamp, message "
            f"FROM event_log(TABLE({DATA})) "
            "WHERE event_type = 'planning_information' "
            "ORDER BY timestamp DESC LIMIT 1"
        )

    def test_refresh_technique_requires_a_name(self):
        with pytest.raises(ValidationError):
            MaterializedViewBuilder.refresh_technique_sql("")


class TestPlanIsIncremental:
    @pytest.mark.parametrize(
        "technique",
        [
            "ROW_BASED",
            "GROUP_AGGREGATE",
            "APPEND_ONLY",
            "PARTITION_OVERWRITE",
            "WINDOW_FUNCTION",
            "GENERIC_AGGREGATE",
            "NO_OP",
        ],
    )
    def test_each_incremental_technique_is_true(self, technique):
        rows = [{"plan": f"... planned to be executed as {technique} ..."}]
        assert MaterializedViewBuilder.plan_is_incremental(rows) is True

    def test_full_recompute_is_false(self):
        rows = [{"plan": "executed as FULL_RECOMPUTE"}]
        assert MaterializedViewBuilder.plan_is_incremental(rows) is False

    def test_full_recompute_wins_over_an_incremental_technique(self):
        rows = [{"plan": "ROW_BASED"}, {"plan": "FULL_RECOMPUTE"}]
        assert MaterializedViewBuilder.plan_is_incremental(rows) is False

    @pytest.mark.parametrize("rows", [None, [], [{}], [{"plan": ""}], [{"plan": "nothing known"}]])
    def test_unknown_is_false(self, rows):
        assert MaterializedViewBuilder.plan_is_incremental(rows) is False

    def test_row_shapes_dict_tuple_list_and_string(self):
        for rows in (
            [{"a": "x", "b": "ROW_BASED"}],
            [("x", "ROW_BASED")],
            [["x", "ROW_BASED"]],
            ["ROW_BASED"],
        ):
            assert MaterializedViewBuilder.plan_is_incremental(rows) is True

    def test_matching_is_case_insensitive(self):
        assert MaterializedViewBuilder.plan_is_incremental([{"p": "row_based"}]) is True

    def test_the_live_eligibility_verdict_is_recognised(self):
        """The real ``EXPLAIN CREATE MATERIALIZED VIEW`` output (Databricks SQL,
        2026-11-02) opens with an eligibility section, not a technique name."""
        rows = [{"plan": "== Incremental Update Eligibility ==\n"
                         "The Materialized View can be incrementally refreshed.\n\n"
                         "== Detailed Incrementalization Info ==\nNo issues detected.\n\n"
                         "== Physical Plan ==\nExecute CreateMaterializedViewCommand ..."}]
        assert MaterializedViewBuilder.plan_is_incremental(rows) is True

    def test_a_not_eligible_verdict_is_not_incremental(self):
        rows = [{"plan": "== Incremental Update Eligibility ==\n"
                         "The Materialized View cannot be incrementally refreshed.\n"
                         "== Detailed Incrementalization Info ==\nstack() is not supported ..."}]
        assert MaterializedViewBuilder.plan_is_incremental(rows) is False
        assert MaterializedViewBuilder.plan_is_incremental([{"p": "full_recompute"}]) is False

    def test_tokens_inside_longer_words_do_not_match(self):
        assert MaterializedViewBuilder.plan_is_incremental([{"p": "NOT_ROW_BASED_AT_ALL"}]) is False

    def test_non_text_values_are_ignored(self):
        assert MaterializedViewBuilder.plan_is_incremental([{"n": 3, "x": None}]) is False


class TestApply:
    """``with_graph_mv=True``: ``_graph`` is replaced by a materialized view too."""

    KWARGS = dict(
        data_fqn=DATA,
        graph_fqn=GRAPH,
        inferred_fqn=INFERRED,
        slots_fqn=SLOTS,
        batch_branch_sql=BATCH,
        with_graph_mv=True,
    )

    def _expected(self, drop_table: bool) -> list[str]:
        statements = []
        if drop_table:
            statements.append(f"DROP TABLE IF EXISTS {DATA}")
        statements += [
            f"DROP VIEW IF EXISTS {DATA}",
            f"DROP VIEW IF EXISTS {GRAPH}",
            MaterializedViewBuilder.data_mv_sql(
                DATA, slots_fqn=SLOTS, batch_branch_sql=BATCH
            ),
            materialize.build_ensure_inferred_sql(INFERRED),
            f"ALTER TABLE {INFERRED} SET TBLPROPERTIES {ROW_TRACKING}",
            MaterializedViewBuilder.graph_mv_sql(GRAPH, DATA, INFERRED),
        ]
        return statements

    def test_statement_order_with_the_table_drop(self):
        client = RecordingClient()
        MaterializedViewBuilder.apply(client, drop_existing_table=True, **self.KWARGS)
        assert client.statements == self._expected(drop_table=True)

    def test_statement_order_without_the_table_drop(self):
        client = RecordingClient()
        MaterializedViewBuilder.apply(client, drop_existing_table=False, **self.KWARGS)
        assert client.statements == self._expected(drop_table=False)

    def test_returns_the_two_relation_names(self):
        result = MaterializedViewBuilder.apply(
            RecordingClient(), drop_existing_table=False, **self.KWARGS
        )
        assert result == {"data_mv": DATA, "graph_mv": GRAPH}

    def test_inferred_companion_is_made_incrementalizable_before_the_graph_mv(self):
        client = RecordingClient()
        MaterializedViewBuilder.apply(client, drop_existing_table=False, **self.KWARGS)
        alter = f"ALTER TABLE {INFERRED} SET TBLPROPERTIES {ROW_TRACKING}"
        graph = next(
            i for i, s in enumerate(client.statements) if GRAPH in s and "MATERIALIZED" in s
        )
        ensure = next(
            i for i, s in enumerate(client.statements) if s.startswith("CREATE TABLE IF NOT EXISTS")
        )
        assert ensure < client.statements.index(alter) < graph

    def test_failing_drops_do_not_abort_the_apply(self):
        client = RecordingClient(fail_when=("DROP TABLE", "DROP VIEW"))
        MaterializedViewBuilder.apply(client, drop_existing_table=True, **self.KWARGS)
        # the three best-effort drops fail; everything after them still runs
        assert client.statements[3:] == self._expected(drop_table=True)[3:]

    def test_a_preexisting_data_view_is_dropped_whatever_the_table_flag(self):
        """A view-mode domain has ``_data`` as a VIEW, which an MV cannot replace."""
        for drop_table in (True, False):
            client = RecordingClient()
            MaterializedViewBuilder.apply(
                client, drop_existing_table=drop_table, **self.KWARGS
            )
            data_mv = next(
                i
                for i, s in enumerate(client.statements)
                if s.startswith(f"CREATE OR REPLACE MATERIALIZED VIEW {DATA}")
            )
            assert f"DROP VIEW IF EXISTS {DATA}" in client.statements[:data_mv]

    def test_the_table_drop_is_still_conditional(self):
        with_drop = RecordingClient()
        without_drop = RecordingClient()
        MaterializedViewBuilder.apply(with_drop, drop_existing_table=True, **self.KWARGS)
        MaterializedViewBuilder.apply(
            without_drop, drop_existing_table=False, **self.KWARGS
        )
        assert f"DROP TABLE IF EXISTS {DATA}" in with_drop.statements
        assert f"DROP TABLE IF EXISTS {DATA}" not in without_drop.statements

    def test_a_failing_mv_statement_propagates(self):
        client = RecordingClient(fail_when=("CREATE OR REPLACE MATERIALIZED VIEW",))
        with pytest.raises(RuntimeError):
            MaterializedViewBuilder.apply(client, drop_existing_table=False, **self.KWARGS)

    def test_invalid_input_raises_before_any_statement_runs(self):
        client = RecordingClient()
        with pytest.raises(ValidationError):
            MaterializedViewBuilder.apply(
                client,
                data_fqn=DATA,
                graph_fqn=GRAPH,
                inferred_fqn=INFERRED,
                slots_fqn=None,
                batch_branch_sql=None,
                drop_existing_table=True,
            )
        assert client.statements == []

    def test_blank_graph_name_raises_before_any_statement_runs(self):
        client = RecordingClient()
        with pytest.raises(ValidationError):
            MaterializedViewBuilder.apply(
                client, **{**self.KWARGS, "graph_fqn": ""}, drop_existing_table=True
            )
        assert client.statements == []

    def test_refresh_policy_is_forwarded_to_both_views(self):
        client = RecordingClient()
        MaterializedViewBuilder.apply(
            client, drop_existing_table=False, refresh_policy="AUTO", **self.KWARGS
        )
        mvs = [s for s in client.statements if "MATERIALIZED VIEW" in s]
        assert len(mvs) == 2 and all("REFRESH POLICY AUTO" in s for s in mvs)


class TestApplyKeepsTheGraphView:
    """Default (``with_graph_mv=False``): ``_graph`` stays the union VIEW.

    The build path relies on it: the inferred reset and the Delta store's app
    writes both ``CREATE OR REPLACE VIEW`` the ``_graph`` relation.
    """

    KWARGS = dict(
        data_fqn=DATA,
        graph_fqn=GRAPH,
        inferred_fqn=INFERRED,
        slots_fqn=SLOTS,
        batch_branch_sql=BATCH,
    )

    def _expected(self, drop_table: bool) -> list[str]:
        statements = [f"DROP TABLE IF EXISTS {DATA}"] if drop_table else []
        return statements + [
            f"DROP VIEW IF EXISTS {DATA}",
            f"DROP MATERIALIZED VIEW IF EXISTS {GRAPH}",
            MaterializedViewBuilder.data_mv_sql(DATA, slots_fqn=SLOTS, batch_branch_sql=BATCH),
            materialize.build_ensure_inferred_sql(INFERRED),
            materialize.build_union_view_sql(GRAPH, DATA, INFERRED),
        ]

    @pytest.mark.parametrize("drop_table", [True, False])
    def test_statement_order(self, drop_table):
        client = RecordingClient()
        MaterializedViewBuilder.apply(client, drop_existing_table=drop_table, **self.KWARGS)
        assert client.statements == self._expected(drop_table)

    def test_the_graph_is_never_a_materialized_view(self):
        client = RecordingClient()
        MaterializedViewBuilder.apply(client, drop_existing_table=True, **self.KWARGS)
        assert not any(
            s.startswith(f"CREATE OR REPLACE MATERIALIZED VIEW {GRAPH}")
            for s in client.statements
        )

    def test_the_graph_view_goes_through_ensure_graph_view(self):
        from unittest.mock import patch

        client = RecordingClient()
        with patch.object(materialize, "ensure_graph_view") as ensure:
            MaterializedViewBuilder.apply(client, drop_existing_table=False, **self.KWARGS)
        ensure.assert_called_once_with(client, GRAPH, DATA, INFERRED)

    def test_returns_the_data_mv_and_the_graph_view(self):
        result = MaterializedViewBuilder.apply(
            RecordingClient(), drop_existing_table=False, **self.KWARGS
        )
        assert result == {"data_mv": DATA, "graph_view": GRAPH}

    def test_a_leftover_graph_mv_drop_failing_does_not_abort(self):
        client = RecordingClient(fail_when=("DROP ",))
        MaterializedViewBuilder.apply(client, drop_existing_table=True, **self.KWARGS)
        assert client.statements[3:] == self._expected(drop_table=True)[3:]

    def test_blank_graph_name_raises_before_any_statement_runs(self):
        client = RecordingClient()
        with pytest.raises(ValidationError):
            MaterializedViewBuilder.apply(
                client, **{**self.KWARGS, "graph_fqn": ""}, drop_existing_table=True
            )
        assert client.statements == []


class TestLastRefreshTechnique:
    def _message(self, technique: str) -> dict:
        return {
            "timestamp": "2026-11-01T10:00:00",
            "message": f"Flow 'x' has been planned to be executed as {technique}. Another 2 "
            "techniques were considered but not selected.",
        }

    def test_parses_the_planned_technique(self):
        client = RecordingClient(rows=[self._message("ROW_BASED")])
        assert MaterializedViewBuilder.last_refresh_technique(client, DATA) == "ROW_BASED"

    def test_parses_full_recompute(self):
        client = RecordingClient(rows=[self._message("FULL_RECOMPUTE")])
        assert (
            MaterializedViewBuilder.last_refresh_technique(client, DATA) == "FULL_RECOMPUTE"
        )

    def test_issues_the_event_log_query(self):
        client = RecordingClient(rows=[self._message("NO_OP")])
        MaterializedViewBuilder.last_refresh_technique(client, DATA)
        assert client.queries == [MaterializedViewBuilder.refresh_technique_sql(DATA)]

    @pytest.mark.parametrize(
        "rows",
        [
            [],
            None,
            [{"message": "no technique mentioned"}],
            [{"message": "executed as lowercase"}],
            [{"message": None}],
            [{}],
            ["not a mapping"],
        ],
    )
    def test_unknown_when_nothing_parses(self, rows):
        client = RecordingClient(rows=rows)
        assert MaterializedViewBuilder.last_refresh_technique(client, DATA) == "UNKNOWN"

    def test_unknown_and_silent_when_the_query_fails(self):
        client = RecordingClient(query_error=RuntimeError("warehouse down"))
        assert MaterializedViewBuilder.last_refresh_technique(client, DATA) == "UNKNOWN"

    def test_unknown_for_an_invalid_name_instead_of_raising(self):
        client = RecordingClient(rows=[self._message("ROW_BASED")])
        assert MaterializedViewBuilder.last_refresh_technique(client, "") == "UNKNOWN"
        assert client.queries == []


class TestExistingMvSha:
    """The stored definition fingerprint lets a build skip an unchanged ``CREATE OR REPLACE``."""

    SHA = "a" * 64

    def test_reads_the_one_property(self):
        client = RecordingClient(rows=[{"key": "ontobricks.mv_sql_sha256", "value": self.SHA}])

        assert MaterializedViewBuilder.existing_mv_sha(client, DATA) == self.SHA
        assert client.queries == [f"SHOW TBLPROPERTIES {DATA} ('ontobricks.mv_sql_sha256')"]

    def test_a_relation_without_the_property_has_no_sha(self):
        client = RecordingClient(
            rows=[
                {
                    "key": "ontobricks.mv_sql_sha256",
                    "value": f"Table {DATA} does not have property: ontobricks.mv_sql_sha256",
                }
            ]
        )

        assert MaterializedViewBuilder.existing_mv_sha(client, DATA) == ""

    def test_any_error_reads_as_no_sha(self):
        client = RecordingClient(query_error=RuntimeError("TABLE_OR_VIEW_NOT_FOUND"))

        assert MaterializedViewBuilder.existing_mv_sha(client, DATA) == ""

    @pytest.mark.parametrize("fqn", ["", "a.b.c; DROP TABLE x"])
    def test_an_invalid_name_reads_as_no_sha_without_a_query(self, fqn):
        client = RecordingClient()

        assert MaterializedViewBuilder.existing_mv_sha(client, fqn) == ""
        assert client.queries == []
