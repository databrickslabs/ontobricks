"""Unit tests for the what-if simulation domain package."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from back.core.errors import ValidationError
from back.objects.digitaltwin.constants import RDF_TYPE
from back.core.graphdb.lakebase import _companion_ddl
from back.core.graphdb.lakebase.SimulationStore import SimulationStore
from back.objects.simulation import (
    Diff,
    Patch,
    Reasoning,
    Sandbox,
    Scope,
    Spec,
)
from back.objects.simulation.overlay import overlay_relation

pytestmark = pytest.mark.unit

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
EX = "http://ex.org/"


def _t(s, p, o):
    return {"subject": EX + s, "predicate": EX + p, "object": o}


# -- sandbox table naming -------------------------------------------------


class TestSimPhy:
    def test_lowercases_and_appends_marker(self):
        assert _companion_ddl.sim_phy("MyDomain_V3", "sAbc") == "mydomain_v3__sim_sabc"

    def test_respects_postgres_identifier_limit(self):
        name = _companion_ddl.sim_phy("x" * 120, "s0123456789")
        assert len(name) <= 63
        assert name.endswith("__sim_s0123456789")

    def test_requires_id(self):
        with pytest.raises(ValueError):
            _companion_ddl.sim_phy("g", "  ")

    def test_is_sim_phy(self):
        assert _companion_ddl.is_sim_phy("g_v1__sim_s1")
        assert _companion_ddl.is_sim_phy('"schema"."g_v1__sim_s1"')
        assert not _companion_ddl.is_sim_phy("g_v1")
        assert not _companion_ddl.is_sim_phy("g_v1__app")
        assert not _companion_ddl.is_sim_phy("g_v1__smu_s1")
        assert not _companion_ddl.is_sim_phy("g_v1__smp_s1")
        assert _companion_ddl.is_sim_overlay_phy("g_v1__smp_s1")
        assert _companion_ddl.sim_overlay_phy("MyDomain_V3", "sAbc") == "mydomain_v3__smp_sabc"
        assert _companion_ddl.overlay_phy_from("g_v1__sim_s1") == "g_v1__smp_s1"
        assert _companion_ddl.overlay_phy_from("g_v1__smu_s1") == "g_v1__smp_s1"
        assert _companion_ddl.before_phy_from("g_v1__sim_s1") == "g_v1__smb_s1"
        assert _companion_ddl.origin_phy_from("g_v1__sim_s1") == "g_v1__smo_s1"
        assert _companion_ddl.is_sim_family_phy("g_v1__smb_s1")
        assert _companion_ddl.is_sim_family_phy("g_v1__smo_s1")
        assert overlay_relation("g_v1__sim_s1") == "g_v1__smp_s1"
        assert overlay_relation("cat.sch.g__smu_s1") == "cat.sch.g__smp_s1"
        assert overlay_relation("live") == ""

    def test_sim_union_phy_marker(self):
        inferred = _companion_ddl.sim_union_phy("MyDomain_V3", "sAbc")
        assert inferred == "mydomain_v3__smu_sabc"

    def test_simulation_union_drop_all_drops_overlay_view(self):
        from back.core.graphdb.lakebase.SimulationUnion import (
            SimulationUnion,
        )

        cur = _Cursor()

        class Store:
            @contextmanager
            def _cursor(self):
                yield cur

        SimulationUnion(Store()).drop_all("G_V1", "s1")
        sqls = [c[0] for c in cur.calls]
        inferred = _companion_ddl.sim_union_phy("G_V1", "s1")
        assert any(inferred in s for s in sqls)
        assert len(sqls) == 1

    def test_sim_union_view_sql_overlays_sandbox_subjects(self):
        cur = _Cursor()
        _companion_ddl.ensure_sim_union_view(
            cur, "g__smu_s1", "g_v1", "g__sim_s1", "g__smp_s1"
        )
        sql = cur.calls[0][0]
        assert "CREATE OR REPLACE VIEW g__smu_s1 AS" in sql
        assert "FROM g_v1 l" in sql
        assert (
            "FROM g__smp_s1 k "
            "WHERE k.subject = l.subject AND k.predicate = l.predicate"
        ) in sql
        assert "UNION ALL" in sql
        assert "FROM g__sim_s1 s" in sql

    def test_sim_union_view_hides_last_removed_predicate_value(self):
        import sqlite3

        db = sqlite3.connect(":memory:")
        db.executescript(
            """
            CREATE TABLE live(subject, predicate, object);
            CREATE TABLE sim(subject, predicate, object);
            CREATE TABLE smp(subject, predicate, object);
            INSERT INTO live VALUES
              ('ex:A','ex:hasContract','ex:X'),
              ('ex:A','ex:name','A');
            INSERT INTO sim VALUES ('ex:A','ex:name','A');
            INSERT INTO smp VALUES
              ('ex:A','ex:hasContract',''),
              ('ex:A','ex:name','');
            CREATE VIEW smu AS
              SELECT l.subject, l.predicate, l.object FROM live l
              WHERE NOT EXISTS (
                SELECT 1 FROM smp k
                WHERE k.subject = l.subject AND k.predicate = l.predicate
              )
              UNION ALL SELECT subject, predicate, object FROM sim;
            """
        )
        assert db.execute(
            "SELECT * FROM sim WHERE predicate='ex:hasContract'"
        ).fetchall() == []
        assert db.execute(
            "SELECT * FROM smu WHERE predicate='ex:hasContract'"
        ).fetchall() == []
        assert db.execute(
            "SELECT object FROM smu WHERE predicate='ex:name'"
        ).fetchall() == [("A",)]

    def test_sandbox_name_matches_physical_resolution(self):
        name = SimulationStore.sandbox_table_name("Dom_V1", "s1")
        assert SimulationStore.sandbox_table_id(name) == name


# -- store write guard ----------------------------------------------------


class _Cursor:
    def __init__(self):
        self.calls = []
        self.rowcount = 3

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchone(self):
        return {"ok": 1}


def _sim_store():
    store = object.__new__(SimulationStore)
    store._schema = "graphs"
    cur = _Cursor()

    @contextmanager
    def _cursor():
        yield cur

    store._cursor = _cursor
    return store, cur


class TestSimulationStoreGuard:
    @pytest.mark.parametrize("live", ["Dom_V1", "dom_v1__app", "dom_v1_sync"])
    def test_rejects_live_tables(self, live):
        store, _ = _sim_store()
        for fn in (store._writable_table_id, store._readable_table_id, store._sql_relation):
            with pytest.raises(ValidationError):
                fn(live)

    def test_accepts_sandbox_tables(self):
        store, _ = _sim_store()
        assert store._writable_table_id("dom_v1__sim_s1") == "dom_v1__sim_s1"
        assert store.synced_table_name("dom_v1__sim_s1") == "dom_v1__sim_s1"
        assert store.adjacency_ready("dom_v1__sim_s1") is False
        assert store.get_inferred_triple_count("dom_v1__sim_s1") == 0

    def test_purge_and_sync_load_are_refused(self):
        store, _ = _sim_store()
        with pytest.raises(NotImplementedError):
            store.purge_materialized_triples("dom_v1__sim_s1")
        with pytest.raises(ValidationError):
            store.bulk_load_into_sync("dom_v1__sim_s1", iter([]))

    def test_copy_reads_live_relation_and_writes_sandbox(self):
        store, cur = _sim_store()
        copied = store.copy_subjects_from("dom_v1__sim_s1", "dom_v1", ["a", "b"])
        assert copied == 3
        sql, params = cur.calls[0]
        assert sql.startswith("INSERT INTO dom_v1__sim_s1 ")
        assert "FROM dom_v1 WHERE subject = ANY(%s)" in sql
        assert params == (["a", "b"],)
        overlay_sql = cur.calls[1][0]
        assert "INSERT INTO dom_v1__smp_s1" in overlay_sql
        assert "FROM dom_v1__sim_s1" in overlay_sql

    @pytest.mark.parametrize("bad", ["dom_v1; DROP TABLE x", "other__sim_s2", ""])
    def test_copy_rejects_unsafe_or_sandbox_sources(self, bad):
        store, _ = _sim_store()
        with pytest.raises(ValidationError):
            store.copy_subjects_from("dom_v1__sim_s1", bad, ["a"])

    def test_create_table_refuses_live_name(self):
        store, cur = _sim_store()
        with pytest.raises(ValidationError):
            store.create_table("Dom_V1")
        assert cur.calls == []

    def test_clone_sandbox_table_inserts_from_source(self):
        store, cur = _sim_store()
        store.clone_sandbox_table("dom_v1__sim_s1", "dom_v1__smb_s1")
        sqls = [c[0] for c in cur.calls]
        assert any("DROP TABLE IF EXISTS dom_v1__smb_s1" in s for s in sqls)
        assert any(
            "INSERT INTO dom_v1__smb_s1 (subject, predicate, object, datatype, lang)" in s
            and "FROM dom_v1__sim_s1" in s
            for s in sqls
        )

    def test_clone_sandbox_table_refuses_same_name(self):
        store, _ = _sim_store()
        with pytest.raises(ValidationError):
            store.clone_sandbox_table("dom_v1__sim_s1", "dom_v1__sim_s1")


class TestBeforePicture:
    def test_persist_before_clones_when_store_supports_it(self):
        from back.objects.simulation.Sandbox import _CompanionMixin

        class Store:
            def __init__(self):
                self.cloned = None

            def clone_sandbox_table(self, src, dst):
                self.cloned = (src, dst)

            def drop_table(self, name):
                raise AssertionError("python recreate should not run")

        class Box(_CompanionMixin):
            def __init__(self):
                self.table = "g__sim_s1"
                self.store = Store()

        box = Box()
        box.persist_before([{"subject": "s", "predicate": "p", "object": "o"}])
        assert box.store.cloned == ("g__sim_s1", "g__smb_s1")

    def test_has_before_picture_uses_table_exists(self):
        from back.objects.simulation.Sandbox import _CompanionMixin

        class Store:
            def table_exists(self, name):
                self.probed = name
                return True

            def query_triples(self, name):
                raise AssertionError("existence must not scan triples")

        class Box(_CompanionMixin):
            def __init__(self):
                self.table = "g__sim_s1"
                self.store = Store()

        box = Box()
        assert box.has_before_picture() is True
        assert box.store.probed == "g__smb_s1"


# -- patches / scope / spec -----------------------------------------------


class TestPatch:
    def test_valid_set(self):
        p = Patch.from_dict({"op": "SET", "subject": "s", "predicate": "p", "object": "7"})
        assert p.op == "set"
        assert p.triple() == {"subject": "s", "predicate": "p", "object": "7"}

    @pytest.mark.parametrize(
        "data",
        [
            {"op": "upsert", "subject": "s", "predicate": "p", "object": "o"},
            {"op": "add", "subject": "", "predicate": "p", "object": "o"},
            {"op": "add", "subject": "s", "predicate": "p", "object": ""},
            "not-a-dict",
        ],
    )
    def test_invalid(self, data):
        with pytest.raises(ValidationError):
            Patch.from_dict(data)

    def test_remove_allows_empty_object(self):
        Patch.from_dict({"op": "remove", "subject": "s", "predicate": "p"})


class TestPatchAppend:
    _S = EX + "person"
    _SCORE = EX + "score"
    _REL = EX + "hasContract"
    _C1 = EX + "C1"
    _C2 = EX + "C2"

    def _set(self, value):
        return {"op": "set", "subject": self._S, "predicate": self._SCORE, "object": value}

    def _add(self, obj):
        return {"op": "add", "subject": self._S, "predicate": self._REL, "object": obj}

    def _remove(self, obj):
        return {"op": "remove", "subject": self._S, "predicate": self._REL, "object": obj}

    def test_appends_new_fact(self):
        assert Patch.append_all([self._set("410")], [self._add(self._C1)]) == [
            self._set("410"),
            self._add(self._C1),
        ]

    def test_skips_exact_duplicate(self):
        existing = [self._set("420")]
        assert Patch.append_all(existing, [self._set("420")]) == existing

    def test_replaces_conflicting_set(self):
        assert Patch.append_all([self._set("410")], [self._set("420")]) == [self._set("420")]

    def test_set_replaces_adds_on_same_predicate(self):
        assert Patch.append_all(
            [self._add(self._C1), self._add(self._C2)],
            [{"op": "set", "subject": self._S, "predicate": self._REL, "object": self._C1}],
        ) == [{"op": "set", "subject": self._S, "predicate": self._REL, "object": self._C1}]

    def test_remove_cancels_matching_add(self):
        assert Patch.append_all([self._add(self._C1)], [self._remove(self._C1)]) == []

    def test_add_cancels_matching_remove(self):
        assert Patch.append_all([self._remove(self._C1)], [self._add(self._C1)]) == [
            self._add(self._C1)
        ]

    def test_remove_of_other_set_value_is_ignored(self):
        existing = [self._set("420")]
        incoming = [
            {
                "op": "remove",
                "subject": self._S,
                "predicate": self._SCORE,
                "object": "410",
            }
        ]
        assert Patch.append_all(existing, incoming) == existing

    def test_dedupes_incoming_batch(self):
        assert Patch.append_all([], [self._add(self._C1), self._add(self._C1)]) == [
            self._add(self._C1)
        ]

    def test_skips_invalid_rows(self):
        assert Patch.append_all(
            [{"op": "upsert", "subject": "s", "predicate": "p", "object": "o"}],
            [self._add(self._C1)],
        ) == [self._add(self._C1)]


class TestSpec:
    def _body(self, **over):
        body = {
            "label": "Score drop",
            "scope": {"type": "entity", "focus": EX + "acme"},
            "patches": [{"op": "set", "subject": EX + "acme", "predicate": EX + "score", "object": "300"}],
        }
        body.update(over)
        return body

    def test_defaults_and_never_materialise(self):
        spec = Spec.from_dict(self._body(), default_hops=2, default_cap=5000)
        assert spec.id.startswith("s")
        assert spec.scope.hops == 2
        assert spec.phases == Spec.DEFAULT_PHASES
        opts = spec.reasoning_options()
        assert opts["materialize"] is False
        assert opts["tbox"] is False and opts["if_then"] is True
        assert "if_then_rule_names" not in opts
        assert spec.rule_names == {}

    def test_cap_is_clamped_to_default(self):
        body = self._body(scope={"type": "entity", "focus": EX + "Customer", "cap": 99999})
        spec = Spec.from_dict(body, default_hops=2, default_cap=5000)
        assert spec.scope.cap == 5000

    def test_body_id_is_ignored(self):
        spec = Spec.from_dict(self._body(id="x; drop"), default_hops=2, default_cap=10)
        assert spec.id != "x; drop"

    @pytest.mark.parametrize(
        "over",
        [
            {"label": ""},
            {"scope": {"type": "planet", "focus": EX + "a"}},
            {"scope": {"type": "entity", "focus": "not a uri"}},
            {"scope": {"type": "entity", "focus": EX + "a", "hops": 9}},
            {"patches": "nope"},
            {"rule_names": "nope"},
        ],
    )
    def test_invalid(self, over):
        with pytest.raises(ValidationError):
            Spec.from_dict(self._body(**over), default_hops=2, default_cap=10)

    def test_round_trip(self):
        spec = Spec.from_dict(self._body(), default_hops=2, default_cap=10)
        again = Spec.from_dict(spec.to_dict(), default_hops=2, default_cap=10, sim_id=spec.id)
        assert again.to_dict() == spec.to_dict()

    def test_fact_prompt_round_trips_and_defaults_empty(self):
        empty = Spec.from_dict(self._body(), default_hops=2, default_cap=10)
        assert empty.fact_prompt == ""
        assert empty.to_dict()["fact_prompt"] == ""
        spec = Spec.from_dict(
            self._body(fact_prompt="  Jacob's score dropped to 420  "),
            default_hops=2,
            default_cap=10,
        )
        assert spec.fact_prompt == "Jacob's score dropped to 420"
        again = Spec.from_dict(spec.to_dict(), default_hops=2, default_cap=10, sim_id=spec.id)
        assert again.fact_prompt == spec.fact_prompt

    def test_fact_prompt_too_long(self):
        with pytest.raises(ValidationError):
            Spec.from_dict(
                self._body(fact_prompt="x" * (Spec.MAX_FACT_PROMPT + 1)),
                default_hops=2,
                default_cap=10,
            )

    def test_selected_rule_names_filter_reasoning_options(self):
        spec = Spec.from_dict(
            self._body(
                phases={"if_then": True, "graph": False},
                rule_names={"if_then": ["AttachMeter", "AttachMeter", ""], "sparql_rules": []},
            ),
            default_hops=2,
            default_cap=10,
        )
        assert spec.rule_names == {"if_then": ["AttachMeter"]}
        opts = spec.reasoning_options()
        assert opts["if_then_rule_names"] == {"AttachMeter"}
        assert "sparql_rule_names" not in opts

    def test_rule_names_ignored_when_phase_off(self):
        spec = Spec.from_dict(
            self._body(phases={"if_then": False}, rule_names={"if_then": ["AttachMeter"]}),
            default_hops=2,
            default_cap=10,
        )
        assert "if_then_rule_names" not in spec.reasoning_options()

    def test_spec_saved_with_legacy_phase_keys_still_loads(self):
        spec = Spec.from_dict(
            self._body(phases={"swrl": False, "graph": True}, rule_names={"swrl": ["AttachMeter"]}),
            default_hops=2,
            default_cap=10,
        )
        assert spec.phases["if_then"] is False
        assert spec.rule_names == {"if_then": ["AttachMeter"]}
        assert "swrl" not in spec.to_dict()["phases"]


class _LiveStore:
    def __init__(self, sub=None):
        self._sub = sub or []
        self.calls = []

    def expand_and_fetch_subgraph(self, table, uris, depth, max_entities, max_triples):
        self.calls.append(("expand", table, tuple(uris), depth, max_entities))
        return {"results": self._sub}


class TestScope:
    def test_focus_label_roundtrips(self):
        scope = Scope.from_dict(
            {
                "type": "entity",
                "focus": EX + "CUST0000037",
                "hops": 2,
                "cap": 10,
                "focus_label": "Acme Industries",
            },
            default_hops=2,
            default_cap=5000,
        )
        assert scope.focus_label == "Acme Industries"
        assert scope.to_dict()["focus_label"] == "Acme Industries"

    def test_entity_neighbourhood_collects_uri_objects_not_types(self):
        store = _LiveStore(sub=[
            {"subject": EX + "a", "predicate": EX + "knows", "object": EX + "b"},
            {"subject": EX + "a", "predicate": RDF_TYPE, "object": EX + "Person"},
            {"subject": EX + "b", "predicate": EX + "age", "object": "42"},
        ])
        scope = Scope(type="entity", focus=EX + "a", hops=1, cap=10)
        assert scope.resolve_entities(store, "g") == [EX + "a", EX + "b"]
        assert store.calls[0][:4] == ("expand", "g", (EX + "a",), 1)

    def test_class_scope_rejected(self):
        with pytest.raises(ValidationError):
            Scope.from_dict(
                {"type": "class", "focus": EX + "C", "cap": 3},
                default_hops=2,
                default_cap=5000,
            )

    def test_cohort_members(self):
        svc = SimpleNamespace(dry_run=lambda rule, store, g: {
            "cohorts": [{"members": [{"uri": EX + "m1"}, {"uri": EX + "m2"}]}, {"members": [{"uri": EX + "m1"}]}]
        })
        scope = Scope(type="cohort", focus="r1", cap=10)
        got = scope.resolve_entities(_LiveStore(), "g", cohort_service=svc, cohort_rule={"id": "r1"})
        assert got == [EX + "m1", EX + "m2"]

    def test_cohort_requires_rule(self):
        scope = Scope(type="cohort", focus="r1", cap=10)
        with pytest.raises(ValidationError):
            scope.resolve_entities(_LiveStore(), "g")

    def test_empty_scope_rejected(self):
        svc = SimpleNamespace(dry_run=lambda rule, store, g: {"cohorts": []})
        scope = Scope(type="cohort", focus="r1", cap=3)
        with pytest.raises(ValidationError):
            scope.resolve_entities(
                _LiveStore(), "g", cohort_service=svc, cohort_rule={"id": "r1"}
            )


# -- diff ---------------------------------------------------------------


class TestDiff:
    def test_tags_patch_and_inferred_additions(self):
        before = [_t("a", "score", "700"), _t("a", "name", "Acme")]
        after = [_t("a", "score", "300"), _t("a", "name", "Acme"), _t("a", "risk", "high")]
        patch_key = Diff.key(_t("a", "score", "300"))
        risk_key = Diff.key(_t("a", "risk", "high"))
        diff = Diff.compute(
            before, after, patch_added={patch_key}, provenance={risk_key: "if_then:Risk"}
        )
        s = diff["summary"]
        assert (s["added_count"], s["added_patch_count"], s["added_inferred_count"]) == (2, 1, 1)
        assert s["removed_count"] == 1
        assert s["touched_entity_count"] == 1
        inferred = [r for r in diff["added"] if r["origin"] == "inferred"]
        assert inferred[0]["provenance"] == "if_then:Risk"
        assert diff["removed"][0]["object"] == "700"
        assert diff["removed"][0]["origin"] == "patch"

    def test_row_cap(self):
        after = [_t(str(i), "p", "o") for i in range(10)]
        diff = Diff.compute([], after, patch_added=set(), row_cap=4)
        assert len(diff["added"]) == 4
        assert diff["summary"]["added_count"] == 10
        assert diff["summary"]["capped"] is True

    def test_unlimited_row_cap(self):
        after = [_t(str(i), "p", "o") for i in range(10)]
        diff = Diff.compute([], after, patch_added=set(), row_cap=None)
        assert len(diff["added"]) == 10
        assert diff["summary"]["capped"] is False
        assert diff["summary"]["row_cap"] is None

    def test_page_and_search(self):
        rows = [{"subject": f"s{i}", "predicate": "p", "object": "o"} for i in range(7)]
        page = Diff.page({"added": rows}, "added", page=2, page_size=3)
        assert [r["subject"] for r in page["rows"]] == ["s3", "s4", "s5"]
        assert page["total"] == 7
        found = Diff.page({"added": rows}, "added", search="S6")
        assert found["total"] == 1
        assert Diff.page({"added": rows}, "bogus")["kind"] == "added"


# -- sandbox patch semantics -----------------------------------------------


class _SandboxStore:
    def __init__(self, triples):
        self.rows = {Diff.key(t) for t in triples}

    def insert_triples(self, table, triples):
        for t in triples:
            self.rows.add(Diff.key(t))
        return len(triples)

    def delete_triples(self, table, triples):
        for t in triples:
            self.rows.discard(Diff.key(t))
        return len(triples)

    def delete_subject_predicate(self, table, pairs):
        for pair in pairs:
            self.rows = {r for r in self.rows if (r[0], r[1]) != (pair["subject"], pair["predicate"])}
        return 1

    def query_triples(self, table):
        return [
            {"subject": subject, "predicate": predicate, "object": obj}
            for subject, predicate, obj in self.rows
        ]


class TestSandboxApply:
    def test_set_add_remove(self):
        sandbox = object.__new__(Sandbox)
        sandbox.table = "g__sim_s1"
        sandbox.store = _SandboxStore([_t("a", "score", "700"), _t("a", "score", "650"), _t("a", "tag", "x")])
        asserted = sandbox.apply([
            Patch("set", EX + "a", EX + "score", "300"),
            Patch("add", EX + "a", EX + "tag", "y"),
            Patch("remove", EX + "a", EX + "tag", "x"),
        ])
        assert sandbox.store.rows == {
            Diff.key(_t("a", "score", "300")),
            Diff.key(_t("a", "tag", "y")),
        }
        assert asserted == sandbox.store.rows

    def test_sandbox_requires_lakebase_store(self):
        with pytest.raises(ValidationError):
            Sandbox(object(), "g", "s1")

    def test_seed_neighbourhood_copies_related_subjects(self):
        sandbox = object.__new__(Sandbox)
        seeded: list = []
        sandbox.seed = lambda uris: seeded.extend(uris) or len(uris)
        sandbox._graph_name = "g"

        class Live:
            def expand_and_fetch_subgraph(self, table, uris, hops, *a):
                assert hops == 1
                assert uris == [EX + "c2"]
                return {
                    "results": [
                        {
                            "subject": EX + "c2",
                            "predicate": EX + "hasMeter",
                            "object": EX + "m1",
                        },
                        {
                            "subject": EX + "m1",
                            "predicate": RDF_TYPE,
                            "object": EX + "Meter",
                        },
                    ]
                }

        sandbox._live = Live()
        sandbox.seed_neighbourhood([EX + "c2"], hops=1)
        assert EX + "c2" in seeded
        assert EX + "m1" in seeded
        assert EX + "Meter" not in seeded

    def test_seed_neighbourhood_skips_inbound_when_disabled(self):
        sandbox = object.__new__(Sandbox)
        seeded: list = []
        sandbox.seed = lambda uris: seeded.extend(uris) or len(uris)
        sandbox._graph_name = "g"
        sandbox.uri_objects_of_many = lambda subjects: (
            [EX + "m1"] if EX + "c2" in subjects else []
        )

        class Live:
            def expand_and_fetch_subgraph(self, *a):
                raise AssertionError("inbound expand must not run")

        sandbox._live = Live()
        sandbox.seed_neighbourhood([EX + "c2"], hops=1, inbound=False)
        assert EX + "c2" in seeded
        assert EX + "m1" in seeded
        assert EX + "otherCustomer" not in seeded

    def test_seed_referrers_copies_subjects_pointing_at_focus(self):
        sandbox = object.__new__(Sandbox)
        seeded: list = []
        sandbox.seed = lambda uris: seeded.extend(uris) or len(uris)
        sandbox._graph_name = "g"

        class Live:
            def expand_and_fetch_subgraph(self, table, uris, hops, *a):
                assert uris == [EX + "jacob"]
                assert hops == 1
                return {
                    "results": [
                        {
                            "subject": EX + "CLM0000030",
                            "predicate": EX + "hasCustomer",
                            "object": EX + "jacob",
                        },
                        {
                            "subject": EX + "otherCust",
                            "predicate": EX + "hasContract",
                            "object": EX + "c2",
                        },
                    ]
                }

        sandbox._live = Live()
        sandbox.seed_referrers([EX + "jacob"])
        assert EX + "CLM0000030" in seeded
        assert EX + "otherCust" not in seeded
        assert EX + "jacob" not in seeded

    def test_seed_neighbourhood_walks_sandbox_objects_when_expand_empty(self):
        sandbox = object.__new__(Sandbox)
        seeded: list = []
        sandbox.seed = lambda uris: seeded.extend(uris) or len(uris)
        sandbox._graph_name = "g"
        sandbox.uri_objects_of_many = lambda subjects: (
            [EX + "m1"] if EX + "c2" in subjects else []
        )

        class Live:
            def expand_and_fetch_subgraph(self, *a):
                return {"results": []}

        sandbox._live = Live()
        sandbox.seed_neighbourhood([EX + "c2"], hops=1)
        assert EX + "c2" in seeded
        assert EX + "m1" in seeded

    def test_seed_focus_for_if_then_copies_sandbox_objects(self):
        sandbox = object.__new__(Sandbox)
        calls = []
        sandbox.seed_neighbourhood = (
            lambda uris, hops=1: calls.append((list(uris), hops)) or len(uris)
        )
        sandbox.uri_objects_of = lambda subject: [EX + "c2", subject]
        sandbox.seed_focus_for_if_then(EX + "cust")
        assert calls[0] == ([EX + "cust"], 2)
        assert calls[1] == ([EX + "c2"], 1)

    def test_uri_objects_of_skips_rdf_type(self):
        sandbox = object.__new__(Sandbox)
        sandbox.table = "g__sim_s1"

        class Store:
            def sql_table_reference(self, table):
                return table

            def execute_query(self, sql):
                assert "g__sim_s1" in sql
                assert "IN (" in sql or "subject =" in sql
                return [
                    {"predicate": RDF_TYPE, "object": EX + "Customer"},
                    {"predicate": EX + "hasContract", "object": EX + "c2"},
                    {"predicate": EX + "name", "object": "Jacob"},
                ]

        sandbox.store = Store()
        assert sandbox.uri_objects_of(EX + "cust") == [EX + "c2"]


class TestSandboxLifecycle:
    def test_reset_drops_explorer_views_before_sandbox(self, monkeypatch):
        events = []

        class Union:
            def __init__(self, live):
                pass

            def drop_all(self, graph_name, sim_id):
                events.append(("views", graph_name, sim_id))

        class Store:
            def drop_table(self, table):
                events.append(("table", table))

            def create_table(self, table):
                events.append(("create", table))

        import back.core.graphdb.lakebase as lakebase
        from back.core.graphdb import GraphDBFactory

        monkeypatch.setattr(lakebase, "SimulationUnion", Union)
        sandbox = object.__new__(Sandbox)
        sandbox._sim = GraphDBFactory.get_simulation("lakebase")
        sandbox._live = object()
        sandbox._graph_name = "g_v1"
        sandbox._sim_id = "s1"
        sandbox.table = "g_v1__sim_s1"
        sandbox.store = Store()

        sandbox.reset()

        assert events == [
            ("views", "g_v1", "s1"),
            ("table", "g_v1__sim_s1"),
            ("create", "g_v1__sim_s1"),
        ]


def test_reasoning_service_targets_sandbox():
    svc = Reasoning(SimpleNamespace(info={"name": "Dom"}, current_version="1"), None, "dom_v1__sim_s1")
    assert svc._get_graph_name() == "dom_v1__sim_s1"


def test_domain_snapshot_loads_if_then_from_ontology():
    from back.objects.digitaltwin.models import DomainSnapshot

    domain = SimpleNamespace(
        info={},
        databricks={},
        ontology={
            "if_then_rules": [{"name": "AttachMeter"}],
            "classes": [],
            "properties": [],
        },
        generated={},
    )
    snap = DomainSnapshot(domain)
    assert snap.if_then_rules[0]["name"] == "AttachMeter"
