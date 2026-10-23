"""Unit tests for the what-if simulation domain package."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from back.core.errors import ValidationError
from back.core.graphdb.lakebase import _companion_ddl
from back.core.graphdb.lakebase.LakebaseSimulationStore import LakebaseSimulationStore
from back.objects.simulation import (
    SimulationDiff,
    SimulationPatch,
    SimulationReasoningService,
    SimulationScope,
    SimulationSpec,
)
from back.objects.simulation.SimulationSandbox import SimulationSandbox

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

    def test_sandbox_name_matches_physical_resolution(self):
        name = LakebaseSimulationStore.sandbox_table_name("Dom_V1", "s1")
        assert LakebaseSimulationStore.sandbox_table_id(name) == name


# -- store write guard ----------------------------------------------------


class _Cursor:
    def __init__(self):
        self.calls = []
        self.rowcount = 3

    def execute(self, sql, params=None):
        self.calls.append((sql, params))


def _sim_store():
    store = object.__new__(LakebaseSimulationStore)
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


# -- patches / scope / spec -----------------------------------------------


class TestSimulationPatch:
    def test_valid_set(self):
        p = SimulationPatch.from_dict({"op": "SET", "subject": "s", "predicate": "p", "object": "7"})
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
            SimulationPatch.from_dict(data)

    def test_remove_allows_empty_object(self):
        SimulationPatch.from_dict({"op": "remove", "subject": "s", "predicate": "p"})


class TestSimulationSpec:
    def _body(self, **over):
        body = {
            "label": "Score drop",
            "scope": {"type": "entity", "focus": EX + "acme"},
            "patches": [{"op": "set", "subject": EX + "acme", "predicate": EX + "score", "object": "300"}],
        }
        body.update(over)
        return body

    def test_defaults_and_never_materialise(self):
        spec = SimulationSpec.from_dict(self._body(), default_hops=2, default_cap=5000)
        assert spec.id.startswith("s")
        assert spec.scope.hops == 2
        assert spec.phases == SimulationSpec.DEFAULT_PHASES
        opts = spec.reasoning_options()
        assert opts["materialize"] is False
        assert opts["tbox"] is False and opts["swrl"] is True

    def test_cap_is_clamped_to_default(self):
        body = self._body(scope={"type": "class", "focus": EX + "Customer", "cap": 99999})
        spec = SimulationSpec.from_dict(body, default_hops=2, default_cap=5000)
        assert spec.scope.cap == 5000

    def test_body_id_is_ignored(self):
        spec = SimulationSpec.from_dict(self._body(id="x; drop"), default_hops=2, default_cap=10)
        assert spec.id != "x; drop"

    @pytest.mark.parametrize(
        "over",
        [
            {"label": ""},
            {"scope": {"type": "planet", "focus": EX + "a"}},
            {"scope": {"type": "entity", "focus": "not a uri"}},
            {"scope": {"type": "entity", "focus": EX + "a", "hops": 9}},
            {"patches": "nope"},
        ],
    )
    def test_invalid(self, over):
        with pytest.raises(ValidationError):
            SimulationSpec.from_dict(self._body(**over), default_hops=2, default_cap=10)

    def test_round_trip(self):
        spec = SimulationSpec.from_dict(self._body(), default_hops=2, default_cap=10)
        again = SimulationSpec.from_dict(spec.to_dict(), default_hops=2, default_cap=10, sim_id=spec.id)
        assert again.to_dict() == spec.to_dict()


class _LiveStore:
    def __init__(self, sub=None, typed=None):
        self._sub = sub or []
        self._typed = typed or []
        self.calls = []

    def expand_and_fetch_subgraph(self, table, uris, depth, max_entities, max_triples):
        self.calls.append(("expand", table, tuple(uris), depth, max_entities))
        return {"results": self._sub}

    def find_subjects_by_type(self, table, type_uri, limit=50, offset=0, search=None):
        self.calls.append(("typed", table, type_uri, limit))
        return self._typed[:limit]


class TestSimulationScope:
    def test_entity_neighbourhood_collects_uri_objects_not_types(self):
        store = _LiveStore(sub=[
            {"subject": EX + "a", "predicate": EX + "knows", "object": EX + "b"},
            {"subject": EX + "a", "predicate": RDF_TYPE, "object": EX + "Person"},
            {"subject": EX + "b", "predicate": EX + "age", "object": "42"},
        ])
        scope = SimulationScope(type="entity", focus=EX + "a", hops=1, cap=10)
        assert scope.resolve_entities(store, "g") == [EX + "a", EX + "b"]
        assert store.calls[0][:4] == ("expand", "g", (EX + "a",), 1)

    def test_class_population_cap_exceeded(self):
        store = _LiveStore(typed=[EX + str(i) for i in range(5)])
        scope = SimulationScope(type="class", focus=EX + "C", cap=3)
        with pytest.raises(ValidationError):
            scope.resolve_entities(store, "g")
        assert store.calls[0][3] == 4

    def test_class_population_within_cap(self):
        store = _LiveStore(typed=[EX + "1", EX + "2"])
        scope = SimulationScope(type="class", focus=EX + "C", cap=3)
        assert scope.resolve_entities(store, "g") == [EX + "1", EX + "2"]

    def test_cohort_members(self):
        svc = SimpleNamespace(dry_run=lambda rule, store, g: {
            "cohorts": [{"members": [{"uri": EX + "m1"}, {"uri": EX + "m2"}]}, {"members": [{"uri": EX + "m1"}]}]
        })
        scope = SimulationScope(type="cohort", focus="r1", cap=10)
        got = scope.resolve_entities(_LiveStore(), "g", cohort_service=svc, cohort_rule={"id": "r1"})
        assert got == [EX + "m1", EX + "m2"]

    def test_cohort_requires_rule(self):
        scope = SimulationScope(type="cohort", focus="r1", cap=10)
        with pytest.raises(ValidationError):
            scope.resolve_entities(_LiveStore(), "g")

    def test_empty_scope_rejected(self):
        scope = SimulationScope(type="class", focus=EX + "C", cap=3)
        with pytest.raises(ValidationError):
            scope.resolve_entities(_LiveStore(), "g")


# -- diff ---------------------------------------------------------------


class TestSimulationDiff:
    def test_tags_patch_and_inferred_additions(self):
        before = [_t("a", "score", "700"), _t("a", "name", "Acme")]
        after = [_t("a", "score", "300"), _t("a", "name", "Acme"), _t("a", "risk", "high")]
        patch_key = SimulationDiff.key(_t("a", "score", "300"))
        risk_key = SimulationDiff.key(_t("a", "risk", "high"))
        diff = SimulationDiff.compute(
            before, after, patch_added={patch_key}, provenance={risk_key: "swrl:Risk"}
        )
        s = diff["summary"]
        assert (s["added_count"], s["added_patch_count"], s["added_inferred_count"]) == (2, 1, 1)
        assert s["removed_count"] == 1
        assert s["touched_entity_count"] == 1
        inferred = [r for r in diff["added"] if r["origin"] == "inferred"]
        assert inferred[0]["provenance"] == "swrl:Risk"
        assert diff["removed"][0]["object"] == "700"

    def test_row_cap(self):
        after = [_t(str(i), "p", "o") for i in range(10)]
        diff = SimulationDiff.compute([], after, patch_added=set(), row_cap=4)
        assert len(diff["added"]) == 4
        assert diff["summary"]["added_count"] == 10
        assert diff["summary"]["capped"] is True

    def test_page_and_search(self):
        rows = [{"subject": f"s{i}", "predicate": "p", "object": "o"} for i in range(7)]
        page = SimulationDiff.page({"added": rows}, "added", page=2, page_size=3)
        assert [r["subject"] for r in page["rows"]] == ["s3", "s4", "s5"]
        assert page["total"] == 7
        found = SimulationDiff.page({"added": rows}, "added", search="S6")
        assert found["total"] == 1
        assert SimulationDiff.page({"added": rows}, "bogus")["kind"] == "added"


# -- sandbox patch semantics -----------------------------------------------


class _SandboxStore:
    def __init__(self, triples):
        self.rows = {SimulationDiff.key(t) for t in triples}

    def insert_triples(self, table, triples):
        for t in triples:
            self.rows.add(SimulationDiff.key(t))
        return len(triples)

    def delete_triples(self, table, triples):
        for t in triples:
            self.rows.discard(SimulationDiff.key(t))
        return len(triples)

    def delete_subject_predicate(self, table, pairs):
        for pair in pairs:
            self.rows = {r for r in self.rows if (r[0], r[1]) != (pair["subject"], pair["predicate"])}
        return 1


class TestSandboxApply:
    def test_set_add_remove(self):
        sandbox = object.__new__(SimulationSandbox)
        sandbox.table = "g__sim_s1"
        sandbox.store = _SandboxStore([_t("a", "score", "700"), _t("a", "score", "650"), _t("a", "tag", "x")])
        asserted = sandbox.apply([
            SimulationPatch("set", EX + "a", EX + "score", "300"),
            SimulationPatch("add", EX + "a", EX + "tag", "y"),
            SimulationPatch("remove", EX + "a", EX + "tag", "x"),
        ])
        assert sandbox.store.rows == {
            SimulationDiff.key(_t("a", "score", "300")),
            SimulationDiff.key(_t("a", "tag", "y")),
        }
        assert asserted == sandbox.store.rows

    def test_sandbox_requires_lakebase_store(self):
        with pytest.raises(ValidationError):
            SimulationSandbox(object(), "g", "s1")


def test_reasoning_service_targets_sandbox():
    svc = SimulationReasoningService(SimpleNamespace(info={"name": "Dom"}, current_version="1"), None, "dom_v1__sim_s1")
    assert svc._get_graph_name() == "dom_v1__sim_s1"
