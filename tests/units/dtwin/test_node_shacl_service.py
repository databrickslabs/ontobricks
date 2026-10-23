"""Entity-scoped SHACL facet check (``.planning/entity-shacl-facets/SPEC.md``)."""

from __future__ import annotations

import json
from importlib import import_module
from types import SimpleNamespace

import pytest

from back.core.errors import InfrastructureError
from back.core.graphdb.constants import RDF_TYPE
from back.core.w3c.shacl.constants import RDFS_LABEL

pytestmark = pytest.mark.unit

BASE = "https://example.com/onto#"
PARTY = BASE + "Party"
CUSTOMER = BASE + "Customer"
ORDER = BASE + "Order"
ENTITY = "https://example.com/Customer/C-42"
TABLE = "main.kg.triplestore_c360_V1"

CLASSES = [
    {"name": "Party", "uri": PARTY},
    {"name": "Customer", "uri": CUSTOMER, "parent": "Party"},
    {"name": "Order", "uri": ORDER},
]


def _shape(shape_id, **kw):
    shape = {
        "id": shape_id,
        "category": "completeness",
        "label": shape_id,
        "target_class": "Customer",
        "target_class_uri": CUSTOMER,
        "property_path": "",
        "property_uri": "",
        "shacl_type": "sh:minCount",
        "parameters": {},
        "severity": "sh:Violation",
        "message": "",
        "enabled": True,
        "conditions": [],
        "condition_logic": "and",
    }
    shape.update(kw)
    return shape


# Property URIs use the ontology's ``#`` form; the triple store holds ``/``.
S_EMAIL = _shape(
    "s_email", label="Customer.email must not be empty",
    property_path="email", property_uri=BASE + "email",
    parameters={"sh:minCount": 1},
)
S_NAME = _shape(
    "s_name", label="Party.name must not be empty",
    target_class="Party", target_class_uri=PARTY,
    property_path="name", property_uri=BASE + "name",
    parameters={"sh:minCount": 1},
)
S_PHONE = _shape(
    "s_phone", category="conformance", label="Phone must start with +",
    severity="sh:Warning", property_path="phone", property_uri=BASE + "phone",
    shacl_type="sh:pattern", parameters={"sh:pattern": "^\\+"},
)
S_LABEL = _shape(
    "s_label", category="structural", label="Every entity has a label",
    target_class="", target_class_uri="",
    property_path="label", property_uri=RDFS_LABEL, shacl_type="sh:sparql",
    parameters={"sh:select": (
        "SELECT $this WHERE { $this a ?t . FILTER NOT EXISTS "
        "{ $this <http://www.w3.org/2000/01/rdf-schema#label> ?l } }"
    )},
)
S_GUARD = _shape(
    "s_guard", category="conformance", label="Active customers are verified",
    property_path="verified", property_uri=BASE + "verified",
    shacl_type="sh:hasValue", parameters={"sh:hasValue": "true"},
    conditions=[{
        "property": "status", "property_uri": BASE + "status",
        "op": "eq", "value": "active",
    }],
)
S_CLOSED = _shape(
    "s_closed", category="structural", label="Closed shape",
    shacl_type="sh:closed", parameters={"sh:closed": True},
)
S_ORDER = _shape(
    "s_order", target_class="Order", target_class_uri=ORDER,
    property_path="amount", property_uri=BASE + "amount",
    parameters={"sh:minCount": 1},
)
S_DISABLED = _shape(
    "s_disabled", enabled=False, property_path="email",
    property_uri=BASE + "email", parameters={"sh:minCount": 1},
)

ALL_SHAPES = [S_EMAIL, S_NAME, S_PHONE, S_LABEL, S_GUARD, S_CLOSED, S_ORDER, S_DISABLED]
APPLICABLE_IDS = {"s_email", "s_name", "s_phone", "s_label", "s_guard", "s_closed"}

ENTITY_TRIPLES = [
    {"predicate": RDF_TYPE, "object": CUSTOMER},
    {"predicate": RDFS_LABEL, "object": "ACME Corp"},
    {"predicate": "https://example.com/onto/name", "object": "ACME Corp"},
    {"predicate": "https://example.com/onto/phone", "object": "06 12 34 56 78"},
    {"predicate": "https://example.com/onto/status", "object": "closed"},
]


def _violation(shape_id, **detail):
    return {"shape_id": shape_id, "kind": "v", "detail": json.dumps({"s": ENTITY, **detail})}


def _guard_match(shape_id):
    return {"shape_id": shape_id, "kind": "g", "detail": json.dumps({"s": ENTITY})}


class FakeStore:
    """Answers the two queries the service issues, keyed on what they select.

    A facet query names the shapes it covers as literals, so the fake returns
    the configured rows of every shape the SQL mentions — the batch and a
    per-shape retry get consistent answers without parsing SQL.
    """

    def __init__(self, triples=None, rows=None, failing=()):
        self.triples = ENTITY_TRIPLES if triples is None else triples
        self.rows = rows or []
        self.failing = set(failing)
        self.queries: list[str] = []

    def execute_query(self, sql):
        self.queries.append(sql)
        if "shape_id" not in sql:
            return [dict(t) for t in self.triples]
        if any(f"'{sid}'" in sql for sid in self.failing):
            raise RuntimeError("[INVALID_PARAMETER_VALUE.PATTERN] bad regex")
        return [r for r in self.rows if f"'{r['shape_id']}'" in sql]

    @property
    def facet_queries(self):
        return [q for q in self.queries if "shape_id" in q]


@pytest.fixture
def svc_module():
    try:
        return import_module("back.objects.digitaltwin.node.NodeShaclService")
    except ImportError as exc:
        pytest.fail(f"NodeShaclService is not implemented yet: {exc}")


@pytest.fixture
def svc(svc_module):
    return svc_module.NodeShaclService


@pytest.fixture
def domain():
    return SimpleNamespace(
        shacl_shapes=[dict(s) for s in ALL_SHAPES],
        get_classes=lambda: CLASSES,
        info={"name": "Customer 360"},
    )


@pytest.fixture
def install_store(monkeypatch, svc_module, svc):
    """Route the service to *store* and a fixed triple-store table."""

    def _install(store):
        monkeypatch.setattr(svc_module, "get_graphdb", lambda *_a, **_kw: store)
        monkeypatch.setattr(svc, "triplestore_table", staticmethod(lambda _d, _s: TABLE))
        return store

    return _install


def _by_id(payload):
    return {f["shape_id"]: f for f in payload["facets"]}


# ---------------------------------------------------------------------------
# Shape selection (SPEC §1.1)
# ---------------------------------------------------------------------------


class TestApplicableShapes:
    def test_own_inherited_and_global_shapes_are_selected(self, svc):
        selected = svc.applicable_shapes(ALL_SHAPES, CLASSES, ["Customer"])

        assert {s["id"] for s in selected} == APPLICABLE_IDS

    def test_inherited_shape_names_its_ancestor(self, svc):
        selected = {s["id"]: s for s in svc.applicable_shapes(ALL_SHAPES, CLASSES, ["Customer"])}

        assert selected["s_name"]["inherited_from"] == "Party"
        assert selected["s_email"]["inherited_from"] is None
        assert selected["s_label"]["inherited_from"] is None

    def test_unrelated_and_disabled_shapes_are_left_out(self, svc):
        ids = {s["id"] for s in svc.applicable_shapes(ALL_SHAPES, CLASSES, ["Customer"])}

        assert "s_order" not in ids
        assert "s_disabled" not in ids

    def test_ancestor_does_not_inherit_its_subclass_shapes(self, svc):
        ids = {s["id"] for s in svc.applicable_shapes(ALL_SHAPES, CLASSES, ["Party"])}

        assert ids == {"s_name", "s_label"}

    def test_selection_does_not_mutate_the_domain_shapes(self, svc):
        shapes = [dict(s) for s in ALL_SHAPES]
        svc.applicable_shapes(shapes, CLASSES, ["Customer"])

        assert all("inherited_from" not in s for s in shapes)

    def test_count_matches_the_selection(self, svc, domain):
        assert svc.count_applicable(domain, "Customer") == len(APPLICABLE_IDS)
        assert svc.count_applicable(domain, "Order") == 2  # own + global


class TestEvaluationShape:
    def test_inherited_shape_is_evaluated_against_the_entity_class(self, svc):
        """The store only carries the asserted ``rdf:type``: evaluating against
        the ancestor URI would match no subject and silently pass."""
        evaluated = svc.evaluation_shape(S_NAME, CUSTOMER)

        assert evaluated["target_class_uri"] == CUSTOMER
        assert S_NAME["target_class_uri"] == PARTY

    def test_global_shape_keeps_its_empty_target(self, svc):
        assert svc.evaluation_shape(S_LABEL, CUSTOMER)["target_class_uri"] == ""


# ---------------------------------------------------------------------------
# Batch SQL (SPEC §3.3 steps 5–7)
# ---------------------------------------------------------------------------


class TestBatchSql:
    def test_every_translatable_shape_is_one_branch_filtered_on_the_entity(self, svc):
        sql = svc.batch_sql([S_EMAIL, S_PHONE], TABLE, ENTITY)

        assert "'s_email'" in sql and "'s_phone'" in sql
        assert "UNION ALL" in sql
        assert sql.count(f"'{ENTITY}'") >= 2

    def test_untranslatable_shape_is_not_queried(self, svc):
        sql = svc.batch_sql([S_EMAIL, S_CLOSED], TABLE, ENTITY)

        assert "'s_closed'" not in sql

    def test_nothing_translatable_yields_no_query(self, svc):
        assert svc.batch_sql([S_CLOSED], TABLE, ENTITY) is None

    def test_guarded_shape_adds_an_applicability_branch(self, svc):
        sql = svc.batch_sql([S_GUARD], TABLE, ENTITY)

        assert "'g'" in sql
        assert "'v'" in sql

    def test_entity_uri_is_escaped(self, svc):
        sql = svc.batch_sql([S_EMAIL], TABLE, "https://example.com/Customer/O'Brien")

        assert "O''Brien" in sql
        assert "O'Brien'" not in sql.replace("O''Brien'", "")

    def test_unsafe_shape_id_is_never_inlined(self, svc):
        hostile = _shape(
            "x'; DROP TABLE t; --", property_path="email",
            property_uri=BASE + "email", parameters={"sh:minCount": 1},
        )
        sql = svc.batch_sql([hostile], TABLE, ENTITY) or ""

        assert "DROP TABLE" not in sql


# ---------------------------------------------------------------------------
# Evaluation (SPEC §3.2 / §3.3 / §4)
# ---------------------------------------------------------------------------


class TestEvaluate:
    async def test_statuses_cover_the_five_outcomes(self, svc, domain, install_store):
        install_store(FakeStore(rows=[
            _violation("s_email", count=0),
            _violation("s_phone", val="06 12 34 56 78"),
        ]))

        payload = await svc.evaluate(domain, None, entity_uri=ENTITY)
        facets = _by_id(payload)

        assert payload["success"] is True
        assert set(facets) == APPLICABLE_IDS
        assert facets["s_email"]["status"] == "fail"
        assert facets["s_phone"]["status"] == "fail"
        assert facets["s_name"]["status"] == "pass"
        assert facets["s_label"]["status"] == "pass"
        assert facets["s_guard"]["status"] == "not_applicable"
        assert facets["s_closed"]["status"] == "not_evaluated"

    async def test_guard_match_makes_the_shape_applicable(self, svc, domain, install_store):
        install_store(FakeStore(rows=[_guard_match("s_guard")]))

        facets = _by_id(await svc.evaluate(domain, None, entity_uri=ENTITY))

        assert facets["s_guard"]["status"] == "pass"

    async def test_inherited_shape_is_queried_with_the_entity_class(
        self, svc, domain, install_store
    ):
        store = install_store(FakeStore())

        payload = await svc.evaluate(domain, None, entity_uri=ENTITY)

        assert _by_id(payload)["s_name"]["inherited_from"] == "Party"
        assert PARTY not in "\n".join(store.facet_queries)

    async def test_failing_facet_carries_its_violation_rows(self, svc, domain, install_store):
        install_store(FakeStore(rows=[_violation("s_email", count=0)]))

        facets = _by_id(await svc.evaluate(domain, None, entity_uri=ENTITY))

        assert facets["s_email"]["details"] == [{"s": ENTITY, "count": 0}]
        assert facets["s_name"]["details"] == []

    async def test_values_are_read_across_the_uri_separator(self, svc, domain, install_store):
        install_store(FakeStore())

        facets = _by_id(await svc.evaluate(domain, None, entity_uri=ENTITY))

        assert facets["s_name"]["values"] == ["ACME Corp"]
        assert facets["s_phone"]["values"] == ["06 12 34 56 78"]
        assert facets["s_email"]["values"] == []
        assert facets["s_closed"]["values"] == []

    async def test_values_are_capped(self, svc, domain, install_store):
        many = ENTITY_TRIPLES + [
            {"predicate": "https://example.com/onto/email", "object": f"u{i}@x.org"}
            for i in range(25)
        ]
        install_store(FakeStore(triples=many))

        email = _by_id(await svc.evaluate(domain, None, entity_uri=ENTITY))["s_email"]

        assert len(email["values"]) == 20
        assert email["values_truncated"] is True

    async def test_facets_are_ordered_failures_first(self, svc, domain, install_store):
        install_store(FakeStore(rows=[
            _violation("s_phone", val="06"),
            _violation("s_email", count=0),
        ]))

        order = [f["shape_id"] for f in (await svc.evaluate(domain, None, entity_uri=ENTITY))["facets"]]

        assert order[:2] == ["s_email", "s_phone"]  # Violation before Warning
        assert order.index("s_closed") < order.index("s_name")  # not_evaluated before pass
        assert order[-1] == "s_guard"  # not_applicable last

    async def test_summary_counts_statuses_and_failing_severities(
        self, svc, domain, install_store
    ):
        install_store(FakeStore(rows=[
            _violation("s_email", count=0),
            _violation("s_phone", val="06"),
        ]))

        summary = (await svc.evaluate(domain, None, entity_uri=ENTITY))["summary"]

        assert summary == {
            "total": 6, "pass": 2, "fail": 2, "not_applicable": 1,
            "not_evaluated": 1, "error": 0,
            "fail_by_severity": {"sh:Violation": 1, "sh:Warning": 1, "sh:Info": 0},
        }

    async def test_entity_header_fields(self, svc, domain, install_store):
        install_store(FakeStore())

        payload = await svc.evaluate(domain, None, entity_uri=ENTITY)

        assert payload["entity_uri"] == ENTITY
        assert payload["entity_local_id"] == "C-42"
        assert payload["entity_label"] == "ACME Corp"
        assert payload["classes"] == [{"name": "Customer", "uri": CUSTOMER}]
        assert payload["lineage"] == ["Customer", "Party"]

    async def test_one_broken_shape_degrades_to_one_error_facet(
        self, svc, domain, install_store
    ):
        install_store(FakeStore(rows=[_violation("s_email", count=0)], failing={"s_phone"}))

        payload = await svc.evaluate(domain, None, entity_uri=ENTITY)
        facets = _by_id(payload)

        assert payload["success"] is True
        assert facets["s_phone"]["status"] == "error"
        assert "INVALID_PARAMETER_VALUE" in facets["s_phone"]["message"]
        assert facets["s_email"]["status"] == "fail"
        assert facets["s_name"]["status"] == "pass"

    async def test_entity_without_rdf_type_falls_back_to_uri_matching(
        self, svc, domain, install_store
    ):
        untyped = [t for t in ENTITY_TRIPLES if t["predicate"] != RDF_TYPE]
        install_store(FakeStore(triples=untyped))

        payload = await svc.evaluate(domain, None, entity_uri=ENTITY)

        assert payload["classes"] == [{"name": "Customer", "uri": CUSTOMER}]
        assert set(_by_id(payload)) == APPLICABLE_IDS

    async def test_no_applicable_shape_is_an_empty_success(self, svc, domain, install_store):
        domain.shacl_shapes = [dict(S_ORDER)]
        store = install_store(FakeStore())

        payload = await svc.evaluate(domain, None, entity_uri=ENTITY)

        assert payload["success"] is True
        assert payload["facets"] == []
        assert payload["summary"]["total"] == 0
        assert store.facet_queries == []

    async def test_missing_store_is_an_infrastructure_error(
        self, svc, svc_module, domain, monkeypatch
    ):
        monkeypatch.setattr(svc_module, "get_graphdb", lambda *_a, **_kw: None)
        monkeypatch.setattr(svc, "triplestore_table", staticmethod(lambda _d, _s: TABLE))

        with pytest.raises(InfrastructureError):
            await svc.evaluate(domain, None, entity_uri=ENTITY)

    async def test_unreadable_graph_is_an_infrastructure_error(
        self, svc, svc_module, domain, monkeypatch
    ):
        """A domain whose graph was never built has no triple-store table:
        the popup must get a readable 502, not an unhandled 500."""

        class MissingTable:
            def execute_query(self, sql):
                raise RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] triplestore_c360_V1")

        monkeypatch.setattr(svc_module, "get_graphdb", lambda *_a, **_kw: MissingTable())
        monkeypatch.setattr(svc, "triplestore_table", staticmethod(lambda _d, _s: TABLE))

        with pytest.raises(InfrastructureError, match="TABLE_OR_VIEW_NOT_FOUND"):
            await svc.evaluate(domain, None, entity_uri=ENTITY)

    async def test_progress_runs_one_query_per_shape(self, svc, domain, install_store):
        store = install_store(FakeStore())
        messages = []

        await svc.evaluate(
            domain, None, entity_uri=ENTITY,
            progress=lambda _pct, msg: messages.append(msg),
        )

        assert "Reading entity triples..." in messages
        assert any(m.startswith("Checking ") for m in messages)
        assert len(store.facet_queries) > 1
        translatable = ["s_email", "s_name", "s_phone", "s_label", "s_guard"]
        for query in store.facet_queries:
            named = {sid for sid in translatable if f"'{sid}'" in query}
            assert len(named) == 1

    async def test_snapshot_task_queries_the_snapshot_table(
        self, svc, domain, install_store, monkeypatch
    ):
        store = install_store(FakeStore())

        class _Snap:
            table = "cat.sch.dq_snap_t1"
            parallel = True

            def __init__(self, *_a, **_k):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *_a):
                return False

        snap_mod = import_module("back.objects.digitaltwin.quality.DataQualitySnapshot")
        monkeypatch.setattr(snap_mod, "DataQualitySnapshot", _Snap)
        messages = []

        await svc.evaluate(
            domain, None, entity_uri=ENTITY,
            progress=lambda _pct, msg: messages.append(msg),
            snapshot_task_id="t1",
        )

        assert "Materializing DQ snapshot..." in messages
        assert store.facet_queries
        assert all("cat.sch.dq_snap_t1" in q for q in store.facet_queries)
