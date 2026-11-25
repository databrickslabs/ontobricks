"""The store-level review gate: Evolve evidence on submit, stale versions refused."""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import ValidationError
from back.objects.ontology.evolution import design_hash
from back.objects.registry.PermissionService import ROLE_BUILDER
from back.objects.registry.RegistryCfg import RegistryCfg
from tests.units.registry._v090_fake_pg import (  # noqa: F401
    QSCHEMA,
    FakeDB,
    fake_psycopg,
    make_store,
)
from back.objects.registry.RegistryService import RegistryService
from back.objects.registry.collab.ReviewService import ReviewService

pytestmark = pytest.mark.unit

_review = importlib.import_module("back.objects.registry.collab.ReviewService")

ONTOLOGY = {"classes": [{"name": "Order", "uri": "urn:Order"}], "properties": []}
ASSIGNMENT = {"entities": [], "relationships": []}


def _doc(stale=False):
    ontology = dict(ONTOLOGY)
    link = {
        "run_id": "r1",
        "parent_version": "1",
        "decision": "PASS",
        "evaluation_hash": "e" * 64,
        "design_hash": design_hash(ONTOLOGY, ASSIGNMENT),
    }
    if stale:
        ontology = {**ONTOLOGY, "classes": []}
    return {
        "info": {"status": "DRAFT", "last_build": "", "evolution": link},
        "versions": {"2": {"ontology": ontology, "assignment": ASSIGNMENT}},
    }


def _store(doc, db=None):
    db = db or FakeDB()
    store = make_store(db)
    store.read_version = lambda folder, version, include_simulations=True: (
        True,
        doc,
        "",
    )
    return store, db


def test_stale_evolve_version_cannot_enter_review(fake_psycopg):
    store, db = _store(_doc(stale=True))
    with pytest.raises(ValidationError):
        store.update_version_status("acme", "2", "IN-REVIEW")
    assert not db.sql_with("UPDATE " + QSCHEMA + ".domain_versions")


def test_unchanged_evolve_version_enters_review(fake_psycopg):
    db = FakeDB().on("UPDATE " + QSCHEMA + ".domain_versions", rowcount=1)
    store, db = _store(_doc(), db)
    assert store.update_version_status("acme", "2", "IN-REVIEW")[0] is True


def test_other_transitions_and_ordinary_versions_are_unaffected(fake_psycopg):
    db = FakeDB().on("UPDATE " + QSCHEMA + ".domain_versions", rowcount=1, once=False)
    store, _ = _store(_doc(stale=True), db)
    assert store.update_version_status("acme", "2", "DRAFT")[0] is True
    store, _ = _store({"info": {}, "versions": {"2": {"ontology": ONTOLOGY}}}, db)
    assert store.update_version_status("acme", "2", "IN-REVIEW")[0] is True


def test_submit_event_carries_the_evaluation(fake_psycopg):
    import json

    db = FakeDB().on("INSERT INTO " + QSCHEMA + ".domain_review_events", rowcount=1)
    store, db = _store(_doc(), db)
    store.record_review_event(
        "acme",
        "2",
        "a@x",
        "submitted",
        from_status="DRAFT",
        to_status="IN-REVIEW",
        meta={"source": "lifecycle"},
    )
    params = db.one("INSERT INTO " + QSCHEMA + ".domain_review_events").params
    meta = json.loads(params[6])
    assert meta["source"] == "lifecycle" and meta["evolution"]["run_id"] == "r1"


def test_audit_row_lands_even_when_the_link_is_stale(fake_psycopg):
    db = FakeDB().on("INSERT INTO " + QSCHEMA + ".domain_review_events", rowcount=1)
    store, db = _store(_doc(stale=True), db)
    ok, _ = store.record_review_event(
        "acme", "2", "a@x", "submitted", to_status="IN-REVIEW"
    )
    assert ok is True


def test_review_service_submit_is_refused_through_the_store(fake_psycopg):
    store, db = _store(_doc(stale=True))
    svc = RegistryService(RegistryCfg("cat", "sch", "vol"), MagicMock(), store=store)
    svc.list_versions_sorted = MagicMock(return_value=["1", "2"])
    domain = MagicMock(domain_folder="other", current_version="9", info={})
    request = MagicMock()
    request.state.user_email = "a@x"
    request.headers = {}
    with (
        patch.multiple(
            _review,
            get_domain=MagicMock(return_value=domain),
            invalidate_registry_cache=MagicMock(),
        ),
        patch.object(_review.RegistryService, "from_context", return_value=svc),
    ):
        with pytest.raises(ValidationError):
            ReviewService.submit(
                request,
                MagicMock(),
                MagicMock(),
                "acme",
                "2",
                comment="",
                user_role="",
                user_domain_role=ROLE_BUILDER,
            )
    assert not db.sql_with("UPDATE " + QSCHEMA + ".domain_versions")
