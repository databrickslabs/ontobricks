"""Studio entity groups: create, update, member moves and delete (release 0.9.0).

Groups behave like Business-View-style sets of entities: every entity belongs
to at most one group, and each class's ``group`` field mirrors membership.
"""

import pytest

from back.core.errors import ValidationError
from back.objects.ontology.Ontology import Ontology
from back.objects.ontology.Groups import Groups

pytestmark = pytest.mark.unit


@pytest.fixture
def session(domain_session):
    domain_session.ontology["classes"] = [
        {"name": n, "uri": f"http://x.org#{n}"} for n in ("Customer", "Order", "Invoice", "Region")
    ]
    domain_session.ontology["groups"] = []
    return domain_session


def _group(name, members, **extra):
    return {"name": name, "label": name, "members": list(members), **extra}


def _class_groups(session):
    return {c["name"]: c["group"] for c in session.get_classes()}


def _members(session):
    return {g["name"]: g["members"] for g in session.groups}


def _actions(session):
    return [e["action"] for e in session._data.get("change_log", [])]


class TestSaveGroup:
    def test_create_appends_group_and_syncs_class_field(self, session):
        groups = Groups(session).save_group(_group("Sales", ["Customer", "Order"]))
        assert [g["name"] for g in groups] == ["Sales"]
        assert _class_groups(session) == {
            "Customer": "Sales", "Order": "Sales", "Invoice": "", "Region": "",
        }
        assert _actions(session) == ["group_added"]

    def test_name_is_required(self, session):
        with pytest.raises(ValidationError, match="Group name is required"):
            Groups(session).save_group({"name": "   ", "members": ["Order"]})

    def test_duplicate_name_rejected_on_create(self, session):
        svc = Groups(session)
        svc.save_group(_group("Sales", ["Customer"]))
        with pytest.raises(ValidationError, match="already exists"):
            svc.save_group(_group("Sales", ["Order"]))
        assert _members(session) == {"Sales": ["Customer"]}

    def test_update_by_index_replaces_group_and_records_diff(self, session):
        svc = Groups(session)
        svc.save_group(_group("Sales", ["Customer"], color="red"))
        svc.save_group(_group("Sales", ["Customer", "Invoice"], color="blue"), index=0)
        assert _members(session) == {"Sales": ["Customer", "Invoice"]}
        event = session._data["change_log"][-1]
        assert event["action"] == "group_updated"
        assert event["meta"]["before"]["color"] == "red"

    def test_new_group_steals_members_from_other_groups(self, session):
        svc = Groups(session)
        svc.save_group(_group("Sales", ["Customer", "Order"]))
        svc.save_group(_group("Billing", ["Order", "Invoice"]))
        assert _members(session) == {"Sales": ["Customer"], "Billing": ["Order", "Invoice"]}
        assert _class_groups(session)["Order"] == "Billing"

    def test_out_of_range_index_creates_instead_of_updating(self, session):
        svc = Groups(session)
        svc.save_group(_group("Sales", ["Customer"]))
        svc.save_group(_group("Geo", ["Region"]), index=5)
        assert [g["name"] for g in session.groups] == ["Sales", "Geo"]


class TestDeleteGroup:
    def test_delete_by_index_clears_class_group_field(self, session):
        svc = Groups(session)
        svc.save_group(_group("Sales", ["Customer"]))
        svc.save_group(_group("Geo", ["Region"]))
        groups = svc.delete_group(index=0)
        assert [g["name"] for g in groups] == ["Geo"]
        assert _class_groups(session)["Customer"] == ""
        assert _class_groups(session)["Region"] == "Geo"
        event = session._data["change_log"][-1]
        assert (event["action"], event["entity_ref"]) == ("group_removed", "Sales")

    def test_delete_by_name(self, session):
        svc = Groups(session)
        svc.save_group(_group("Sales", ["Customer"]))
        svc.delete_group(name="Sales")
        assert session.groups == []
        assert all(v == "" for v in _class_groups(session).values())

    def test_delete_unknown_name_is_a_noop_but_audited(self, session):
        svc = Groups(session)
        svc.save_group(_group("Sales", ["Customer"]))
        svc.delete_group(name="Ghost")
        assert _members(session) == {"Sales": ["Customer"]}
        assert session._data["change_log"][-1]["entity_ref"] == "Ghost"

    def test_index_without_name_falls_back_to_index_ref(self, session):
        session.ontology["groups"] = [{"members": ["Order"]}]
        Groups(session).delete_group(index=0)
        assert session.groups == []
        assert session._data["change_log"][-1]["entity_ref"] == "0"

    def test_missing_identifier_rejected(self, session):
        with pytest.raises(ValidationError, match="Provide index or name"):
            Groups(session).delete_group()


class TestUpdateGroupMembers:
    def test_add_and_remove_members(self, session):
        svc = Groups(session)
        svc.save_group(_group("Sales", ["Customer", "Order"]))
        svc.update_group_members("Sales", add=["Invoice", "Invoice", ""], remove=["Order"])
        assert _members(session) == {"Sales": ["Customer", "Invoice"]}
        assert _class_groups(session) == {
            "Customer": "Sales", "Order": "", "Invoice": "Sales", "Region": "",
        }

    def test_moving_member_removes_it_from_previous_group(self, session):
        svc = Groups(session)
        svc.save_group(_group("Sales", ["Customer", "Order"]))
        svc.save_group(_group("Billing", ["Invoice"]))
        svc.update_group_members("Billing", add=["Order"])
        assert _members(session) == {"Sales": ["Customer"], "Billing": ["Invoice", "Order"]}
        assert _class_groups(session)["Order"] == "Billing"

    def test_moving_several_members_at_once_keeps_membership_exclusive(self, session):
        svc = Groups(session)
        svc.save_group(_group("Sales", ["Customer", "Order", "Invoice"]))
        svc.save_group(_group("Geo", ["Region"]))
        svc.update_group_members("Geo", add=["Customer", "Order"])
        all_members = [m for ms in _members(session).values() for m in ms]
        assert sorted(all_members) == sorted(set(all_members))
        assert _members(session) == {"Sales": ["Invoice"], "Geo": ["Region", "Customer", "Order"]}

    def test_existing_member_not_duplicated(self, session):
        svc = Groups(session)
        svc.save_group(_group("Sales", ["Customer"]))
        svc.update_group_members("Sales", add=["Customer"])
        assert _members(session) == {"Sales": ["Customer"]}

    def test_group_name_required(self, session):
        with pytest.raises(ValidationError, match="Group name is required"):
            Groups(session).update_group_members("", add=["Order"])

    def test_unknown_group_rejected(self, session):
        with pytest.raises(ValidationError, match="not found"):
            Groups(session).update_group_members("Ghost", add=["Order"])


class TestHelpers:
    def test_exclusive_membership_ignores_unknown_authority(self):
        groups = [_group("A", ["x"]), _group("B", ["x"])]
        Groups._enforce_exclusive_membership(groups, "Missing")
        assert groups == [_group("A", ["x"]), _group("B", ["x"])]


class TestOntologyFacadeGroups:
    def test_facade_delegates_full_group_lifecycle(self, session):
        ont = Ontology(session)
        ont.save_group(_group("Sales", ["Customer"]))
        ont.update_group_members("Sales", add=["Order"])
        assert _members(session) == {"Sales": ["Customer", "Order"]}
        groups = [_group("A", ["Customer"]), _group("B", ["Customer", "Order"])]
        Ontology._enforce_exclusive_membership(groups, "A")
        assert groups[1]["members"] == ["Order"]
        ont._sync_class_group_field([_group("Z", ["Region"])])
        assert _class_groups(session)["Region"] == "Z"
        ont.delete_group(name="Sales")
        assert session.groups == []
