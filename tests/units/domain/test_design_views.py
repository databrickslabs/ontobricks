"""Unit tests for Domain design-view management methods.

Covers the full create → switch → save → get → rename → delete lifecycle,
including the legacy-layout migration path and the visibility payload used
by the auto-generated "Create Business View" feature.
"""

import copy
import pytest
from unittest.mock import MagicMock

from back.objects.domain import Domain
from back.core.errors import ValidationError, ConflictError, NotFoundError

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_session(design_layout=None):
    """Return a minimal session mock with an in-memory _data dict."""
    session = MagicMock()
    session._data = {"design_layout": design_layout if design_layout is not None else {}}
    session.save = MagicMock()
    return session


def _domain(design_layout=None):
    return Domain(_make_session(design_layout))


# Pre-built layouts used across multiple tests
_EMPTY_VIEWS = {"views": {}, "current_view": None, "map": {}}

_ONE_VIEW = {
    "views": {
        "default": {
            "entities": [{"id": "e1", "name": "Customer", "x": 100, "y": 100}],
            "relationships": [],
            "inheritances": [],
        }
    },
    "current_view": "default",
    "map": {},
}


# ===========================================================================
# get_design_views
# ===========================================================================

class TestGetDesignViews:
    def test_empty_returns_no_views(self):
        r = _domain({}).get_design_views()
        assert r["success"] is True
        assert r["views"] == []
        assert r["current_view"] is None

    def test_views_structure_returned(self):
        r = _domain(copy.deepcopy(_ONE_VIEW)).get_design_views()
        assert r["success"] is True
        assert "default" in r["views"]
        assert r["current_view"] == "default"

    def test_no_current_view_falls_back_to_first(self):
        layout = {"views": {"alpha": {}, "beta": {}}, "map": {}}
        r = _domain(layout).get_design_views()
        assert r["current_view"] == "alpha"

    def test_legacy_layout_with_entities_reports_default(self):
        """Old format (no 'views' key but has entities) is treated as 'default'."""
        legacy = {"entities": [{"name": "A"}], "relationships": []}
        r = _domain(legacy).get_design_views()
        assert "default" in r["views"]
        assert r["current_view"] == "default"

    def test_legacy_layout_empty_reports_no_views(self):
        r = _domain({"entities": [], "relationships": []}).get_design_views()
        assert r["views"] == []
        assert r["current_view"] is None


# ===========================================================================
# create_design_view
# ===========================================================================

class TestCreateDesignView:
    def test_creates_new_view(self):
        d = _domain(copy.deepcopy(_EMPTY_VIEWS))
        r = d.create_design_view("MyView", copy_from=None)
        assert r["success"] is True
        assert "MyView" in r["views"]

    def test_empty_name_raises(self):
        d = _domain(copy.deepcopy(_EMPTY_VIEWS))
        with pytest.raises(ValidationError):
            d.create_design_view("", copy_from=None)

    def test_duplicate_name_raises_conflict(self):
        d = _domain(copy.deepcopy(_ONE_VIEW))
        with pytest.raises(ConflictError):
            d.create_design_view("default", copy_from=None)

    def test_copy_from_clones_content(self):
        layout = copy.deepcopy(_ONE_VIEW)
        d = _domain(layout)
        r = d.create_design_view("Clone", copy_from="default")
        assert "Clone" in r["views"]
        saved = d._s._data["design_layout"]["views"]["Clone"]
        assert saved["entities"][0]["name"] == "Customer"

    def test_copy_from_missing_source_creates_empty(self):
        d = _domain(copy.deepcopy(_ONE_VIEW))
        r = d.create_design_view("Fresh", copy_from="nonexistent")
        assert d._s._data["design_layout"]["views"]["Fresh"]["entities"] == []

    def test_legacy_layout_migrated_on_create(self):
        """Creating a view on a legacy (no-views) layout migrates old data."""
        legacy = {
            "entities": [{"name": "X"}],
            "relationships": [{"name": "r"}],
            "inheritances": [],
        }
        d = _domain(legacy)
        d.create_design_view("v2", copy_from=None)
        layout = d._s._data["design_layout"]
        assert "views" in layout
        # Old data preserved under 'default'
        assert layout["views"]["default"]["entities"] == [{"name": "X"}]
        assert "v2" in layout["views"]

    def test_save_called(self):
        d = _domain(copy.deepcopy(_EMPTY_VIEWS))
        d.create_design_view("X", copy_from=None)
        d._s.save.assert_called()


# ===========================================================================
# switch_design_view
# ===========================================================================

class TestSwitchDesignView:
    def test_switches_current_view(self):
        layout = {
            "views": {"a": {"entities": []}, "b": {"entities": []}},
            "current_view": "a",
            "map": {},
        }
        d = _domain(layout)
        r = d.switch_design_view("b")
        assert r["success"] is True
        assert r["current_view"] == "b"
        assert d._s._data["design_layout"]["current_view"] == "b"

    def test_unknown_view_raises_not_found(self):
        d = _domain(copy.deepcopy(_ONE_VIEW))
        with pytest.raises(NotFoundError):
            d.switch_design_view("ghost")

    def test_empty_name_raises_validation(self):
        d = _domain(copy.deepcopy(_ONE_VIEW))
        with pytest.raises(ValidationError):
            d.switch_design_view("")

    def test_no_views_key_raises_validation(self):
        d = _domain({})
        with pytest.raises(ValidationError):
            d.switch_design_view("default")

    def test_returns_layout_content(self):
        d = _domain(copy.deepcopy(_ONE_VIEW))
        r = d.switch_design_view("default")
        assert "entities" in r["layout"]


# ===========================================================================
# get_current_design_view
# ===========================================================================

class TestGetCurrentDesignView:
    def test_returns_current_layout(self):
        d = _domain(copy.deepcopy(_ONE_VIEW))
        r = d.get_current_design_view()
        assert r["success"] is True
        assert r["current_view"] == "default"
        assert "entities" in r["layout"]

    def test_legacy_layout_returned_as_default(self):
        legacy = {"entities": [{"name": "A"}], "relationships": []}
        d = _domain(legacy)
        r = d.get_current_design_view()
        assert r["current_view"] == "default"
        assert "entities" in r["layout"]


# ===========================================================================
# save_current_design_view
# ===========================================================================

class TestSaveCurrentDesignView:
    def _layout_payload(self, extra_entity_fields=None):
        ent = {"id": "e1", "name": "Customer", "x": 450, "y": 280}
        if extra_entity_fields:
            ent.update(extra_entity_fields)
        return {
            "entities": [ent],
            "relationships": [
                {
                    "id": "r1",
                    "name": "orders",
                    "sourceEntityId": "e1",
                    "targetEntityId": "e2",
                }
            ],
            "inheritances": [],
            "visibility": {
                "hiddenEntities": ["Order", "Product"],
                "hiddenRelationships": [],
                "hiddenInheritances": [],
            },
        }

    def test_saves_to_current_view(self):
        d = _domain(copy.deepcopy(_ONE_VIEW))
        payload = self._layout_payload()
        r = d.save_current_design_view(payload)
        assert r["success"] is True
        assert r["current_view"] == "default"
        saved = d._s._data["design_layout"]["views"]["default"]
        assert saved["entities"][0]["name"] == "Customer"

    def test_entity_fields_stripped_to_allowed_set(self):
        """save_current strips unknown entity keys (security / storage hygiene)."""
        d = _domain(copy.deepcopy(_ONE_VIEW))
        payload = self._layout_payload(extra_entity_fields={"__proto__": "evil", "secret": "x"})
        d.save_current_design_view(payload)
        saved_ent = d._s._data["design_layout"]["views"]["default"]["entities"][0]
        assert "secret" not in saved_ent
        assert "__proto__" not in saved_ent
        assert saved_ent["name"] == "Customer"

    def test_visibility_payload_persisted(self):
        """hiddenEntities / hiddenRelationships are stored verbatim (used by designer)."""
        d = _domain(copy.deepcopy(_ONE_VIEW))
        payload = self._layout_payload()
        d.save_current_design_view(payload)
        saved = d._s._data["design_layout"]["views"]["default"]
        assert saved["visibility"]["hiddenEntities"] == ["Order", "Product"]

    def test_auto_creates_views_structure_if_missing(self):
        d = _domain({})
        d.save_current_design_view({"entities": [], "relationships": [], "inheritances": []})
        assert "views" in d._s._data["design_layout"]
        assert "default" in d._s._data["design_layout"]["views"]

    def test_save_called(self):
        d = _domain(copy.deepcopy(_ONE_VIEW))
        d.save_current_design_view({"entities": [], "relationships": [], "inheritances": []})
        d._s.save.assert_called()


# ===========================================================================
# rename_design_view
# ===========================================================================

class TestRenameDesignView:
    def test_renames_view(self):
        d = _domain(copy.deepcopy(_ONE_VIEW))
        r = d.rename_design_view("default", "renamed")
        assert r["success"] is True
        assert "renamed" in r["views"]
        assert "default" not in r["views"]

    def test_updates_current_view_pointer(self):
        d = _domain(copy.deepcopy(_ONE_VIEW))
        d.rename_design_view("default", "new_name")
        assert d._s._data["design_layout"]["current_view"] == "new_name"

    def test_empty_names_raise(self):
        d = _domain(copy.deepcopy(_ONE_VIEW))
        with pytest.raises(ValidationError):
            d.rename_design_view("", "x")
        with pytest.raises(ValidationError):
            d.rename_design_view("default", "")

    def test_missing_source_raises_not_found(self):
        d = _domain(copy.deepcopy(_ONE_VIEW))
        with pytest.raises(NotFoundError):
            d.rename_design_view("ghost", "new")

    def test_target_already_exists_raises_conflict(self):
        layout = {"views": {"a": {}, "b": {}}, "current_view": "a", "map": {}}
        d = _domain(layout)
        with pytest.raises(ConflictError):
            d.rename_design_view("a", "b")


# ===========================================================================
# delete_design_view
# ===========================================================================

class TestDeleteDesignView:
    def test_deletes_view(self):
        layout = {
            "views": {"v1": {}, "v2": {}},
            "current_view": "v1",
            "map": {},
        }
        d = _domain(layout)
        r = d.delete_design_view("v2")
        assert r["success"] is True
        assert "v2" not in r["views"]

    def test_deletes_current_view_switches_to_remaining(self):
        layout = {
            "views": {"a": {}, "b": {}},
            "current_view": "a",
            "map": {},
        }
        d = _domain(layout)
        r = d.delete_design_view("a")
        assert r["current_view"] == "b"

    def test_delete_last_view_sets_current_none(self):
        d = _domain(copy.deepcopy(_ONE_VIEW))
        r = d.delete_design_view("default")
        assert r["views"] == []
        assert r["current_view"] is None

    def test_empty_name_raises(self):
        d = _domain(copy.deepcopy(_ONE_VIEW))
        with pytest.raises(ValidationError):
            d.delete_design_view("")

    def test_missing_view_raises_not_found(self):
        d = _domain(copy.deepcopy(_ONE_VIEW))
        with pytest.raises(NotFoundError):
            d.delete_design_view("ghost")


# ===========================================================================
# Full create → switch → save flow (mirrors JS createBusinessViewFromEntity)
# ===========================================================================

class TestCreateBusinessViewFlow:
    """Integration-style test that walks through the exact three-step
    sequence performed by the JS createBusinessViewFromEntity helper."""

    def _build_visibility_payload(self, visible_names, all_names):
        hidden = [n for n in all_names if n not in visible_names]
        return {
            "hiddenEntities": hidden,
            "hiddenRelationships": [],
            "hiddenInheritances": [],
        }

    def test_end_to_end(self):
        # Start from an existing domain with one regular view
        d = _domain(copy.deepcopy(_ONE_VIEW))

        all_nodes = ["Customer", "Order", "Product", "Invoice"]
        visible = {"Customer", "Order", "Invoice"}

        # 1. Create the auto view
        view_name = "Auto_Customer"
        r_create = d.create_design_view(view_name, copy_from=None)
        assert view_name in r_create["views"]

        # 2. Switch to it
        r_switch = d.switch_design_view(view_name)
        assert r_switch["current_view"] == view_name

        # 3. Save layout with visibility
        payload = {
            "entities": [
                {"id": f"ent_{i}", "name": n, "x": 100 * i, "y": 100}
                for i, n in enumerate(sorted(visible))
            ],
            "relationships": [
                {
                    "id": "rel_0",
                    "name": "orders",
                    "sourceEntityId": "ent_0",
                    "targetEntityId": "ent_1",
                }
            ],
            "inheritances": [],
            "visibility": self._build_visibility_payload(visible, all_nodes),
        }
        r_save = d.save_current_design_view(payload)
        assert r_save["success"] is True
        assert r_save["current_view"] == view_name

        # Verify persistence
        saved = d._s._data["design_layout"]["views"][view_name]
        assert saved["visibility"]["hiddenEntities"] == ["Product"]
        assert len(saved["entities"]) == len(visible)
        assert d._s.save.call_count >= 3

    def test_numeric_suffix_uniqueness(self):
        """Simulates the JS name-collision loop: Auto_X → Auto_X_1 → Auto_X_2."""
        d = _domain(copy.deepcopy(_EMPTY_VIEWS))
        d.create_design_view("Auto_X", copy_from=None)
        d.create_design_view("Auto_X_1", copy_from=None)
        d.create_design_view("Auto_X_2", copy_from=None)
        views = d._s._data["design_layout"]["views"]
        assert all(n in views for n in ("Auto_X", "Auto_X_1", "Auto_X_2"))
        # Next unique name would be Auto_X_3 — confirm it doesn't exist yet
        assert "Auto_X_3" not in views
