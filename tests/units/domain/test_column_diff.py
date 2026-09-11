"""Tests for the shared column-diff helper used by metadata refresh preview."""

from back.objects.domain.Domain import compute_column_diff, has_column_changes


def _names(entries):
    return sorted(e["name"] for e in entries)


class TestComputeColumnDiff:
    def test_added_column(self):
        old = [{"name": "id", "type": "int"}]
        new = [{"name": "id", "type": "int"}, {"name": "email", "type": "string"}]
        diff = compute_column_diff(old, new)
        assert _names(diff["added"]) == ["email"]
        assert diff["removed"] == []
        assert _names(diff["unchanged"]) == ["id"]

    def test_removed_column(self):
        old = [{"name": "id", "type": "int"}, {"name": "legacy", "type": "string"}]
        new = [{"name": "id", "type": "int"}]
        diff = compute_column_diff(old, new)
        assert _names(diff["removed"]) == ["legacy"]
        assert diff["added"] == []
        assert _names(diff["unchanged"]) == ["id"]

    def test_rename_reported_as_remove_plus_add(self):
        old = [{"name": "cust_id", "type": "int"}]
        new = [{"name": "customer_id", "type": "int"}]
        diff = compute_column_diff(old, new)
        assert _names(diff["added"]) == ["customer_id"]
        assert _names(diff["removed"]) == ["cust_id"]
        assert diff["unchanged"] == []

    def test_type_change(self):
        old = [{"name": "amount", "type": "int"}]
        new = [{"name": "amount", "type": "decimal(10,2)"}]
        diff = compute_column_diff(old, new)
        assert diff["type_changed"] == [
            {"name": "amount", "old_type": "int", "new_type": "decimal(10,2)"}
        ]
        assert diff["unchanged"] == []

    def test_comment_change_alone_is_unchanged(self):
        old = [{"name": "id", "type": "int", "comment": "old"}]
        new = [{"name": "id", "type": "int", "comment": "new"}]
        diff = compute_column_diff(old, new)
        assert _names(diff["unchanged"]) == ["id"]
        assert not has_column_changes(diff)

    def test_empty_old(self):
        diff = compute_column_diff([], [{"name": "id", "type": "int"}])
        assert _names(diff["added"]) == ["id"]
        assert diff["removed"] == []

    def test_empty_new(self):
        diff = compute_column_diff([{"name": "id", "type": "int"}], [])
        assert _names(diff["removed"]) == ["id"]
        assert diff["added"] == []

    def test_both_empty(self):
        diff = compute_column_diff([], [])
        assert diff == {
            "added": [],
            "removed": [],
            "type_changed": [],
            "unchanged": [],
        }

    def test_none_inputs(self):
        diff = compute_column_diff(None, None)
        assert not has_column_changes(diff)

    def test_col_name_and_data_type_keys(self):
        """Stored metadata may use col_name/data_type; UC fetches use name/type."""
        old = [{"col_name": "id", "data_type": "int"}]
        new = [{"name": "id", "type": "bigint"}]
        diff = compute_column_diff(old, new)
        assert diff["type_changed"] == [
            {"name": "id", "old_type": "int", "new_type": "bigint"}
        ]

    def test_unnamed_columns_are_skipped(self):
        """DESCRIBE output can include blank separator rows."""
        old = [{"name": "", "type": ""}, {"name": "id", "type": "int"}]
        new = [{"name": "id", "type": "int"}]
        diff = compute_column_diff(old, new)
        assert not has_column_changes(diff)


class TestHasColumnChanges:
    def test_true_for_each_change_kind(self):
        assert has_column_changes({"added": [{"name": "a"}]})
        assert has_column_changes({"removed": [{"name": "a"}]})
        assert has_column_changes({"type_changed": [{"name": "a"}]})

    def test_false_when_only_unchanged(self):
        assert not has_column_changes({"unchanged": [{"name": "a"}]})

    def test_false_for_empty_dict(self):
        assert not has_column_changes({})
