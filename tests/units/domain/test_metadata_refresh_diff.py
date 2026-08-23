"""Tests for the metadata-refresh diff on both the async and sync paths."""

import sys
from unittest.mock import MagicMock

import pytest

from back.objects.domain import Domain, _metadata_tasks

# ``back.objects.domain.Domain`` resolves to the re-exported *class*, so the
# module holding the patchable module-level names has to come from sys.modules.
import back.objects.domain.Domain as _  # noqa: F401  (ensures it is imported)

domain_module = sys.modules["back.objects.domain.Domain"]


class _FakeClient:
    """Stands in for DatabricksClient with a canned post-refresh schema."""

    def __init__(self, columns_by_table):
        self._columns = columns_by_table

    def get_table_columns(self, catalog, schema, table):
        return self._columns.get(table, [])

    def get_table_comment(self, catalog, schema, table):
        return ""

    def check_table_select_permission(self, catalog, schema, table):
        return {"can_select": True, "error": ""}


@pytest.fixture
def task_manager(monkeypatch):
    tm = MagicMock()
    monkeypatch.setattr(_metadata_tasks, "get_task_manager", lambda: tm)
    return tm


def _run(monkeypatch, task_manager, existing_tables, new_columns_by_table):
    monkeypatch.setattr(
        _metadata_tasks,
        "DatabricksClient",
        lambda **kwargs: _FakeClient(new_columns_by_table),
    )
    existing_metadata = {
        "tables": list(existing_tables.values()),
        "table_count": len(existing_tables),
    }
    _metadata_tasks.run_metadata_update_task(
        "task-1",
        "https://host",
        "token",
        "wh",
        "cat",
        "sch",
        list(existing_tables.keys()),
        existing_metadata,
        existing_tables,
    )
    return task_manager.complete_task.call_args.kwargs["result"]


def _table(name, columns):
    return {"name": name, "full_name": f"cat.sch.{name}", "columns": columns}


class TestUpdateTaskDiff:
    def test_reports_added_and_removed_columns(self, monkeypatch, task_manager):
        existing = {
            "customers": _table(
                "customers",
                [{"name": "id", "type": "int"}, {"name": "legacy", "type": "string"}],
            )
        }
        result = _run(
            monkeypatch,
            task_manager,
            existing,
            {
                "customers": [
                    {"name": "id", "type": "int"},
                    {"name": "email", "type": "string"},
                ]
            },
        )
        diff = result["diff"]["customers"]
        assert [c["name"] for c in diff["added"]] == ["email"]
        assert [c["name"] for c in diff["removed"]] == ["legacy"]

    def test_reports_type_changes(self, monkeypatch, task_manager):
        existing = {"orders": _table("orders", [{"name": "total", "type": "int"}])}
        result = _run(
            monkeypatch,
            task_manager,
            existing,
            {"orders": [{"name": "total", "type": "decimal(10,2)"}]},
        )
        assert result["diff"]["orders"]["type_changed"] == [
            {"name": "total", "old_type": "int", "new_type": "decimal(10,2)"}
        ]

    def test_unchanged_table_is_absent_from_the_diff(self, monkeypatch, task_manager):
        """A no-op refresh must not trigger the review modal."""
        existing = {"orders": _table("orders", [{"name": "id", "type": "int"}])}
        result = _run(
            monkeypatch,
            task_manager,
            existing,
            {"orders": [{"name": "id", "type": "int"}]},
        )
        assert result["diff"] == {}

    def test_diff_snapshots_columns_before_the_merge(self, monkeypatch, task_manager):
        """merge_table_metadata overwrites columns in place, so the diff must
        be computed first or the previous schema is lost."""
        existing = {"orders": _table("orders", [{"name": "old_col", "type": "int"}])}
        result = _run(
            monkeypatch,
            task_manager,
            existing,
            {"orders": [{"name": "new_col", "type": "int"}]},
        )
        assert [c["name"] for c in result["diff"]["orders"]["removed"]] == ["old_col"]
        # The merge still applied to the in-memory metadata.
        assert [c["name"] for c in existing["orders"]["columns"]] == ["new_col"]

    def test_diff_covers_multiple_tables(self, monkeypatch, task_manager):
        existing = {
            "customers": _table("customers", [{"name": "id", "type": "int"}]),
            "orders": _table("orders", [{"name": "id", "type": "int"}]),
        }
        result = _run(
            monkeypatch,
            task_manager,
            existing,
            {
                "customers": [
                    {"name": "id", "type": "int"},
                    {"name": "email", "type": "string"},
                ],
                "orders": [{"name": "id", "type": "int"}],
            },
        )
        assert set(result["diff"]) == {"customers"}

    def test_task_result_keeps_existing_keys(self, monkeypatch, task_manager):
        existing = {"orders": _table("orders", [{"name": "id", "type": "int"}])}
        result = _run(
            monkeypatch,
            task_manager,
            existing,
            {"orders": [{"name": "id", "type": "int"}]},
        )
        for key in ("message", "updated_count", "total_count", "errors", "metadata"):
            assert key in result

    def test_task_does_not_persist_the_session(self, monkeypatch, task_manager):
        """The task only mutates the in-memory metadata; persistence is the
        browser's follow-up /domain/metadata/save call, which is what the
        review modal gates."""
        existing = {"orders": _table("orders", [{"name": "id", "type": "int"}])}
        result = _run(
            monkeypatch,
            task_manager,
            existing,
            {"orders": [{"name": "id", "type": "int"}]},
        )
        assert "metadata" in result
        task_manager.fail_task.assert_not_called()

    def test_a_failing_table_does_not_lose_the_other_diffs(
        self, monkeypatch, task_manager
    ):
        existing = {
            "customers": _table("customers", [{"name": "id", "type": "int"}]),
            "boom": _table("boom", [{"name": "id", "type": "int"}]),
        }
        class _PartlyBrokenClient(_FakeClient):
            def get_table_columns(self, catalog, schema, table):
                if table == "boom":
                    raise RuntimeError("describe failed")
                return super().get_table_columns(catalog, schema, table)

        monkeypatch.setattr(
            _metadata_tasks,
            "DatabricksClient",
            lambda **kwargs: _PartlyBrokenClient(
                {"customers": [{"name": "id", "type": "bigint"}]}
            ),
        )
        _metadata_tasks.run_metadata_update_task(
            "task-1",
            "https://host",
            "token",
            "wh",
            "cat",
            "sch",
            list(existing.keys()),
            {"tables": list(existing.values())},
            existing,
        )
        result = task_manager.complete_task.call_args.kwargs["result"]
        assert "customers" in result["diff"]
        assert "boom" not in result["diff"]
        assert result["errors"] and "boom" in result["errors"][0]


class TestSyncUpdateDiff:
    """`Domain.update_metadata_tables` must report the same diff as the async
    task — the UI only drives the async path, but the two must not disagree."""

    def _domain(self, monkeypatch, tables, new_columns_by_table):
        monkeypatch.setattr(
            domain_module,
            "get_databricks_host_and_token",
            lambda session, settings: ("https://host", "token"),
        )
        monkeypatch.setattr(
            domain_module, "resolve_warehouse_id", lambda session, settings: "wh"
        )
        monkeypatch.setattr(
            domain_module,
            "DatabricksClient",
            lambda **kwargs: _FakeClient(new_columns_by_table),
        )
        session = MagicMock()
        metadata = {"tables": list(tables), "table_count": len(tables)}
        session.catalog_metadata = metadata
        session._data = {"domain": {"metadata": metadata}}
        return Domain(session, MagicMock())

    def test_reports_added_and_removed_columns(self, monkeypatch):
        domain = self._domain(
            monkeypatch,
            [
                _table(
                    "customers",
                    [
                        {"name": "id", "type": "int"},
                        {"name": "legacy", "type": "string"},
                    ],
                )
            ],
            {
                "customers": [
                    {"name": "id", "type": "int"},
                    {"name": "email", "type": "string"},
                ]
            },
        )
        result = domain.update_metadata_tables(None)
        diff = result["diff"]["customers"]
        assert [c["name"] for c in diff["added"]] == ["email"]
        assert [c["name"] for c in diff["removed"]] == ["legacy"]

    def test_unchanged_table_is_absent_from_the_diff(self, monkeypatch):
        domain = self._domain(
            monkeypatch,
            [_table("orders", [{"name": "id", "type": "int"}])],
            {"orders": [{"name": "id", "type": "int"}]},
        )
        assert domain.update_metadata_tables(None)["diff"] == {}

    def test_type_change_reported(self, monkeypatch):
        domain = self._domain(
            monkeypatch,
            [_table("orders", [{"name": "total", "type": "int"}])],
            {"orders": [{"name": "total", "type": "decimal(10,2)"}]},
        )
        assert domain.update_metadata_tables(None)["diff"]["orders"][
            "type_changed"
        ] == [{"name": "total", "old_type": "int", "new_type": "decimal(10,2)"}]

    def test_sync_path_does_persist(self, monkeypatch):
        """Unlike the async task, this one is the write — there is no browser
        round-trip afterwards to gate."""
        domain = self._domain(
            monkeypatch,
            [_table("orders", [{"name": "id", "type": "int"}])],
            {"orders": [{"name": "id", "type": "bigint"}]},
        )
        domain.update_metadata_tables(None)
        domain._s.save.assert_called_once()


class TestAsyncUpdateDoesNotMutateTheLiveSession:
    """Regression: ``start_metadata_update_async`` must hand the background
    thread an isolated snapshot, not ``self._s.catalog_metadata`` itself.

    ``merge_table_metadata`` overwrites ``old_table["columns"]`` in place;
    without a deep copy first, that mutation lands directly on the live
    session dict the moment the UC re-fetch completes — before the user has
    even seen the review modal — which silently defeats "Discard" (Found by
    running the Pillar-1 live scenario end to end: the stored metadata showed
    the renamed column even though the test deliberately never called
    ``/domain/metadata/save``).
    """

    def _start(self, monkeypatch, tables, new_columns_by_table):
        monkeypatch.setattr(
            domain_module,
            "get_databricks_host_and_token",
            lambda session, settings: ("https://host", "token"),
        )
        monkeypatch.setattr(
            domain_module, "resolve_warehouse_id", lambda session, settings: "wh"
        )
        captured_thread_args = {}

        class _ImmediateThread:
            """Runs the target synchronously so the test stays deterministic."""

            def __init__(self, target, args, daemon=None):
                captured_thread_args["args"] = args
                self._target = target
                self._args = args

            def start(self):
                self._target(*self._args)

        monkeypatch.setattr(domain_module.threading, "Thread", _ImmediateThread)
        monkeypatch.setattr(
            _metadata_tasks,
            "DatabricksClient",
            lambda **kwargs: _FakeClient(new_columns_by_table),
        )

        session = MagicMock()
        live_metadata = {"tables": list(tables), "table_count": len(tables)}
        session.catalog_metadata = live_metadata
        session._data = {"domain": {"metadata": live_metadata}}
        domain = Domain(session, MagicMock())
        domain.start_metadata_update_async(None)
        return live_metadata, captured_thread_args["args"]

    def test_thread_receives_a_copy_not_the_live_metadata_object(self, monkeypatch):
        live_metadata, args = self._start(
            monkeypatch,
            [_table("customers", [{"name": "email", "type": "string"}])],
            {"customers": [{"name": "email_address", "type": "string"}]},
        )
        existing_metadata_arg = args[7]
        assert existing_metadata_arg is not live_metadata
        assert existing_metadata_arg["tables"][0] is not live_metadata["tables"][0]

    def test_live_session_metadata_is_unchanged_after_the_task_runs(
        self, monkeypatch
    ):
        live_metadata, _args = self._start(
            monkeypatch,
            [_table("customers", [{"name": "email", "type": "string"}])],
            {"customers": [{"name": "email_address", "type": "string"}]},
        )
        cols = {c["name"] for c in live_metadata["tables"][0]["columns"]}
        assert cols == {"email"}, (
            "the background refresh must not touch session state until the "
            "browser explicitly calls /domain/metadata/save"
        )
