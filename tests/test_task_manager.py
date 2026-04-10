"""Tests for back.core.task_manager — async task tracking."""
import pytest
from back.core.task_manager import TaskManager, TaskStatus, Task, TaskStep


@pytest.fixture(autouse=True)
def fresh_manager():
    """Reset the TaskManager singleton between tests."""
    TaskManager._instance = None
    TaskManager._lock.__init__()
    yield
    TaskManager._instance = None


@pytest.fixture
def mgr():
    return TaskManager()


class TestTaskStatus:
    def test_enum_values(self):
        assert TaskStatus.PENDING.value == "pending"
        assert TaskStatus.RUNNING.value == "running"
        assert TaskStatus.COMPLETED.value == "completed"
        assert TaskStatus.FAILED.value == "failed"
        assert TaskStatus.CANCELLED.value == "cancelled"


class TestTaskStep:
    def test_to_dict(self):
        step = TaskStep(name="Step1", description="First step")
        d = step.to_dict()
        assert d["name"] == "Step1"
        assert d["description"] == "First step"
        assert d["status"] == "pending"
        assert d["started_at"] is None


class TestTask:
    def test_to_dict(self):
        task = Task(id="abc", name="Test", task_type="unit")
        d = task.to_dict()
        assert d["id"] == "abc"
        assert d["status"] == "pending"
        assert d["progress"] == 0
        assert d["steps"] == []

    def test_to_dict_with_steps(self):
        step = TaskStep(name="s1", description="d1")
        task = Task(id="x", name="T", task_type="t", steps=[step])
        d = task.to_dict()
        assert len(d["steps"]) == 1
        assert d["steps"][0]["name"] == "s1"


class TestTaskManagerSingleton:
    def test_singleton(self):
        a = TaskManager()
        b = TaskManager()
        assert a is b


class TestCreateTask:
    def test_basic_create(self, mgr):
        task = mgr.create_task("Build", "build")
        assert task.name == "Build"
        assert task.task_type == "build"
        assert task.status == TaskStatus.PENDING
        assert len(task.id) == 8

    def test_create_with_steps(self, mgr):
        steps = [
            {"name": "Parse", "description": "Parse input"},
            {"name": "Generate", "description": "Generate output"},
        ]
        task = mgr.create_task("Pipeline", "pipeline", steps=steps)
        assert len(task.steps) == 2
        assert task.steps[0].name == "Parse"
        assert task.steps[1].status == "pending"


class TestTaskLifecycle:
    def test_start(self, mgr):
        task = mgr.create_task("T", "t")
        assert mgr.start_task(task.id, "Running...")
        assert task.status == TaskStatus.RUNNING
        assert task.started_at is not None
        assert task.message == "Running..."

    def test_start_with_steps(self, mgr):
        task = mgr.create_task("T", "t", steps=[{"name": "s1", "description": "d1"}])
        mgr.start_task(task.id)
        assert task.steps[0].status == "running"
        assert task.steps[0].started_at is not None

    def test_start_nonexistent(self, mgr):
        assert mgr.start_task("nope") is False

    def test_update_progress(self, mgr):
        task = mgr.create_task("T", "t")
        mgr.start_task(task.id)
        assert mgr.update_progress(task.id, 50, "Halfway")
        assert task.progress == 50
        assert task.message == "Halfway"

    def test_progress_clamped(self, mgr):
        task = mgr.create_task("T", "t")
        mgr.update_progress(task.id, 200)
        assert task.progress == 100
        mgr.update_progress(task.id, -10)
        assert task.progress == 0

    def test_update_progress_nonexistent(self, mgr):
        assert mgr.update_progress("nope", 50) is False

    def test_advance_step(self, mgr):
        steps = [
            {"name": "s1", "description": "d1"},
            {"name": "s2", "description": "d2"},
        ]
        task = mgr.create_task("T", "t", steps=steps)
        mgr.start_task(task.id)
        assert mgr.advance_step(task.id)
        assert task.steps[0].status == "completed"
        assert task.steps[1].status == "running"
        assert task.current_step == 1
        assert task.progress == 50

    def test_advance_step_no_steps(self, mgr):
        task = mgr.create_task("T", "t")
        assert mgr.advance_step(task.id) is False

    def test_advance_step_nonexistent(self, mgr):
        assert mgr.advance_step("nope") is False

    def test_complete(self, mgr):
        task = mgr.create_task("T", "t")
        mgr.start_task(task.id)
        assert mgr.complete_task(task.id, result={"ok": True}, message="Done")
        assert task.status == TaskStatus.COMPLETED
        assert task.progress == 100
        assert task.result == {"ok": True}
        assert task.completed_at is not None

    def test_complete_marks_remaining_steps(self, mgr):
        steps = [{"name": "s1", "description": "d1"}, {"name": "s2", "description": "d2"}]
        task = mgr.create_task("T", "t", steps=steps)
        mgr.start_task(task.id)
        mgr.complete_task(task.id)
        assert all(s.status == "completed" for s in task.steps)

    def test_complete_nonexistent(self, mgr):
        assert mgr.complete_task("nope") is False

    def test_fail(self, mgr):
        task = mgr.create_task("T", "t")
        mgr.start_task(task.id)
        assert mgr.fail_task(task.id, "Something broke")
        assert task.status == TaskStatus.FAILED
        assert task.error == "Something broke"
        assert "Failed:" in task.message

    def test_fail_marks_current_step(self, mgr):
        steps = [{"name": "s1", "description": "d1"}, {"name": "s2", "description": "d2"}]
        task = mgr.create_task("T", "t", steps=steps)
        mgr.start_task(task.id)
        mgr.advance_step(task.id)
        mgr.fail_task(task.id, "error")
        assert task.steps[1].status == "failed"

    def test_fail_nonexistent(self, mgr):
        assert mgr.fail_task("nope", "err") is False

    def test_cancel_pending(self, mgr):
        task = mgr.create_task("T", "t")
        assert mgr.cancel_task(task.id)
        assert task.status == TaskStatus.CANCELLED
        assert task.completed_at is not None

    def test_cancel_running(self, mgr):
        task = mgr.create_task("T", "t")
        mgr.start_task(task.id)
        assert mgr.cancel_task(task.id)
        assert task.status == TaskStatus.CANCELLED

    def test_cancel_completed_fails(self, mgr):
        task = mgr.create_task("T", "t")
        mgr.complete_task(task.id)
        assert mgr.cancel_task(task.id) is False

    def test_cancel_nonexistent(self, mgr):
        assert mgr.cancel_task("nope") is False


class TestQueryAndCleanup:
    def test_get_task(self, mgr):
        task = mgr.create_task("T", "t")
        assert mgr.get_task(task.id) is task
        assert mgr.get_task("unknown") is None

    def test_get_all_tasks(self, mgr):
        mgr.create_task("T1", "t")
        mgr.create_task("T2", "t")
        assert len(mgr.get_all_tasks()) == 2

    def test_get_all_tasks_exclude_completed(self, mgr):
        t1 = mgr.create_task("T1", "t")
        mgr.create_task("T2", "t")
        mgr.complete_task(t1.id)
        active = mgr.get_all_tasks(include_completed=False)
        assert len(active) == 1

    def test_get_active_tasks(self, mgr):
        t1 = mgr.create_task("T1", "t")
        t2 = mgr.create_task("T2", "t")
        mgr.complete_task(t1.id)
        assert len(mgr.get_active_tasks()) == 1

    def test_delete_task(self, mgr):
        task = mgr.create_task("T", "t")
        assert mgr.delete_task(task.id)
        assert mgr.get_task(task.id) is None
        assert mgr.delete_task("nope") is False

    def test_clear_completed(self, mgr):
        t1 = mgr.create_task("T1", "t")
        t2 = mgr.create_task("T2", "t")
        t3 = mgr.create_task("T3", "t")
        mgr.complete_task(t1.id)
        mgr.fail_task(t2.id, "err")
        removed = mgr.clear_completed()
        assert removed == 2
        assert len(mgr.get_all_tasks()) == 1

    def test_cleanup_old_tasks(self, mgr):
        mgr._max_tasks = 5
        tasks = [mgr.create_task(f"T{i}", "t") for i in range(4)]
        for t in tasks:
            mgr.complete_task(t.id)
        mgr.create_task("T4", "t")
        mgr.create_task("T5", "t")
        mgr.create_task("T6", "t")
        assert len(mgr.get_all_tasks()) <= 5
