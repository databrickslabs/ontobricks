"""Navbar Background Tasks know entity_job_action."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
TRACKER = REPO_ROOT / "src/front/static/global/js/task-tracker.js"


def test_task_type_urls_include_entity_job_action():
    js = TRACKER.read_text(encoding="utf-8")
    assert "'entity_job_action': '/dtwin'" in js
    assert "run_page_url" in js
    assert "Open run" in js
    assert "notifyTaskTransition" in js
