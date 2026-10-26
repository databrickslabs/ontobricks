"""GET /settings/jobs lists Jobs that declare entity_id."""

from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient

from shared.fastapi.main import app


def test_settings_jobs_returns_filtered_list():
    jobs = [
        {"job_id": 1, "name": "enrich-customer"},
        {"job_id": 2, "name": "other-job"},
    ]
    with patch(
        "back.objects.domain.settings.RegistrySettings.RegistrySettings.fetch_entity_jobs",
        new_callable=AsyncMock,
        return_value={"success": True, "jobs": jobs},
    ) as mocked:
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/settings/jobs?q=enrich")
    assert resp.status_code == 200, resp.text
    mocked.assert_called_once()
    body = resp.json()
    assert body["success"] is True
    assert body["jobs"] == jobs


def test_fetch_entity_jobs_filters_by_name():
    from back.objects.domain.settings.RegistrySettings import RegistrySettings

    runner = MagicMock()
    runner.list_entity_jobs.return_value = [
        {"job_id": 1, "name": "enrich-customer"},
        {"job_id": 2, "name": "nightly-dq"},
    ]
    with patch(
        "back.objects.domain.settings.RegistrySettings.get_domain",
        return_value=MagicMock(),
    ), patch(
        "back.core.databricks.jobs.JobActionRunner.JobActionRunner.from_domain",
        return_value=runner,
    ):
        import asyncio

        payload = asyncio.run(
            RegistrySettings.fetch_entity_jobs(
                MagicMock(), MagicMock(), query="dq"
            )
        )
    assert [j["job_id"] for j in payload["jobs"]] == [2]
