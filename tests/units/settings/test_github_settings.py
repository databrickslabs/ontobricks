"""Tests for Settings → GitHub (GitHubSettings)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import ValidationError
from back.objects.domain.settings.GitHubSettings import GitHubSettings

_SS = "back.objects.domain.settings.SettingsService"


@pytest.fixture
def gcs():
    store = {"github_config": {}}
    svc = MagicMock()
    svc.get_github_config.side_effect = lambda *a: dict(store["github_config"])

    def _set(host, token, registry_cfg, cfg):
        store["github_config"] = cfg
        return True, "ok"

    svc.set_github_config.side_effect = _set
    with patch(f"{_SS}.global_config_service", svc), patch(
        f"{_SS}.SettingsService._resolve_context",
        return_value=(MagicMock(), "h", "t", {"catalog": "c", "schema": "s"}),
    ):
        yield store


def test_save_assigns_stable_instance_uuid(gcs):
    out = GitHubSettings.save_github_result(
        {"enabled": True, "owner": "acme", "repo": "onto"}, MagicMock(), MagicMock()
    )
    uuid1 = out["github"]["instance_uuid"]
    assert len(uuid1) == 32
    out = GitHubSettings.save_github_result({"branch": "dev"}, MagicMock(), MagicMock())
    assert out["github"]["instance_uuid"] == uuid1
    assert out["github"]["branch"] == "dev"
    assert out["github"]["owner"] == "acme"


@pytest.mark.parametrize(
    "payload",
    [
        {"enabled": True, "owner": "", "repo": "onto"},
        {"owner": "acme org", "repo": "onto"},
        {"owner": "acme", "repo": "onto", "base_path": "a/../b"},
    ],
)
def test_save_rejects_invalid_settings(gcs, payload):
    with pytest.raises(ValidationError):
        GitHubSettings.save_github_result(payload, MagicMock(), MagicMock())


def test_obx_result_lists_repo_without_reading_blobs(gcs):
    sync = MagicMock()
    sync.list_domain_versions.return_value = {"Sales": ["2", "1"]}
    cfg = MagicMock()
    cfg.label = "acme/onto@main"
    fake_svc = MagicMock()
    fake_svc.cfg.is_configured = True
    fake_svc.list_domain_details_cached.return_value = (
        True,
        [{"name": "Sales", "versions": [{"version": "1"}]}],
        "",
    )
    with patch.object(GitHubSettings, "_load", return_value=cfg), patch.object(
        GitHubSettings, "_require_ready"
    ), patch(
        "back.objects.domain.settings.GitHubSettings.GitHubSyncService",
        return_value=sync,
    ), patch(
        f"{_SS}.RegistryService.from_context", return_value=fake_svc
    ):
        out = GitHubSettings.github_obx_envelope_result(
            "me@x", MagicMock(), MagicMock()
        )
    sync.build_obx_envelope.assert_not_called()
    (row,) = out["domains"]
    assert row["name"] == "Sales"
    assert row["incoming_versions"] == ["2", "1"]
    assert row["exists"] is True
    assert row["conflicting_versions"] == ["1"]
    assert row["suggested_new_name"] == "SalesImported"


def test_obx_result_rejects_empty_repo(gcs):
    sync = MagicMock()
    sync.list_domain_versions.return_value = {}
    with patch.object(GitHubSettings, "_load", return_value=MagicMock()), patch.object(
        GitHubSettings, "_require_ready"
    ), patch(
        "back.objects.domain.settings.GitHubSettings.GitHubSyncService",
        return_value=sync,
    ):
        with pytest.raises(ValidationError, match="No domains found"):
            GitHubSettings.github_obx_envelope_result(
                "empty@x", MagicMock(), MagicMock()
            )


def test_delete_mirrored_domain_skips_when_github_is_off(gcs):
    GitHubSettings.delete_mirrored_domain("Sales", MagicMock(), MagicMock())


def test_delete_mirrored_domain_commits_the_folder(gcs):
    sync = MagicMock()
    sync.delete_domain.return_value = {"commit_sha": "abc", "errors": []}
    cfg = MagicMock()
    cfg.is_ready = True
    with patch.object(GitHubSettings, "_load", return_value=cfg), patch.object(
        GitHubSettings, "_require_ready"
    ), patch(
        "back.objects.domain.settings.GitHubSettings.GitHubSyncService",
        return_value=sync,
    ), patch(
        "back.objects.domain.settings.GitHubSettings.github_mirror"
    ):
        GitHubSettings.delete_mirrored_domain(
            "Sales", MagicMock(), MagicMock(), actor="me@x"
        )
    sync.delete_domain.assert_called_once_with("Sales", actor="me@x")
    sync.close.assert_called_once()


def test_token_is_never_part_of_the_config(gcs):
    out = GitHubSettings.save_github_result(
        {"owner": "acme", "repo": "onto", "token": "ghp_secret"}, MagicMock(), MagicMock()
    )
    assert "token" not in out["github"]
    assert "ghp_secret" not in str(gcs["github_config"])
