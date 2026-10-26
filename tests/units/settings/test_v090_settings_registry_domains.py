"""RegistryDomainSettings (0.9.0): domain listing, bridges, domain/version
deletion, target-domain role resolution, and lifecycle status transitions."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from back.core.errors import InfrastructureError, NotFoundError, ValidationError
from back.objects.domain.settings.GitHubSettings import GitHubSettings
from back.objects.domain.settings.RegistryDomainSettings import RegistryDomainSettings

pytestmark = pytest.mark.unit

_ss = importlib.import_module("back.objects.domain.settings.SettingsService")


@pytest.fixture
def domain():
    d = MagicMock()
    d.domain_folder = "claims"
    d.current_version = "2"
    d.info = {"status": "DRAFT"}
    return d


@pytest.fixture
def svc(monkeypatch, domain):
    registry = MagicMock()
    registry.cfg.is_configured = True
    rs_cls = MagicMock()
    rs_cls.from_context.return_value = registry
    rs_cls._version_sort_key = lambda v: int(v)
    rs_cls.version_document_has_ontology.return_value = True
    monkeypatch.setattr(_ss, "RegistryService", rs_cls)
    monkeypatch.setattr(_ss, "get_domain", lambda sm: domain)
    monkeypatch.setattr(_ss, "invalidate_registry_cache", MagicMock())
    monkeypatch.setattr(_ss, "clear_version_status_cache", MagicMock())
    return registry


# ---------------------------------------------------------------------------
# list_registry_domains_result
# ---------------------------------------------------------------------------


class TestListRegistryDomains:
    def test_unconfigured_registry_raises_validation(self, svc):
        svc.cfg.is_configured = False
        with pytest.raises(ValidationError, match="Registry not configured"):
            RegistryDomainSettings.list_registry_domains_result(MagicMock(), MagicMock())

    def test_listing_failure_raises_infrastructure(self, svc):
        svc.list_domain_details_cached.return_value = (False, [], "lakebase down")
        with pytest.raises(InfrastructureError) as exc_info:
            RegistryDomainSettings.list_registry_domains_result(MagicMock(), MagicMock())
        assert exc_info.value.detail == "lakebase down"

    def test_unexpected_error_is_wrapped(self, svc):
        svc.list_domain_details_cached.side_effect = RuntimeError("boom")
        with pytest.raises(InfrastructureError, match="Failed to list registry domains"):
            RegistryDomainSettings.list_registry_domains_result(MagicMock(), MagicMock())

    def test_annotates_versions_without_mutating_cache(self, svc, monkeypatch):
        cached = [
            {
                "name": "claims",
                "versions": [{"version": "2", "status": "PUBLISHED"}, {"version": "1"}],
            },
            {"name": "empty", "versions": []},
        ]
        svc.list_domain_details_cached.return_value = (True, cached, "")
        calls = []

        def capability(**kw):
            calls.append(kw)
            return {"can_delete": not kw["is_loaded"]}

        monkeypatch.setattr(_ss, "version_deletion_capability", capability)

        result = RegistryDomainSettings.list_registry_domains_result(
            MagicMock(), MagicMock(), user_role="admin"
        )

        v2, v1 = result["domains"][0]["versions"]
        assert v2 == {"version": "2", "status": "PUBLISHED", "can_delete": False}
        assert v1 == {"version": "1", "can_delete": True}
        assert calls == [
            dict(user_role="admin", status="PUBLISHED", is_loaded=True, is_latest=True, version_count=2),
            dict(user_role="admin", status="DRAFT", is_loaded=False, is_latest=False, version_count=2),
        ]
        assert cached[0]["versions"][0] == {"version": "2", "status": "PUBLISHED"}
        assert result["github_enabled"] is False

    def test_exposes_github_enabled_when_mirror_is_ready(self, svc):
        svc.list_domain_details_cached.return_value = (True, [], "")
        svc.store.load_global_config.return_value = {
            "github_config": {"enabled": True, "owner": "acme", "repo": "onto"}
        }
        result = RegistryDomainSettings.list_registry_domains_result(
            MagicMock(), MagicMock()
        )
        assert result["github_enabled"] is True


# ---------------------------------------------------------------------------
# list_registry_bridges_result
# ---------------------------------------------------------------------------


class TestListRegistryBridges:
    def test_returns_bridges_per_domain(self, svc):
        svc.list_all_bridges.return_value = (True, [{"name": "claims", "bridges": []}], "")
        assert RegistryDomainSettings.list_registry_bridges_result(MagicMock(), MagicMock()) == {
            "success": True,
            "domains": [{"name": "claims", "bridges": []}],
        }

    def test_unconfigured_registry_raises_validation(self, svc):
        svc.cfg.is_configured = False
        with pytest.raises(ValidationError):
            RegistryDomainSettings.list_registry_bridges_result(MagicMock(), MagicMock())

    def test_listing_failure_raises_infrastructure(self, svc):
        svc.list_all_bridges.return_value = (False, None, "bad")
        with pytest.raises(InfrastructureError, match="bridges"):
            RegistryDomainSettings.list_registry_bridges_result(MagicMock(), MagicMock())

    def test_unexpected_error_is_wrapped(self, svc):
        svc.list_all_bridges.side_effect = RuntimeError("x")
        with pytest.raises(InfrastructureError, match="Failed to list registry bridges"):
            RegistryDomainSettings.list_registry_bridges_result(MagicMock(), MagicMock())


# ---------------------------------------------------------------------------
# delete_registry_domain_result
# ---------------------------------------------------------------------------


class TestDeleteRegistryDomain:
    def test_success_message_names_domain(self, svc):
        svc.delete_domain.return_value = []
        result = RegistryDomainSettings.delete_registry_domain_result(
            "claims", MagicMock(), MagicMock()
        )
        assert result == {"success": True, "message": 'Domain "claims" deleted from registry'}
        svc.delete_domain.assert_called_once_with("claims")

    def test_opt_in_github_delete_runs_after_registry_delete(self, svc, monkeypatch):
        svc.delete_domain.return_value = []
        called = []

        def _gh(folder, session_mgr, settings, *, actor=""):
            called.append((folder, actor))

        monkeypatch.setattr(GitHubSettings, "delete_mirrored_domain", _gh)
        RegistryDomainSettings.delete_registry_domain_result(
            "claims", MagicMock(), MagicMock(), delete_github=True
        )
        assert called[0][0] == "claims"

    def test_skips_github_delete_unless_requested(self, svc, monkeypatch):
        svc.delete_domain.return_value = []
        gh = MagicMock()
        monkeypatch.setattr(GitHubSettings, "delete_mirrored_domain", gh)
        RegistryDomainSettings.delete_registry_domain_result(
            "claims", MagicMock(), MagicMock()
        )
        gh.assert_not_called()

    def test_partial_delete_joins_errors_in_detail(self, svc):
        svc.delete_domain.return_value = ["volume busy", "lakebase row locked"]
        with pytest.raises(InfrastructureError, match="partially deleted") as exc_info:
            RegistryDomainSettings.delete_registry_domain_result("claims", MagicMock(), MagicMock())
        assert exc_info.value.detail == "volume busy; lakebase row locked"

    def test_unconfigured_registry_raises_validation(self, svc):
        svc.cfg.is_configured = False
        with pytest.raises(ValidationError):
            RegistryDomainSettings.delete_registry_domain_result("claims", MagicMock(), MagicMock())

    def test_unexpected_error_is_wrapped(self, svc):
        svc.delete_domain.side_effect = RuntimeError("x")
        with pytest.raises(InfrastructureError, match="Delete registry domain failed"):
            RegistryDomainSettings.delete_registry_domain_result("claims", MagicMock(), MagicMock())


# ---------------------------------------------------------------------------
# delete_registry_version_result
# ---------------------------------------------------------------------------


class TestDeleteRegistryVersion:
    def _call(self, version="1"):
        return RegistryDomainSettings.delete_registry_version_result(
            "claims", version, user_role="admin", session_mgr=MagicMock(), settings=MagicMock()
        )

    def test_unconfigured_registry_raises_validation(self, svc):
        svc.cfg.is_configured = False
        with pytest.raises(ValidationError):
            self._call()

    def test_unexpected_error_is_wrapped(self, svc):
        svc.list_versions.side_effect = RuntimeError("x")
        with pytest.raises(InfrastructureError, match="Delete registry version failed"):
            self._call()

    def test_unknown_version_raises_not_found(self, svc):
        svc.list_versions.return_value = (True, ["1", "2"], "")
        with pytest.raises(NotFoundError):
            self._call("9")

    def test_last_version_opt_in_github_delete(self, svc, monkeypatch):
        svc.list_versions.return_value = (True, ["1"], "")
        svc.read_version.return_value = (True, {"info": {"status": "DRAFT"}}, "")
        svc.delete_version.return_value = (True, "")
        svc.delete_domain.return_value = []
        gh = MagicMock()
        monkeypatch.setattr(GitHubSettings, "delete_mirrored_domain", gh)
        monkeypatch.setattr(_ss, "check_version_deletion", lambda **_k: None)
        result = RegistryDomainSettings.delete_registry_version_result(
            "claims",
            "1",
            user_role="admin",
            session_mgr=MagicMock(),
            settings=MagicMock(),
            delete_github=True,
        )
        assert result["domain_deleted"] is True
        gh.assert_called_once()
        assert gh.call_args.args[0] == "claims"


# ---------------------------------------------------------------------------
# resolve_domain_role
# ---------------------------------------------------------------------------


class TestResolveDomainRole:
    @pytest.fixture
    def wiring(self, monkeypatch):
        import back.core.helpers as helpers

        monkeypatch.setattr(_ss, "get_domain", lambda sm: MagicMock())
        monkeypatch.setattr(_ss, "SessionManager", lambda req: MagicMock())
        monkeypatch.setattr(helpers, "get_databricks_host_and_token", lambda d, s: ("h", "t"))
        rcfg = MagicMock()
        rcfg.from_domain.return_value.as_dict.return_value = {"catalog": "c"}
        monkeypatch.setattr(_ss, "RegistryCfg", rcfg)
        perm = MagicMock()
        perm.get_domain_role.return_value = "builder"
        monkeypatch.setattr(_ss, "permission_service", perm)
        return perm

    def _request(self, state_email="", headers=None):
        return SimpleNamespace(
            state=SimpleNamespace(user_email=state_email), headers=headers or {}
        )

    def test_resolves_role_for_target_domain(self, wiring):
        req = self._request("bob@x", {"x-forwarded-access-token": "utok"})
        settings = SimpleNamespace(ontobricks_app_name="app")

        role = RegistryDomainSettings.resolve_domain_role(
            req, "other_domain", settings, app_role="editor"
        )

        assert role == "builder"
        wiring.get_domain_role.assert_called_once_with(
            "bob@x", "h", "t", {"catalog": "c"}, "app", "other_domain",
            user_token="utok", app_role="editor",
        )

    def test_falls_back_to_forwarded_email_header(self, wiring):
        req = self._request("", {"x-forwarded-email": "hdr@x"})
        RegistryDomainSettings.resolve_domain_role(
            req, "d", SimpleNamespace(ontobricks_app_name="app")
        )
        assert wiring.get_domain_role.call_args.args[0] == "hdr@x"

    def test_any_failure_returns_empty_role(self, wiring):
        wiring.get_domain_role.side_effect = RuntimeError("scim down")
        assert RegistryDomainSettings.resolve_domain_role(
            self._request("a@x"), "d", SimpleNamespace(ontobricks_app_name="app")
        ) == ""


# ---------------------------------------------------------------------------
# set_registry_version_status_result
# ---------------------------------------------------------------------------


class TestSetRegistryVersionStatus:
    @pytest.fixture(autouse=True)
    def _transition(self, monkeypatch):
        self.check = MagicMock()
        monkeypatch.setattr(_ss, "check_status_transition", self.check)

    def _call(self, version="2", new_status=" in-review ", actor=""):
        return RegistryDomainSettings.set_registry_version_status_result(
            "claims",
            version,
            new_status,
            user_role="builder",
            user_domain_role="builder",
            actor_email=actor,
            session_mgr=MagicMock(),
            settings=MagicMock(),
        )

    def _ready(self, svc, status="DRAFT"):
        svc.list_versions_sorted.return_value = ["2", "1"]
        svc.read_version.return_value = (
            True,
            {"info": {"status": status, "last_build": "2026-10-01"}},
            "",
        )
        svc.set_version_status.return_value = (True, "ok")

    def test_unconfigured_registry_raises_validation(self, svc):
        svc.cfg.is_configured = False
        with pytest.raises(ValidationError):
            self._call()

    def test_unknown_version_raises_not_found(self, svc):
        svc.list_versions_sorted.return_value = ["1"]
        with pytest.raises(NotFoundError):
            self._call("7")

    def test_read_failure_raises_infrastructure(self, svc):
        svc.list_versions_sorted.return_value = ["2"]
        svc.read_version.return_value = (False, {}, "missing blob")
        with pytest.raises(InfrastructureError, match="Failed to read registry version"):
            self._call()

    def test_set_status_failure_raises_infrastructure(self, svc):
        self._ready(svc)
        svc.set_version_status.return_value = (False, "conflict")
        with pytest.raises(InfrastructureError, match="Failed to update version status"):
            self._call()

    def test_success_normalizes_status_and_updates_loaded_domain(self, svc, domain):
        self._ready(svc)

        result = self._call(actor="me@x")

        assert result == {
            "success": True,
            "version": "2",
            "status": "IN-REVIEW",
            "previous_status": "DRAFT",
        }
        self.check.assert_called_once_with(
            "DRAFT",
            "IN-REVIEW",
            user_role="builder",
            user_domain_role="builder",
            last_build="2026-10-01",
            has_ontology=True,
        )
        assert svc.record_review_event.call_args.args[:4] == ("claims", "2", "me@x", "submitted")
        assert domain.info["status"] == "IN-REVIEW"
        domain.save.assert_called_once()
        _ss.invalidate_registry_cache.assert_called_once()
        _ss.clear_version_status_cache.assert_called_once()

    def test_audit_write_failure_does_not_roll_back(self, svc, domain):
        self._ready(svc, status="IN-REVIEW")
        svc.record_review_event.side_effect = RuntimeError("audit table missing")

        result = self._call(version="1", new_status="published")

        assert result["status"] == "PUBLISHED"
        svc.set_version_status.assert_called_once_with("claims", "1", "PUBLISHED")
        domain.save.assert_not_called()

    def test_unexpected_error_is_wrapped(self, svc):
        svc.list_versions_sorted.side_effect = RuntimeError("x")
        with pytest.raises(InfrastructureError, match="Set registry version status failed"):
            self._call()
