"""Tests for Databricks auth utilities and DatabricksAuth."""

import time
import pytest
from unittest.mock import MagicMock, patch

from back.core.errors import ValidationError
from shared.config.constants import HTTP_USER_AGENT
from back.core.databricks import (
    DatabricksAuth,
    get_workspace_host,
    is_databricks_app,
    normalize_host,
)


def _clear_databricks_env(monkeypatch):
    """Remove Databricks-related env vars for isolated tests."""
    for key in (
        "DATABRICKS_APP_PORT",
        "DATABRICKS_TOKEN",
        "DATABRICKS_HOST",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
        "DATABRICKS_SQL_WAREHOUSE_ID",
        "DATABRICKS_CONFIG_PROFILE",
        "DATABRICKS_DISABLE_CLOUD_FETCH",
        "DATABRICKS_FORCE_CLOUD_FETCH",
    ):
        monkeypatch.delenv(key, raising=False)


class TestIsDatabricksApp:
    def test_false_when_port_not_set(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        assert is_databricks_app() is False

    def test_true_when_port_set(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_APP_PORT", "8080")
        assert is_databricks_app() is True


class TestNormalizeHost:
    def test_empty_string(self):
        assert normalize_host("") == ""

    def test_adds_https_when_no_scheme(self):
        assert normalize_host("test.databricks.com") == "https://test.databricks.com"

    def test_strips_trailing_slash(self):
        assert (
            normalize_host("https://test.databricks.com/")
            == "https://test.databricks.com"
        )

    def test_preserves_https(self):
        assert normalize_host("https://already.good.com") == "https://already.good.com"

    def test_preserves_http(self):
        assert normalize_host("http://local.test/") == "http://local.test"

    def test_strips_whitespace(self):
        assert normalize_host("  my.workspace.com  ") == "https://my.workspace.com"


class TestGetWorkspaceHost:
    def test_reads_from_databricks_host_and_normalizes(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_HOST", "https://env.databricks.com/")
        assert get_workspace_host() == "https://env.databricks.com"

    @patch("databricks.sdk.WorkspaceClient")
    def test_sdk_fallback_when_host_empty(self, mock_wc, monkeypatch):
        _clear_databricks_env(monkeypatch)
        mock_instance = MagicMock()
        mock_instance.config.host = "https://sdk.databricks.com/"
        mock_wc.return_value = mock_instance
        assert get_workspace_host() == "https://sdk.databricks.com"


class TestDatabricksAuthInit:
    def test_explicit_host_token_warehouse(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        auth = DatabricksAuth(
            host="https://explicit.databricks.com",
            token="explicit-token",
            warehouse_id="wh-123",
        )
        assert auth.host == "https://explicit.databricks.com"
        assert auth.token == "explicit-token"
        assert auth.warehouse_id == "wh-123"
        assert auth.is_app_mode is False

    def test_auto_detect_from_env(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_HOST", "https://from-env.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "env-token")
        auth = DatabricksAuth()
        assert auth.host == "https://from-env.databricks.com"
        assert auth.token == "env-token"
        assert auth.warehouse_id == ""


class TestGetOauthToken:
    @patch("requests.post")
    def test_calls_oidc_endpoint_and_returns_token(self, mock_post, monkeypatch):
        _clear_databricks_env(monkeypatch)
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "oauth-access"}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        auth = DatabricksAuth(
            host="https://ws.databricks.com",
            token="",
        )
        auth.client_id = "cid"
        auth.client_secret = "csec"

        token = auth.get_oauth_token()
        assert token == "oauth-access"
        mock_post.assert_called_once()
        call_kw = mock_post.call_args
        assert call_kw[0][0] == "https://ws.databricks.com/oidc/v1/token"
        assert call_kw[1]["data"] == {
            "grant_type": "client_credentials",
            "scope": "all-apis",
        }
        assert call_kw[1]["auth"] == ("cid", "csec")

    def test_caches_token_within_ttl(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        auth = DatabricksAuth(host="https://ws.databricks.com", token="")
        auth.client_id = "cid"
        auth.client_secret = "csec"
        auth._oauth_token = "cached-token"
        auth._oauth_token_ts = time.time()

        assert auth.get_oauth_token() == "cached-token"

    def test_raises_when_host_missing(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        auth = DatabricksAuth(host="https://ws.databricks.com", token="")
        auth.host = ""
        auth.client_id = "cid"
        auth.client_secret = "csec"

        with pytest.raises(ValidationError, match="DATABRICKS_HOST is not configured"):
            auth.get_oauth_token()


class TestGetAuthHeaders:
    def test_pat_mode_bearer_headers(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        auth = DatabricksAuth(
            host="https://ws.databricks.com",
            token="my-pat",
        )
        headers = auth.get_auth_headers()
        assert headers == {
            "Authorization": "Bearer my-pat",
            "Content-Type": "application/json",
            "User-Agent": HTTP_USER_AGENT,
        }

    @patch.object(DatabricksAuth, "get_oauth_token", return_value="oauth-header-token")
    def test_app_mode_oauth_headers(self, mock_oauth, monkeypatch):
        _clear_databricks_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_APP_PORT", "8080")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")

        auth = DatabricksAuth(host="https://ws.databricks.com", token="")
        assert auth.is_app_mode is True

        headers = auth.get_auth_headers()
        assert headers == {
            "Authorization": "Bearer oauth-header-token",
            "Content-Type": "application/json",
            "User-Agent": HTTP_USER_AGENT,
        }
        mock_oauth.assert_called()

    def test_no_auth_empty_dict(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        auth = DatabricksAuth(host="https://ws.databricks.com", token="")
        assert auth.get_auth_headers() == {"User-Agent": HTTP_USER_AGENT}


class TestGetSqlConnectionParams:
    @patch.object(DatabricksAuth, "can_use_cloud_fetch", return_value=False)
    def test_params_with_pat(self, _mock_cf, monkeypatch):
        _clear_databricks_env(monkeypatch)
        auth = DatabricksAuth(
            host="https://ws.databricks.com",
            token="sql-pat",
            warehouse_id="wh-99",
        )
        params = auth.get_sql_connection_params()
        assert params["server_hostname"] == "ws.databricks.com"
        assert params["http_path"] == "/sql/1.0/warehouses/wh-99"
        assert params["access_token"] == "sql-pat"
        assert params["_socket_timeout"] == 30
        assert params["use_cloud_fetch"] is False

    @patch.object(DatabricksAuth, "can_use_cloud_fetch", return_value=True)
    @patch.object(DatabricksAuth, "get_oauth_token", return_value="sql-oauth")
    def test_params_with_oauth_in_app_mode(self, mock_oauth, _mock_cf, monkeypatch):
        _clear_databricks_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_APP_PORT", "8080")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")

        auth = DatabricksAuth(
            host="https://ws.databricks.com",
            token="",
            warehouse_id="wh-oauth",
        )
        params = auth.get_sql_connection_params()
        assert params["access_token"] == "sql-oauth"
        assert params["use_cloud_fetch"] is True
        mock_oauth.assert_called()


class TestCloudFetchCapability:
    @staticmethod
    def _reset_cache():
        DatabricksAuth._cloud_fetch_cache.clear()

    def test_cloud_fetch_disabled_when_pyarrow_missing(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        self._reset_cache()
        auth = DatabricksAuth(
            host="https://ws.cloud.databricks.com",
            token="sql-pat",
            warehouse_id="wh-1",
        )
        with patch.dict("sys.modules", {"pyarrow": None}):
            assert auth.can_use_cloud_fetch() is False
            ok, reason = auth.probe_cloud_fetch_capability()
            assert ok is False
            assert "pyarrow" in reason.lower()

    @patch("databricks.sql.connect")
    def test_probe_cloud_fetch_capability_success(self, mock_connect, monkeypatch):
        _clear_databricks_env(monkeypatch)
        self._reset_cache()
        auth = DatabricksAuth(
            host="https://ws.cloud.databricks.com",
            token="sql-pat",
            warehouse_id="wh-1",
        )

        conn_cm = MagicMock()
        cur_cm = MagicMock()
        mock_connect.return_value.__enter__.return_value = conn_cm
        conn_cm.cursor.return_value.__enter__.return_value = cur_cm
        cur_cm.fetchall.return_value = [(1,)]

        ok, reason = auth.probe_cloud_fetch_capability()
        assert ok is True
        assert "succeeded" in reason.lower()
        kwargs = mock_connect.call_args.kwargs
        assert kwargs["use_cloud_fetch"] is True
        assert kwargs["server_hostname"] == "ws.cloud.databricks.com"

        # Cached result drives can_use_cloud_fetch with no extra connect.
        mock_connect.reset_mock()
        assert auth.can_use_cloud_fetch() is True
        mock_connect.assert_not_called()

    @patch("databricks.sql.connect", side_effect=RuntimeError("blocked"))
    def test_probe_cloud_fetch_capability_failure(self, _mock_connect, monkeypatch):
        _clear_databricks_env(monkeypatch)
        self._reset_cache()
        auth = DatabricksAuth(
            host="https://ws.cloud.databricks.com",
            token="sql-pat",
            warehouse_id="wh-1",
        )
        ok, reason = auth.probe_cloud_fetch_capability()
        assert ok is False
        assert "blocked" in reason

        assert auth.can_use_cloud_fetch() is False

    def test_can_use_cloud_fetch_default_app_mode_enabled(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        self._reset_cache()
        monkeypatch.setenv("DATABRICKS_APP_PORT", "8080")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")
        auth = DatabricksAuth(
            host="https://ws.cloud.databricks.com",
            token="",
            warehouse_id="wh-1",
        )
        assert auth.can_use_cloud_fetch() is True

    def test_can_use_cloud_fetch_disabled_by_global_setting(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        self._reset_cache()
        auth = DatabricksAuth(
            host="https://ws.cloud.databricks.com",
            token="sql-pat",
            warehouse_id="wh-1",
            use_cloud_fetch=False,
        )
        assert auth.can_use_cloud_fetch() is False

    def test_env_force_overrides(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        self._reset_cache()
        monkeypatch.setenv("DATABRICKS_FORCE_CLOUD_FETCH", "1")
        auth = DatabricksAuth(
            host="https://ws.cloud.databricks.com",
            token="sql-pat",
            warehouse_id="wh-1",
        )
        assert auth.can_use_cloud_fetch() is True

    def test_env_disable_overrides(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        self._reset_cache()
        monkeypatch.setenv("DATABRICKS_DISABLE_CLOUD_FETCH", "1")
        auth = DatabricksAuth(
            host="https://ws.cloud.databricks.com",
            token="sql-pat",
            warehouse_id="wh-1",
        )
        assert auth.can_use_cloud_fetch() is False


class TestHasValidAuth:
    def test_true_with_token_non_app(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        auth = DatabricksAuth(host="https://ws.databricks.com", token="tok")
        assert auth.has_valid_auth() is True

    def test_true_with_client_id_and_secret_in_app_mode(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_APP_PORT", "8080")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")
        auth = DatabricksAuth(host="https://ws.databricks.com", token="")
        assert auth.has_valid_auth() is True

    def test_false_without_credentials(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        auth = DatabricksAuth(host="https://ws.databricks.com", token="")
        assert auth.has_valid_auth() is False


class TestGetBearerToken:
    def test_returns_instance_pat_first(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        auth = DatabricksAuth(
            host="https://ws.databricks.com",
            token="instance-pat",
        )
        monkeypatch.setenv("DATABRICKS_TOKEN", "env-should-not-win")
        assert auth.get_bearer_token() == "instance-pat"

    def test_returns_env_pat_when_instance_token_cleared(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        auth = DatabricksAuth(host="https://ws.databricks.com", token="")
        monkeypatch.setenv("DATABRICKS_TOKEN", "only-env-pat")
        assert auth.get_bearer_token() == "only-env-pat"

    @patch.object(DatabricksAuth, "get_oauth_token", return_value="m2m-token")
    def test_returns_oauth_in_app_mode_when_no_pat(self, mock_oauth, monkeypatch):
        _clear_databricks_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_APP_PORT", "8080")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")

        auth = DatabricksAuth(host="https://ws.databricks.com", token="")
        assert auth.get_bearer_token() == "m2m-token"
        mock_oauth.assert_called_once()

    def test_returns_empty_when_no_auth(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        auth = DatabricksAuth(host="https://ws.databricks.com", token="")
        assert auth.get_bearer_token() == ""


class TestCliMode:
    """Databricks CLI profile auth (~/.databrickscfg via SDK Config)."""

    def _make_cli_auth(self, monkeypatch, profile=""):
        """Build a DatabricksAuth with a stubbed CLI Config."""
        _clear_databricks_env(monkeypatch)
        if profile:
            monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", profile)
        fake_cfg = MagicMock()
        fake_cfg.host = "https://cli.databricks.com"
        fake_cfg.authenticate.return_value = {
            "Authorization": "Bearer cli-token-xyz"
        }
        with patch.object(
            DatabricksAuth, "_resolve_cli_config", return_value=fake_cfg
        ):
            auth = DatabricksAuth(warehouse_id="wh-cli")
        auth._cli_config = fake_cfg
        return auth, fake_cfg

    def test_auth_mode_is_cli_when_only_profile_resolves(self, monkeypatch):
        auth, _ = self._make_cli_auth(monkeypatch)
        assert auth.auth_mode == "cli"
        assert auth.is_app_mode is False
        assert auth.token == ""

    def test_host_falls_back_to_cli_config_host(self, monkeypatch):
        auth, _ = self._make_cli_auth(monkeypatch)
        assert auth.host == "https://cli.databricks.com"

    def test_has_valid_auth_true_in_cli_mode(self, monkeypatch):
        auth, _ = self._make_cli_auth(monkeypatch)
        assert auth.has_valid_auth() is True

    def test_get_auth_headers_uses_sdk_config(self, monkeypatch):
        auth, fake_cfg = self._make_cli_auth(monkeypatch)
        headers = auth.get_auth_headers()
        assert headers["Authorization"] == "Bearer cli-token-xyz"
        assert headers["Content-Type"] == "application/json"
        assert headers["User-Agent"] == HTTP_USER_AGENT
        fake_cfg.authenticate.assert_called()

    def test_get_bearer_token_extracts_token_from_headers(self, monkeypatch):
        auth, _ = self._make_cli_auth(monkeypatch)
        assert auth.get_bearer_token() == "cli-token-xyz"

    @patch.object(DatabricksAuth, "can_use_cloud_fetch", return_value=False)
    def test_sql_params_use_credentials_provider(self, _cf, monkeypatch):
        auth, fake_cfg = self._make_cli_auth(monkeypatch)
        params = auth.get_sql_connection_params()
        assert "access_token" not in params
        assert "credentials_provider" in params
        provider = params["credentials_provider"]
        inner = provider()
        assert inner is fake_cfg.authenticate
        assert inner() == {"Authorization": "Bearer cli-token-xyz"}

    def test_cli_profile_name_defaults_to_default(self, monkeypatch):
        auth, _ = self._make_cli_auth(monkeypatch)
        assert auth.cli_profile_name == "default"

    def test_cli_profile_name_reflects_env_var(self, monkeypatch):
        auth, _ = self._make_cli_auth(monkeypatch, profile="sandbox")
        assert auth.cli_profile_name == "sandbox"

    def test_pat_takes_precedence_over_cli(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_TOKEN", "pat-wins")
        with patch.object(
            DatabricksAuth, "_resolve_cli_config", return_value=MagicMock()
        ) as mock_resolve:
            auth = DatabricksAuth(host="https://ws.databricks.com")
        mock_resolve.assert_not_called()
        assert auth.auth_mode == "pat"
        assert auth._cli_config is None

    def test_app_mode_takes_precedence_over_cli(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_APP_PORT", "8080")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")
        with patch.object(
            DatabricksAuth, "_resolve_cli_config", return_value=MagicMock()
        ) as mock_resolve:
            auth = DatabricksAuth(host="https://ws.databricks.com", token="")
        mock_resolve.assert_not_called()
        assert auth.auth_mode == "app"
        assert auth._cli_config is None

    @pytest.mark.allow_cli_resolve
    def test_resolve_cli_config_returns_none_when_sdk_raises(self, monkeypatch):
        _clear_databricks_env(monkeypatch)
        with patch(
            "databricks.sdk.core.Config",
            side_effect=ValueError("no creds"),
        ):
            cfg = DatabricksAuth._resolve_cli_config("", "")
        assert cfg is None
