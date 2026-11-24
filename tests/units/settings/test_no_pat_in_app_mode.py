"""POST /settings/save must never accept caller credentials in Databricks Apps mode.

Marketplace SOP (Phase 1): the app MUST NOT ask for or store user PAT tokens.
A caller-supplied workspace host is rejected too: the next OAuth mint would
POST the app service principal's client secret to that host.
Local dev (no DATABRICKS_APP_PORT) keeps the host override.
"""

import importlib
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import ValidationError
from back.objects.domain.settings.WarehouseSettings import WarehouseSettings

pytestmark = pytest.mark.unit

# WarehouseSettings resolves get_domain / is_databricks_app through this module.
_ss = importlib.import_module("back.objects.domain.settings.SettingsService")


def _domain():
    domain = MagicMock()
    domain.databricks = {}
    return domain


def _save(domain, body, app_mode):
    with patch.object(_ss, "get_domain", return_value=domain), patch.object(
        _ss, "is_databricks_app", return_value=app_mode
    ):
        return WarehouseSettings.apply_config_save(body, "a@b.com", "", MagicMock(), MagicMock())


def test_host_rejected_in_app_mode():
    with pytest.raises(ValidationError, match="workspace host"):
        _save(_domain(), {"host": "https://elsewhere.example"}, app_mode=True)


def test_rejected_host_is_not_stored():
    domain = _domain()
    with pytest.raises(ValidationError):
        _save(domain, {"host": "https://elsewhere.example"}, app_mode=True)
    assert domain.databricks == {}


def test_host_still_accepted_in_local_mode():
    domain = _domain()
    _save(domain, {"host": "https://dev.example"}, app_mode=False)
    assert domain.databricks["host"] == "https://dev.example"


@pytest.mark.parametrize("app_mode", [True, False])
def test_token_in_body_is_never_stored(app_mode):
    # Regression guard for the 0.10.0 behaviour: /settings/save ignores tokens.
    domain = _domain()
    _save(domain, {"token": "dapi-caller"}, app_mode=app_mode)
    assert "token" not in domain.databricks
