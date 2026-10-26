"""The DEBUG-only session dump must never print a Databricks token."""

import json
from unittest.mock import MagicMock

from back.objects.domain.Domain import Domain
from back.objects.session.DomainSession import get_empty_domain


def test_session_debug_masks_the_settings_token(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    data = get_empty_domain()
    data["settings"]["databricks"] = {"host": "https://ws", "token": "dapi-secret"}
    session = MagicMock()
    session._data = data

    dumped = json.dumps(Domain(session).get_session_debug_response(), default=str)

    assert "dapi-secret" not in dumped
    assert data["settings"]["databricks"]["token"] == "dapi-secret"
