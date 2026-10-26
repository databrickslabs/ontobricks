from types import SimpleNamespace

from back.core.databricks.request_identity import (
    RequestIdentity,
    set_request_identity,
    reset_request_identity,
)


def test_mapping_module_uses_data_plane_client():
    """mapping.py imports the OBO data-plane factory for source-data reads."""
    import api.routers.internal.mapping as m

    assert hasattr(m, "get_data_plane_client")


def test_data_plane_client_binds_user_token(monkeypatch):
    """The shared factory binds the caller's forwarded token in app mode."""
    import importlib

    H = importlib.import_module("back.core.helpers.DatabricksHelpers")

    class _FakeClient:
        def __init__(self, host="", token="", warehouse_id="", use_cloud_fetch=False):
            self.token = token
            self.warehouse_id = warehouse_id

    monkeypatch.setattr(H._databricks, "is_databricks_app", lambda: True)
    monkeypatch.setattr(H._databricks, "DatabricksClient", _FakeClient)
    monkeypatch.setattr(H.DatabricksHelpers, "resolve_warehouse_id", lambda d, s: "wh")
    monkeypatch.setattr(
        H.DatabricksHelpers, "resolve_use_cloud_fetch", lambda d, s: False
    )
    monkeypatch.setattr(H, "_domain_databricks", lambda d: {"host": "https://x"})

    from back.core.helpers import get_data_plane_client

    tok = set_request_identity(
        RequestIdentity(email="u", user_token="UT", domain_role="editor")
    )
    try:
        client = get_data_plane_client(
            SimpleNamespace(),
            SimpleNamespace(databricks_host="", databricks_token=""),
        )
    finally:
        reset_request_identity(tok)

    assert client.token == "UT"
