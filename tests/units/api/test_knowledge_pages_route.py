"""Route tests for ``GET /domain/pages/list`` (Context → Genie Pages)."""

import asyncio
import importlib
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.unit

_domain = importlib.import_module("api.routers.internal.domain")


def _call(svc, **kwargs):
    params = dict(
        page_size=1, page_token=None, filter=None, include_domains=False,
        session_mgr=MagicMock(), settings=MagicMock(),
    )
    params.update(kwargs)
    with (
        patch.object(_domain, "get_domain", return_value=MagicMock()),
        patch.object(_domain, "_knowledge_page_service", return_value=svc),
    ):
        return asyncio.run(_domain.list_knowledge_pages(**params))


def _svc():
    svc = MagicMock()
    svc.list_pages.return_value = {"pages": [{"name": "pages/1"}], "next_page_token": "t"}
    svc.list_domains.return_value = [{"name": "domains/1", "label": "Sales"}]
    return svc


def test_pages_list_skips_domains_unless_requested():
    svc = _svc()
    res = _call(svc)
    assert "domains" not in res and not svc.list_domains.called


def test_pages_list_includes_domains_when_requested():
    res = _call(_svc(), include_domains=True)
    assert res["domains"] == [{"name": "domains/1", "label": "Sales"}]


def test_pages_list_reports_domains_failure_without_failing():
    svc = _svc()
    svc.list_domains.side_effect = RuntimeError("boom")
    res = _call(svc, include_domains=True)
    assert res["success"] is True and res["domains"] == [] and res["domains_error"] == "boom"


def test_pages_list_runs_blocking_calls_off_the_event_loop():
    seen = {}

    async def fake_run_blocking(func, *args, **kwargs):
        seen["func"] = func
        return func(*args, **kwargs)

    with patch.object(_domain, "run_blocking", side_effect=fake_run_blocking):
        _call(_svc())
    assert seen["func"] is _domain._knowledge_pages_payload


def test_knowledge_page_service_uses_on_behalf_of_client():
    """Knowledge Pages are read with the signed-in user's token (OBO), not the SP."""
    client = MagicMock()
    with (
        patch.object(_domain, "get_data_plane_client", return_value=client) as obo,
        patch.object(_domain, "get_databricks_client") as sp,
    ):
        svc = _domain._knowledge_page_service(MagicMock(), MagicMock())
    obo.assert_called_once()
    sp.assert_not_called()
    assert svc is client.knowledge_pages


def test_pages_list_surfaces_missing_user_token_as_authorization_error():
    from back.core.errors import AuthorizationError

    with (
        patch.object(_domain, "get_domain", return_value=MagicMock()),
        patch.object(
            _domain,
            "_knowledge_page_service",
            side_effect=AuthorizationError("no user token"),
        ),
    ):
        with pytest.raises(AuthorizationError):
            asyncio.run(
                _domain.list_knowledge_pages(
                    page_size=1, page_token=None, filter=None, include_domains=False,
                    session_mgr=MagicMock(), settings=MagicMock(),
                )
            )


def test_pages_preview_surfaces_missing_user_token_as_authorization_error():
    from back.core.errors import AuthorizationError

    with (
        patch.object(_domain, "get_domain", return_value=MagicMock()),
        patch.object(
            _domain,
            "_knowledge_page_service",
            side_effect=AuthorizationError("no user token"),
        ),
    ):
        with pytest.raises(AuthorizationError):
            asyncio.run(
                _domain.preview_knowledge_page(
                    name="pages/1", session_mgr=MagicMock(), settings=MagicMock(),
                )
            )
