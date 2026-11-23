"""``run_blocking`` must keep request-scoped ContextVars in the worker thread."""

import asyncio

import pytest

from back.core.databricks.request_identity import (
    RequestIdentity,
    get_request_identity,
    reset_request_identity,
    set_request_identity,
)
from back.core.helpers import run_blocking

pytestmark = pytest.mark.unit


def test_request_identity_is_visible_inside_run_blocking():
    async def scenario():
        token = set_request_identity(RequestIdentity(email="alice@acme.com", user_token="obo"))
        try:
            return await run_blocking(lambda: get_request_identity().user_token)
        finally:
            reset_request_identity(token)

    assert asyncio.run(scenario()) == "obo"


def test_worker_context_changes_do_not_leak_back_to_caller():
    async def scenario():
        token = set_request_identity(RequestIdentity(email="alice@acme.com"))
        try:
            await run_blocking(set_request_identity, RequestIdentity(email="mallory@acme.com"))
            return get_request_identity().email
        finally:
            reset_request_identity(token)

    assert asyncio.run(scenario()) == "alice@acme.com"
