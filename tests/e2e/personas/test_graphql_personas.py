"""UAT — GraphQL surface gating per persona.

Two GraphQL entry points behave differently:

* ``GET /dtwin/graphql/schema`` is a domain-scoped *read* — any team member
  (viewer included) may fetch it.
* ``POST /dtwin/graphql/execute`` is a domain-scoped *write-method* (POST), so
  the middleware's viewer-write block refuses a viewer, while editor+ pass.

(The ``/graphql/`` prefix is a separate, middleware-bypassed surface like the
REST API; persona gating there is characterized in ``test_api_personas``.)
"""

from __future__ import annotations

import pytest

from tests.e2e.personas import personas as P
from tests.e2e.personas._helpers import csrf_headers, seed_ontology

pytestmark = [pytest.mark.e2e, pytest.mark.uat]

_GQL = '{"query": "{ __schema { queryType { name } } }"}'


class TestGraphqlSchemaReadable:
    @pytest.mark.parametrize("persona", P.ALL_PERSONAS, ids=P.ids)
    def test_schema_read_not_blocked(self, persona_page, live_server, persona):
        page = persona_page(persona)
        seed_ontology(page, live_server)  # schema needs an ontology to build
        resp = page.context.request.get(
            f"{live_server}/dtwin/graphql/schema",
            headers={"Accept": "application/json"},
        )
        assert (
            resp.status != 403
        ), f"{persona.key} should read the GraphQL schema: {resp.text()[:200]}"


class TestGraphqlExecuteGating:
    @pytest.mark.parametrize("persona", P.ALL_PERSONAS, ids=P.ids)
    def test_execute_respects_role(self, persona_page, live_server, persona):
        page = persona_page(persona)
        seed_ontology(page, live_server)
        headers = csrf_headers(page.context)
        expect_block = persona.effective_domain_level < P.ROLE_LEVEL[P.ROLE_EDITOR]
        if expect_block:
            resp = page.context.request.post(
                f"{live_server}/dtwin/graphql/execute", headers=headers, data=_GQL
            )
            assert (
                resp.status == 403
            ), f"viewer GraphQL execute must be 403: {resp.text()[:200]}"
        else:
            try:
                resp = page.context.request.post(
                    f"{live_server}/dtwin/graphql/execute",
                    headers=headers,
                    data=_GQL,
                    timeout=20000,
                )
            except Exception as exc:  # noqa: BLE001
                pytest.skip(f"GraphQL execute needs the triple store (offline): {exc}")
            assert resp.status != 403, resp.text()[:200]
