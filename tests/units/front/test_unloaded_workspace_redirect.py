"""Workspace HTML pages send an unloaded session straight Home.

Domain / Ontology / Mapping / Knowledge Graph have no useful chrome without
a domain in session. The four page routes share ``home_redirect_if_unloaded``
and answer 303 ``/`` instead of rendering an empty workspace.
"""

from pathlib import Path

import pytest

from tests.units.api.test_ui_rendering import ensure_loaded_domain

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
DOMAIN_ROUTE = REPO_ROOT / "src/front/routes/domain.py"
ONTOLOGY_ROUTE = REPO_ROOT / "src/front/routes/ontology.py"
MAPPING_ROUTE = REPO_ROOT / "src/front/routes/mapping.py"
DTWIN_ROUTE = REPO_ROOT / "src/front/routes/dtwin.py"
DEPENDENCIES = REPO_ROOT / "src/front/fastapi/dependencies.py"

_WORKSPACE_PAGES = (
    "/domain/",
    "/ontology/",
    "/mapping/",
    "/dtwin/",
)


class TestTheGuardLivesOnEveryWorkspacePage:
    @pytest.mark.parametrize(
        "route",
        [DOMAIN_ROUTE, ONTOLOGY_ROUTE, MAPPING_ROUTE, DTWIN_ROUTE],
    )
    def test_the_page_route_calls_the_shared_unloaded_guard(self, route):
        src = route.read_text(encoding="utf-8")
        assert "home_redirect_if_unloaded" in src

    def test_the_guard_sends_home_with_a_see_other(self):
        src = DEPENDENCIES.read_text(encoding="utf-8")
        start = src.index("def home_redirect_if_unloaded")
        body = src[start : start + 900]
        assert "WorkspacePresence.is_loaded(domain_session)" in body
        assert 'RedirectResponse("/", status_code=303)' in body


class TestAnUnloadedSessionNeverRendersAWorkspace:
    @pytest.mark.parametrize("path", _WORKSPACE_PAGES)
    def test_get_redirects_home(self, client, path):
        response = client.get(path, follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"] == "/"

    @pytest.mark.parametrize("path", _WORKSPACE_PAGES)
    def test_a_named_domain_still_renders_the_page(self, client, path):
        ensure_loaded_domain(client)
        response = client.get(path, follow_redirects=False)
        assert response.status_code == 200
