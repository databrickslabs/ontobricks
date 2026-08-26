"""Rendered contracts for desktop alignment and mobile natural flow."""

import pytest


DESKTOP_VIEWPORT = {"width": 1600, "height": 1000}
MOBILE_VIEWPORT = {"width": 390, "height": 844}

# Representative primary panes from every sidebar page family. Keeping the
# route, SidebarNav section key, and rendered pane selector together makes
# selector drift fail as a readable contract rather than as an auth-dependent
# traversal of every sidebar item.
SIDEBAR_PRIMARY_PANES = [
    pytest.param("/ontology", "map", "#map-section", True, id="ontology-designer"),
    pytest.param("/ontology", "entities", "#entities-section", False, id="ontology-entities"),
    pytest.param(
        "/ontology",
        "relationships",
        "#relationships-section",
        False,
        id="ontology-relationships",
    ),
    pytest.param(
        "/ontology?section=dataquality",
        "dataquality",
        "#dataquality-section > .content-section",
        True,
        id="ontology-data-quality",
    ),
    pytest.param("/mapping", "design", "#design-section", True, id="mapping-designer"),
    pytest.param("/dtwin/", "sigmagraph", "#sigmagraph-section", True, id="kg-explorer"),
    pytest.param("/dtwin/", "graphql", "#graphql-section", True, id="kg-query"),
    pytest.param("/dtwin/?section=chat", "chat", "#chat-section", True, id="kg-chat"),
    pytest.param(
        "/dtwin/?section=dataquality",
        "dataquality",
        "#dataquality-section > .dq-card-fill",
        True,
        id="kg-data-quality",
    ),
    pytest.param("/domain", "information", "#information-section", False, id="domain-information"),
    pytest.param("/settings", "ui", "#ui-section", False, id="settings-ui"),
    pytest.param(
        "/settings?section=logs",
        "logs",
        "#logs-section > .content-section",
        True,
        id="settings-logs",
    ),
]


@pytest.mark.parametrize(
    "path,section,pane_selector,fixed_height",
    SIDEBAR_PRIMARY_PANES,
)
def test_sidebar_and_primary_pane_bottoms_align(
    page,
    live_server,
    path,
    section,
    pane_selector,
    fixed_height,
):
    page.set_viewport_size(DESKTOP_VIEWPORT)
    page.goto(f"{live_server}{path}")
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_function("typeof SidebarNav !== 'undefined'")
    page.evaluate("(section) => SidebarNav.switchTo(section)", section)

    pane = page.locator(pane_selector)
    pane.wait_for(state="visible")

    geometry = page.evaluate(
        """(selector) => {
            const sidebar = document.querySelector('.sidebar-nav');
            const pane = document.querySelector(selector);
            const sidebarRect = sidebar.getBoundingClientRect();
            const paneRect = pane.getBoundingClientRect();
            return {
                sidebarBottom: sidebarRect.bottom,
                paneBottom: paneRect.bottom,
                viewportHeight: window.innerHeight,
                documentHeight: document.documentElement.scrollHeight,
            };
        }""",
        pane_selector,
    )

    delta = abs(geometry["sidebarBottom"] - geometry["paneBottom"])
    assert delta <= 1, (
        f"{path} section {section}: sidebar bottom "
        f"{geometry['sidebarBottom']:.2f}px != pane bottom "
        f"{geometry['paneBottom']:.2f}px (delta {delta:.2f}px)"
    )
    if fixed_height:
        assert geometry["documentHeight"] <= geometry["viewportHeight"] + 1, (
            f"{path} section {section}: fixed-height pane overflows the viewport "
            f"({geometry['documentHeight']}px > {geometry['viewportHeight']}px)"
        )
    if section == "chat":
        assert page.locator("#chatMessages").evaluate(
            "(messages) => getComputedStyle(messages).overflowY"
        ) == "auto"


@pytest.mark.parametrize(
    "path,section,pane_selector",
    [
        pytest.param(
            "/dtwin/?section=chat",
            "chat",
            "#chat-section.active",
            id="kg-chat",
        ),
        pytest.param(
            "/dtwin/?section=dataquality",
            "dataquality",
            "#dataquality-section.active",
            id="kg-data-quality",
        ),
        pytest.param(
            "/settings?section=logs",
            "logs",
            "#logs-section.active",
            id="settings-logs",
        ),
    ],
)
def test_mobile_sidebar_pages_use_window_natural_flow(
    page,
    live_server,
    path,
    section,
    pane_selector,
):
    page.set_viewport_size(MOBILE_VIEWPORT)
    page.goto(f"{live_server}{path}")
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_function("typeof SidebarNav !== 'undefined'")
    page.evaluate("(section) => SidebarNav.switchTo(section)", section)
    page.locator(pane_selector).wait_for(state="visible")

    flow = page.evaluate(
        """(selector) => {
            const pane = document.querySelector(selector);
            const content = document.querySelector('.sidebar-content');
            return {
                paneOverflowY: getComputedStyle(pane).overflowY,
                contentOverflowY: getComputedStyle(content).overflowY,
                horizontalOverflow:
                    document.documentElement.scrollWidth - window.innerWidth,
                documentHeight: document.documentElement.scrollHeight,
                viewportHeight: window.innerHeight,
            };
        }""",
        pane_selector,
    )
    assert flow["paneOverflowY"] == "visible"
    assert flow["contentOverflowY"] == "visible"
    assert flow["horizontalOverflow"] <= 1
    assert flow["documentHeight"] > flow["viewportHeight"]

    page.evaluate("window.scrollTo(0, 0)")
    page.mouse.move(5, MOBILE_VIEWPORT["height"] // 2)
    page.mouse.wheel(0, 600)
    page.wait_for_function("window.scrollY > 0")
    assert page.evaluate("window.scrollY") > 0
