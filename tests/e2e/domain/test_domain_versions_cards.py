"""Mocked browser contracts for Domain version cards and actions."""

import json

from playwright.sync_api import expect

DESKTOP = {"width": 1600, "height": 1000}
MOBILE = {"width": 390, "height": 844}

VERSIONS = {
    "success": True,
    "domain_folder": "acme",
    "versions": [
        {
            "version": "12",
            "description": "Current version",
            "status": "DRAFT",
            "author": "alice@example.com",
            "last_update": "2026-09-25T10:00:00Z",
            "last_build": "",
            "is_current": True,
            "is_active": True,
            "transitions": [],
            "delete_control_visible": True,
            "can_delete": False,
            "delete_block_reason": "The latest version cannot be deleted.",
        },
        {
            "version": "11",
            "description": "Old draft",
            "status": "DRAFT",
            "author": "alice@example.com",
            "last_update": "2026-09-20T10:00:00Z",
            "last_build": "2026-09-20T11:00:00Z",
            "is_current": False,
            "is_active": False,
            "transitions": [
                {
                    "target_status": "IN-REVIEW",
                    "label": "Submit for Review",
                    "enabled": True,
                    "blocked_reason": "",
                }
            ],
            "delete_control_visible": True,
            "can_delete": True,
            "delete_block_reason": "",
        },
    ]
    + [
        {
            "version": str(version),
            "description": f"Published version {version}",
            "status": "PUBLISHED",
            "author": "alice@example.com",
            "last_update": f"2026-09-{version:02d}T10:00:00Z",
            "last_build": f"2026-09-{version:02d}T11:00:00Z",
            "is_current": False,
            "is_active": False,
            "transitions": [],
            "delete_control_visible": True,
            "can_delete": False,
            "delete_block_reason": "Published versions cannot be deleted.",
        }
        for version in range(10, 0, -1)
    ],
}


def _watch_console_errors(page):
    errors = []

    def capture(message):
        if message.type == "error":
            errors.append(message.text)

    page.on("console", capture)
    return errors


def _assert_no_console_errors(errors):
    assert errors == [], f"Unexpected browser console errors: {errors}"


def _open(page, live_server, viewport, versions=VERSIONS):
    page.set_viewport_size(viewport)
    page.route(
        "**/domain/versions-list*",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(versions),
        ),
    )
    page.goto(f"{live_server}/domain")
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_function(
        "() => typeof SidebarNav !== 'undefined'"
        " && typeof loadVersionsList === 'function'"
    )
    page.evaluate("SidebarNav.switchTo('versions')")
    page.locator(".dm-version-card").first.wait_for(state="visible")


def _track_refresh_completion(page):
    page.evaluate(
        """() => {
            const originalLoadVersionsList = window.loadVersionsList;
            window.__versionsRefreshComplete = 0;
            window.loadVersionsList = async (...args) => {
                const result = await originalLoadVersionsList(...args);
                window.__versionsRefreshComplete += 1;
                return result;
            };
        }"""
    )


def test_desktop_cards_order_actions_and_full_height(page, live_server):
    console_errors = _watch_console_errors(page)
    _open(page, live_server, DESKTOP)

    cards = page.locator(".dm-version-card")
    expect(cards).to_have_count(12)
    expect(cards.nth(0).locator("h5")).to_have_text("v12")
    expect(cards.nth(1).locator("h5")).to_have_text("v11")
    expect(cards.nth(1).get_by_role("button", name="Submit for Review")).to_be_visible()
    expect(cards.nth(1).get_by_role("button", name="Delete")).to_be_enabled()

    geometry = page.evaluate(
        """() => {
            const sidebar = document.querySelector('.sidebar-nav');
            const workspace = document.querySelector('.dm-versions-workspace');
            const list = document.querySelector('#versionsCardList');
            return {
                sidebarBottom: sidebar.getBoundingClientRect().bottom,
                workspaceBottom: workspace.getBoundingClientRect().bottom,
                listOverflowY: getComputedStyle(list).overflowY,
                listClientHeight: list.clientHeight,
                listScrollHeight: list.scrollHeight,
                horizontalOverflow:
                    document.documentElement.scrollWidth - window.innerWidth,
            };
        }"""
    )
    assert abs(geometry["sidebarBottom"] - geometry["workspaceBottom"]) <= 1
    assert geometry["listOverflowY"] == "auto"
    assert geometry["listScrollHeight"] > geometry["listClientHeight"]
    assert geometry["horizontalOverflow"] <= 1
    _assert_no_console_errors(console_errors)


def test_mobile_cards_use_natural_page_flow(page, live_server):
    console_errors = _watch_console_errors(page)
    _open(page, live_server, MOBILE)

    flow = page.evaluate(
        """() => {
            const section = document.querySelector('#versions-section .content-section');
            const workspace = document.querySelector('.dm-versions-workspace');
            const list = document.querySelector('#versionsCardList');
            const content = document.querySelector('.sidebar-content');
            return {
                sectionOverflowY: getComputedStyle(section).overflowY,
                workspaceOverflowY: getComputedStyle(workspace).overflowY,
                listOverflowY: getComputedStyle(list).overflowY,
                contentOverflowY: getComputedStyle(content).overflowY,
                horizontalOverflow:
                    document.documentElement.scrollWidth - window.innerWidth,
            };
        }"""
    )
    assert flow["sectionOverflowY"] == "visible"
    assert flow["workspaceOverflowY"] == "visible"
    assert flow["listOverflowY"] == "visible"
    assert flow["contentOverflowY"] == "visible"
    assert flow["horizontalOverflow"] <= 1
    _assert_no_console_errors(console_errors)


def test_transition_posts_target_status_and_refreshes(page, live_server):
    console_errors = _watch_console_errors(page)
    page.route(
        "**/domain/set-version-status",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({"success": True, "status": "IN-REVIEW"}),
        ),
    )
    _open(page, live_server, DESKTOP)
    _track_refresh_completion(page)
    page.evaluate("window.showConfirmDialog = () => Promise.resolve(true)")

    with page.expect_request("**/domain/set-version-status") as request_info:
        with page.expect_request("**/domain/versions-list") as refresh_info:
            page.get_by_role("button", name="Submit for Review").click()
    page.wait_for_function("window.__versionsRefreshComplete === 1")

    assert request_info.value.post_data_json == {
        "domain_name": "acme",
        "version": "11",
        "status": "IN-REVIEW",
    }
    assert request_info.value.method == "POST"
    assert refresh_info.value.url.endswith("/domain/versions-list")
    _assert_no_console_errors(console_errors)


def test_delete_calls_loaded_domain_endpoint_and_refreshes(page, live_server):
    console_errors = _watch_console_errors(page)
    page.route(
        "**/domain/versions/11",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(
                {"success": True, "message": 'Version 11 deleted from "acme"'}
            ),
        ),
    )
    _open(page, live_server, DESKTOP)
    _track_refresh_completion(page)
    page.evaluate("window.showConfirmDialog = () => Promise.resolve(true)")

    delete_button = page.locator(".dm-version-card").nth(1).get_by_role(
        "button", name="Delete"
    )
    with page.expect_request("**/domain/versions/11") as request_info:
        with page.expect_request("**/domain/versions-list") as refresh_info:
            delete_button.click()
    page.wait_for_function("window.__versionsRefreshComplete === 1")

    assert request_info.value.method == "DELETE"
    assert request_info.value.url.endswith("/domain/versions/11")
    assert refresh_info.value.url.endswith("/domain/versions-list")
    _assert_no_console_errors(console_errors)


def test_cancelled_delete_returns_focus_to_action(page, live_server):
    console_errors = _watch_console_errors(page)
    _open(page, live_server, DESKTOP)

    delete_button = page.locator(".dm-version-card").nth(1).get_by_role(
        "button", name="Delete"
    )
    delete_button.click()

    modal = page.locator(".modal.show").filter(has_text="Permanently delete")
    modal.wait_for(state="visible")
    modal.get_by_role("button", name="Cancel").click()
    modal.wait_for(state="hidden")
    expect(delete_button).to_be_focused()
    _assert_no_console_errors(console_errors)


def test_cancelled_transition_and_load_return_focus_to_actions(page, live_server):
    console_errors = _watch_console_errors(page)
    _open(page, live_server, DESKTOP)

    transition_button = page.get_by_role("button", name="Submit for Review")
    transition_button.click()
    transition_modal = page.locator(".modal.show").filter(
        has_text="Update Lifecycle Status"
    )
    transition_modal.wait_for(state="visible")
    transition_modal.get_by_role("button", name="Cancel").click()
    transition_modal.wait_for(state="hidden")
    expect(transition_button).to_be_focused()

    load_button = page.locator(".dm-version-card").nth(1).get_by_role(
        "button", name="Load"
    )
    load_button.click()
    load_modal = page.locator(".modal.show").filter(has_text="Load Version")
    load_modal.wait_for(state="visible")
    load_modal.get_by_role("button", name="Cancel").click()
    load_modal.wait_for(state="hidden")
    expect(load_button).to_be_focused()
    _assert_no_console_errors(console_errors)


def test_loaded_transition_updates_global_read_only_state(page, live_server):
    console_errors = _watch_console_errors(page)
    versions = json.loads(json.dumps(VERSIONS))
    versions["versions"][0]["transitions"] = [
        {
            "target_status": "IN-REVIEW",
            "label": "Submit Loaded for Review",
            "enabled": True,
            "blocked_reason": "",
        }
    ]
    lifecycle = {"status": "DRAFT"}

    def transition(route):
        lifecycle["status"] = "IN-REVIEW"
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({"success": True, "status": "IN-REVIEW"}),
        )

    def version_status(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(
                {
                    "success": True,
                    "version": "12",
                    "status": lifecycle["status"],
                    "is_latest": True,
                    "has_registry": True,
                }
            ),
        )

    def navbar_state(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(
                {
                    "domain": {
                        "info": {
                            "name": "Acme",
                            "version": "12",
                            "status": lifecycle["status"],
                        },
                        "stats": {},
                        "domain_folder": "acme",
                    },
                    "warehouse": {},
                    "branding": {},
                }
            ),
        )

    page.route("**/domain/set-version-status", transition)
    page.route("**/domain/version-status*", version_status)
    page.route("**/navbar/state*", navbar_state)
    _open(page, live_server, DESKTOP, versions)

    button = page.get_by_role("button", name="Submit Loaded for Review")
    button.click()
    modal = page.locator(".modal.show").filter(has_text="Update Lifecycle Status")
    modal.wait_for(state="visible")
    modal.get_by_role("button", name="Update Status").click()

    page.wait_for_function(
        "() => document.body.classList.contains('read-only-version')"
        " && window.versionStatus === 'IN-REVIEW'"
    )
    expect(page.locator("#currentDomainName").locator("xpath=..").locator(
        ".domain-status-badge"
    )).to_have_text("In Review")
    expect(page.locator("body")).to_have_class(
        __import__("re").compile(r"\bread-only-version\b")
    )
    _assert_no_console_errors(console_errors)
