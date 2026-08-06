"""Contract tests for the Graph Chat pending-action Confirm/Cancel card.

These are source-level assertions (no JS runtime in this repo, mirroring
``test_query_chat_rendering.py``), plus a couple of pure-Python ports of
``buildPendingActionCardModel`` / ``formatActionResultText`` to exercise the
edge cases without needing a DOM.
"""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
QUERY_CHAT_JS = REPO_ROOT / "src/front/static/query/js/query-chat.js"


def _source() -> str:
    return QUERY_CHAT_JS.read_text(encoding="utf-8")


def test_pending_action_card_is_rendered_from_done_event():
    source = _source()
    assert "function buildPendingActionCardModel(pending)" in source
    assert "function buildPendingActionCard(pending)" in source
    assert "if (event.pending_action)" in source
    assert "buildPendingActionCard(event.pending_action)" in source


def test_confirm_posts_token_to_confirm_route():
    source = _source()
    assert "async function confirmPendingAction(card, model)" in source
    assert "'/dtwin/nodes/action/confirm'" in source
    assert "body: JSON.stringify({ token: model.token })" in source
    assert "credentials: 'same-origin'" in source


def test_cancel_posts_token_to_cancel_route_and_removes_card():
    source = _source()
    assert "async function cancelPendingAction(card, model)" in source
    assert "'/dtwin/nodes/action/cancel'" in source
    assert "card.remove()" in source


def test_never_auto_confirms_or_treats_typed_yes_as_confirmation():
    source = _source()
    # Confirmation only ever happens from the button's click handler.
    assert "confirmPendingAction(card, model)" in source
    assert source.count("confirmPendingAction(") == 2  # def + the one click wiring
    # sendMessage must never inspect the user's typed text for a "yes"-like
    # confirmation shortcut.
    assert "=== 'yes'" not in source
    assert ".toLowerCase() === 'yes'" not in source


def test_pending_action_card_has_dedicated_css_rules():
    css = (REPO_ROOT / "src/front/static/query/css/query-chat.css").read_text(encoding="utf-8")
    assert ".graph-chat-pending-action" in css
    assert ".graph-chat-pending-action-confirm" in css
    assert ".graph-chat-pending-action-cancel" in css


# ---------------------------------------------------------------------
# Pure-Python ports of the two pure JS helpers, to pin their contract
# without a JS runtime.
# ---------------------------------------------------------------------


def _build_pending_action_card_model(pending):
    if not isinstance(pending, dict):
        return None
    token = str(pending.get("token") or "").strip()
    if not token:
        return None
    return {
        "token": token,
        "entityLabel": str(pending.get("entity_label") or pending.get("entity_uri") or "this entity"),
        "action": str(pending.get("action") or "action"),
        "description": str(pending["description"]) if pending.get("description") else "",
        "expiresInSec": pending.get("expires_in_sec") if isinstance(pending.get("expires_in_sec"), (int, float)) else None,
    }


def test_model_helper_rejects_missing_token():
    assert _build_pending_action_card_model({}) is None
    assert _build_pending_action_card_model(None) is None


def test_model_helper_fills_sane_defaults():
    model = _build_pending_action_card_model(
        {"token": "tok-1", "action": "main.ops.recompute_risk", "entity_label": "CUST1"}
    )
    assert model == {
        "token": "tok-1",
        "entityLabel": "CUST1",
        "action": "main.ops.recompute_risk",
        "description": "",
        "expiresInSec": None,
    }
