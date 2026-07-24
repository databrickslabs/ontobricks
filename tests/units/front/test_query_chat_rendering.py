"""Contract tests for defensive Graph Chat reply rendering."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
QUERY_CHAT_JS = REPO_ROOT / "src/front/static/query/js/query-chat.js"


def _source() -> str:
    return QUERY_CHAT_JS.read_text(encoding="utf-8")


def test_query_chat_defines_readable_message_safeguard():
    source = _source()
    assert "function readableMessage(value)" in source
    assert "I couldn't display that answer. Please try again." in source
    assert "JSON.stringify(value)" not in source


def test_all_assistant_markdown_passes_through_safeguard():
    source = _source()
    assert "renderMarkdown(readableMessage(text))" in source
    assert "const reply = readableMessage(event.reply);" in source
    assert "content: reply" in source
    assert "const content = readableMessage(text);" in source
