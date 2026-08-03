# Graph Chat Readable Results Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure Graph Chat always renders model answers as readable, non-technical Markdown instead of `[object Object]`.

**Architecture:** Normalize model content once at the Graph Chat agent boundary so API responses and saved history contain strings. Add a small, pure frontend safeguard before Markdown rendering to handle malformed legacy or unexpected payloads without exposing raw objects.

**Tech Stack:** Python 3.10+, pytest, vanilla JavaScript, FastAPI/SSE, Marked

## Global Constraints

- Scope is Graph Chat only.
- Preserve plain string replies.
- Keep text blocks in their original order and separate them with blank lines.
- Ignore non-text metadata; never display raw objects or JSON.
- Use `I couldn't display that answer. Please try again.` when no readable text exists.
- Keep tool traces separate and unchanged.
- Do not create a git commit unless the user explicitly requests one.

---

### Task 1: Normalize Graph Chat replies at the agent boundary

**Files:**
- Create: `tests/units/agents/test_agent_dtwin_chat_engine.py`
- Modify: `src/agents/agent_dtwin_chat/engine.py:37-52,254-320`

**Interfaces:**
- Produces: `normalize_reply_content(content: object) -> str`
- Consumers: `run_agent()` final-answer handling and Task 2's equivalent browser-side contract

- [ ] **Step 1: Write failing normalization tests**

```python
"""Unit tests for Graph Chat final-answer normalization."""

import pytest

from agents.agent_dtwin_chat.engine import normalize_reply_content


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        ("A readable answer.", "A readable answer."),
        (
            [
                {"type": "text", "text": "First paragraph."},
                {"type": "text", "text": "Second paragraph."},
            ],
            "First paragraph.\n\nSecond paragraph.",
        ),
        (
            [
                {"type": "metadata", "data": {"internal": True}},
                {"type": "text", "text": "Visible answer."},
                {"type": "image", "url": "https://example.invalid/image"},
            ],
            "Visible answer.",
        ),
        (
            {"type": "text", "text": {"value": "Nested text."}},
            "Nested text.",
        ),
    ],
)
def test_normalize_reply_content_extracts_readable_text(content, expected):
    assert normalize_reply_content(content) == expected


@pytest.mark.parametrize("content", [None, "", [], {}, [{"type": "metadata"}]])
def test_normalize_reply_content_uses_non_technical_fallback(content):
    assert normalize_reply_content(content) == (
        "I couldn't display that answer. Please try again."
    )
```

- [ ] **Step 2: Run the tests and verify the new function is missing**

Run:

```bash
uv run pytest -q tests/units/agents/test_agent_dtwin_chat_engine.py
```

Expected: collection fails because `normalize_reply_content` is not yet defined.

- [ ] **Step 3: Implement minimal backend normalization**

Add near the Graph Chat engine constants:

```python
_UNREADABLE_REPLY = "I couldn't display that answer. Please try again."


def _extract_reply_text(content: object) -> list[str]:
    if isinstance(content, str):
        return [content] if content.strip() else []
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            parts.extend(_extract_reply_text(item))
        return parts
    if isinstance(content, dict):
        block_type = content.get("type")
        if block_type and block_type not in ("text", "output_text"):
            return []
        for key in ("text", "content", "value"):
            if key in content:
                return _extract_reply_text(content[key])
    return []


def normalize_reply_content(content: object) -> str:
    """Return readable Markdown for a model response content payload."""
    if isinstance(content, str) and content.strip():
        return content
    parts = [part.strip() for part in _extract_reply_text(content) if part.strip()]
    return "\n\n".join(parts) if parts else _UNREADABLE_REPLY
```

Replace the final-answer assignment:

```python
        else:
            content = normalize_reply_content(message.get("content"))
            result.success = True
            result.reply = content
```

Keep tool-call messages in their original provider-compatible structure; normalize only the final user-visible answer.

- [ ] **Step 4: Run focused backend tests**

Run:

```bash
uv run pytest -q tests/units/agents/test_agent_dtwin_chat_engine.py tests/units/agents/test_agent_dtwin_chat.py
```

Expected: all tests pass.

- [ ] **Step 5: Review checkpoint**

Verify `AgentResult.reply` is always a string on every successful final-answer path. Do not commit unless explicitly requested.

---

### Task 2: Add a defensive Graph Chat frontend formatter

**Files:**
- Create: `tests/units/front/test_query_chat_rendering.py`
- Modify: `src/front/static/query/js/query-chat.js:55-69,195-215,358-370,534-544`

**Interfaces:**
- Consumes: Graph Chat reply/history values from the API
- Produces: `readableMessage(value) -> string`

- [ ] **Step 1: Write failing frontend asset-contract tests**

```python
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
```

- [ ] **Step 2: Run the tests and verify the safeguard is missing**

Run:

```bash
uv run pytest -q tests/units/front/test_query_chat_rendering.py
```

Expected: tests fail because `readableMessage` is not yet defined.

- [ ] **Step 3: Implement the pure frontend safeguard**

Add above `renderMarkdown`:

```javascript
    const UNREADABLE_REPLY = "I couldn't display that answer. Please try again.";

    function _extractReadableParts(value) {
        if (typeof value === 'string') return value.trim() ? [value] : [];
        if (Array.isArray(value)) {
            return value.flatMap(function (item) {
                return _extractReadableParts(item);
            });
        }
        if (value && typeof value === 'object') {
            const type = value.type;
            if (type && type !== 'text' && type !== 'output_text') return [];
            for (const key of ['text', 'content', 'value']) {
                if (Object.prototype.hasOwnProperty.call(value, key)) {
                    return _extractReadableParts(value[key]);
                }
            }
        }
        return [];
    }

    function readableMessage(value) {
        if (typeof value === 'string' && value.trim()) return value;
        const parts = _extractReadableParts(value)
            .map(function (part) { return part.trim(); })
            .filter(Boolean);
        return parts.length ? parts.join('\n\n') : UNREADABLE_REPLY;
    }
```

Route every assistant answer through it:

```javascript
        } else {
            body.innerHTML = renderMarkdown(readableMessage(text));
            enhanceEntityLinks(body);
        }
```

Normalize streaming replies before rendering and history storage:

```javascript
        const reply = readableMessage(event.reply);
        bodyEl.innerHTML = renderMarkdown(reply);
```

Return the normalized reply from `finalizeStreamingBubble` and use that value in `sendMessage`:

```javascript
        return reply;
```

```javascript
            if (doneEvent) {
                const reply = finalizeStreamingBubble(bubble, doneEvent);
                conversationHistory.push({
                    role: 'assistant',
                    content: reply,
                });
```

Allow malformed legacy history to reach the safeguard while retaining valid roles:

```javascript
        messages.forEach(function (m) {
            if (!m || !Object.prototype.hasOwnProperty.call(m, 'content')) return;
            const role = m.role === 'assistant' ? 'assistant' : 'user';
            const content = role === 'assistant'
                ? readableMessage(m.content)
                : String(m.content || '');
            appendMessage(role, content);
            conversationHistory.push({ role: role, content: content });
        });
```

Normalize replies inserted through the public Graph Chat helper as well:

```javascript
    window.appendChatAssistantMessage = function (text) {
        const content = readableMessage(text);
        appendMessage('assistant', content);
        conversationHistory.push({ role: 'assistant', content: content });
    };
```

- [ ] **Step 4: Run frontend contract tests**

Run:

```bash
uv run pytest -q tests/units/front/test_query_chat_rendering.py
```

Expected: both tests pass.

- [ ] **Step 5: Verify the reported payload manually**

In a browser console on the Graph Chat page, inject a structured assistant value:

```javascript
window.appendChatAssistantMessage([
    { type: 'text', text: 'First readable section.' },
    { type: 'text', text: 'Second readable section.' },
]);
```

Expected: two readable paragraphs appear; `[object Object]` and raw JSON do not appear.

- [ ] **Step 6: Review checkpoint**

Verify tool traces still appear after the answer and entity-link enhancement still runs. Do not commit unless explicitly requested.

---

### Task 3: Document and verify the complete fix

**Files:**
- Create: `changelogs/v0.7.0/benoitcayladbx_2026-07-21.log`
- Verify: `documentation/superpowers/specs/2026-07-21-graph-chat-readable-results-design.md`
- Verify: `documentation/superpowers/plans/2026-07-21-graph-chat-readable-results.md`

**Interfaces:**
- Consumes: completed backend/frontend implementation and test results
- Produces: repository-required changelog and verification evidence

- [ ] **Step 1: Run focused tests together**

Run:

```bash
uv run pytest -q tests/units/agents/test_agent_dtwin_chat_engine.py tests/units/agents/test_agent_dtwin_chat.py tests/units/front/test_query_chat_rendering.py
```

Expected: all focused tests pass.

- [ ] **Step 2: Run the mandatory non-scenario suite**

Run:

```bash
uv run pytest -q -m "not scenario"
```

Expected: suite passes with zero failures. Record the exact final summary.

- [ ] **Step 3: Create the versioned changelog**

Use this content, then append the exact pytest summary captured in Step 2 after
the final arrow:

```text
## Keep Graph Chat answers readable

Context: Some serving endpoints return assistant answers as structured content
blocks. Graph Chat previously coerced those blocks to JavaScript strings and
displayed `[object Object]` before the tool trace.

Changes:

1. src/agents/agent_dtwin_chat/engine.py
   Normalize structured model content into readable Markdown before persistence.
2. src/front/static/query/js/query-chat.js
   Add a defensive renderer for streamed and saved Graph Chat answers.
3. tests/units/agents/test_agent_dtwin_chat_engine.py
   Cover string, structured, mixed, nested, and unreadable model content.
4. tests/units/front/test_query_chat_rendering.py
   Protect the frontend normalization and rendering integration points.

Modified files:
- src/agents/agent_dtwin_chat/engine.py
- src/front/static/query/js/query-chat.js
- tests/units/agents/test_agent_dtwin_chat_engine.py
- tests/units/front/test_query_chat_rendering.py

Tests: uv run pytest -q -m "not scenario" →
```

- [ ] **Step 4: Check lint diagnostics**

Run IDE lint checks for:

- `src/agents/agent_dtwin_chat/engine.py`
- `src/front/static/query/js/query-chat.js`
- `tests/units/agents/test_agent_dtwin_chat_engine.py`
- `tests/units/front/test_query_chat_rendering.py`

Expected: no newly introduced diagnostics.

- [ ] **Step 5: Final review checkpoint**

Confirm the implementation meets every success criterion in the approved design and report focused/full test results. Do not commit unless explicitly requested.
