# Graph Chat Readable Results Design

## Context

Some model-serving endpoints return assistant content as a list of structured
content blocks rather than a plain string. Graph Chat currently passes that
value directly to the Markdown renderer, causing JavaScript to display values
such as `[object Object],[object Object]`.

## Scope

This change applies only to Graph Chat. It does not alter other OntoBricks
assistants or expose raw model payloads to users.

## Design

Normalize the final Graph Chat response at the agent boundary, before it is
stored in history or returned by either chat endpoint:

- Preserve plain string responses unchanged.
- Extract readable text from structured content blocks in their original order.
- Join multiple readable blocks into valid Markdown.
- Ignore non-text metadata and technical payloads.
- Return a short, non-technical fallback when no readable text exists.

Add a defensive formatter in the Graph Chat frontend. It will accept either a
string or an unexpected structured value, extract readable text when possible,
and otherwise use the same non-technical fallback. This protects current
streamed responses and previously saved malformed history.

The normalized string remains the only value rendered as Markdown and stored in
conversation history. Tool traces continue to render separately beneath the
answer.

## Error Handling

Malformed, empty, or unknown content blocks must never be string-coerced into
`[object Object]` or displayed as raw JSON. Graph Chat should instead display:

> I couldn't display that answer. Please try again.

## Testing

Add focused tests covering:

1. Plain string content.
2. A list of text content blocks.
3. Mixed text and non-text blocks.
4. Empty or malformed structured content.
5. Frontend rendering safeguards for unexpected reply and history values.

Run the repository test suite with:

`uv run pytest -q -m "not scenario"`

## Success Criteria

- Graph Chat never displays `[object Object]` for assistant replies.
- Structured model responses appear as readable, non-technical Markdown.
- Saved conversation history contains normalized strings.
- Tool trace rendering remains unchanged.
