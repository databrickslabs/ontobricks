/**
 * OntoBricks -- query-chat.js
 * Graph Chat: converse with the selected domain's knowledge graph.
 *
 * Re-uses the .assistant-* DOM class family for bubbles / thinking /
 * input styling (see /static/ontology/css/ontology-assistant.css), but
 * runs embedded inside the Knowledge Graph sidebar section -- no floating
 * popup, no FAB.
 *
 * Talks to POST /dtwin/assistant/chat which drives the
 * ``agent_dtwin_chat`` agent.
 */

(function () {
    'use strict';

    // Default client-side cap; server may return a different limit
    // (see /dtwin/assistant/history) which overrides this.
    let MAX_HISTORY = 20;

    let conversationHistory = [];
    let isSending = false;
    let initialized = false;
    let historyLoaded = false;

    const OB_ICON_SVG =
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32" fill="none" width="16" height="16">' +
        '<g stroke="#fff" stroke-width="1.5"><line x1="16" y1="5" x2="24" y2="9"/>' +
        '<line x1="24" y1="9" x2="26" y2="16"/><line x1="26" y1="16" x2="24" y2="23"/>' +
        '<line x1="24" y1="23" x2="16" y2="27"/><line x1="16" y1="27" x2="8" y2="23"/>' +
        '<line x1="8" y1="23" x2="6" y2="16"/><line x1="6" y1="16" x2="8" y2="9"/>' +
        '<line x1="8" y1="9" x2="16" y2="5"/></g>' +
        '<circle cx="16" cy="5" r="2.5" fill="#FF3621"/><circle cx="24" cy="9" r="2.5" fill="#6366F1"/>' +
        '<circle cx="26" cy="16" r="2.5" fill="#4ECDC4"/><circle cx="24" cy="23" r="2.5" fill="#F59E0B"/>' +
        '<circle cx="16" cy="27" r="2.5" fill="#FF3621"/><circle cx="8" cy="23" r="2.5" fill="#6366F1"/>' +
        '<circle cx="6" cy="16" r="2.5" fill="#4ECDC4"/><circle cx="8" cy="9" r="2.5" fill="#F59E0B"/>' +
        '<g transform="translate(16,16)"><path d="M0-5 L4-2.5 L0 0 L-4-2.5Z" fill="#FF3621"/>' +
        '<path d="M0-2 L4 .5 L0 3 L-4 .5Z" fill="#FF3621" opacity=".85"/>' +
        '<path d="M0 1 L4 3.5 L0 6 L-4 3.5Z" fill="#FF3621" opacity=".7"/></g></svg>';

    // =====================================================
    // DOM helpers
    // =====================================================

    function el(id)        { return document.getElementById(id); }
    function messagesEl()  { return el('chatMessages'); }
    function inputEl()     { return el('chatInput'); }
    function sendBtn()     { return el('chatSendBtn'); }
    function clearBtn()    { return el('chatClearBtn'); }
    function clearBtnTop() { return el('chatClearBtnTop'); }
    function limitEl()     { return el('chatHistoryLimit'); }
    function depthEl()     { return el('chatDepth'); }
    function getDepth()    { return parseInt(depthEl()?.value || '1', 10); }

    // =====================================================
    // Markdown rendering
    // =====================================================

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

    function renderMarkdown(text) {
        if (typeof marked !== 'undefined' && marked.parse) {
            try {
                marked.setOptions({ breaks: true, gfm: true });
                return marked.parse(text);
            } catch (_) { /* fall through */ }
        }
        return String(text)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/\n/g, '<br>');
    }

    // =====================================================
    // Entity link enhancement
    // Turns /resolve?uri=... links into fancy badges, and auto-
    // linkifies bare knowledge-graph URIs the LLM forgot to wrap.
    // =====================================================

    function _localName(uri) {
        const hashIdx = uri.lastIndexOf('#');
        if (hashIdx > -1 && hashIdx < uri.length - 1) return uri.slice(hashIdx + 1);
        const slashIdx = uri.lastIndexOf('/');
        if (slashIdx > -1 && slashIdx < uri.length - 1) return uri.slice(slashIdx + 1);
        return uri;
    }

    function _decorateResolveAnchor(a) {
        if (a._gcDecorated) return;
        a._gcDecorated = true;
        a.classList.add('graph-chat-entity-link');
        a.setAttribute(
            'title',
            'Open in the Graph Viewer viewer'
        );
        a.setAttribute('target', '_self');
        a.setAttribute('rel', 'noopener');
        const icon = document.createElement('i');
        icon.className = 'bi bi-box-arrow-up-right ms-1';
        icon.setAttribute('aria-hidden', 'true');
        a.appendChild(icon);
    }

    // Match http(s)://... sequences that look like entity URIs
    // (anything that isn't clearly a docs / schema URL).
    const BARE_URI_RE =
        /(https?:\/\/[^\s<>"'`()]+?)(?=[.,;:!?)\]]*(?:\s|$))/g;

    function _autoLinkifyBareUris(root) {
        const walker = document.createTreeWalker(
            root,
            NodeFilter.SHOW_TEXT,
            {
                acceptNode: function (node) {
                    if (!node.nodeValue || !BARE_URI_RE.test(node.nodeValue)) {
                        return NodeFilter.FILTER_REJECT;
                    }
                    // Skip if already inside an <a> or <code>/<pre>
                    let p = node.parentNode;
                    while (p && p !== root) {
                        const tag = p.nodeName;
                        if (tag === 'A' || tag === 'CODE' || tag === 'PRE') {
                            return NodeFilter.FILTER_REJECT;
                        }
                        p = p.parentNode;
                    }
                    return NodeFilter.FILTER_ACCEPT;
                },
            }
        );

        const targets = [];
        let n;
        while ((n = walker.nextNode())) targets.push(n);

        targets.forEach(function (node) {
            const text = node.nodeValue;
            BARE_URI_RE.lastIndex = 0;
            const frag = document.createDocumentFragment();
            let lastIdx = 0;
            let m;
            while ((m = BARE_URI_RE.exec(text)) !== null) {
                if (m.index > lastIdx) {
                    frag.appendChild(
                        document.createTextNode(text.slice(lastIdx, m.index))
                    );
                }
                const uri = m[1];
                const a = document.createElement('a');
                a.href = '/resolve?uri=' + encodeURIComponent(uri);
                a.textContent = _localName(uri);
                _decorateResolveAnchor(a);
                frag.appendChild(a);
                lastIdx = m.index + m[0].length;
            }
            if (lastIdx < text.length) {
                frag.appendChild(
                    document.createTextNode(text.slice(lastIdx))
                );
            }
            node.parentNode.replaceChild(frag, node);
        });
    }

    function enhanceEntityLinks(container) {
        if (!container) return;

        // 1. Beautify any anchor already pointing at /resolve
        container.querySelectorAll('a[href^="/resolve"]').forEach(function (a) {
            _decorateResolveAnchor(a);
        });

        // 2. Convert anchors whose href is a raw http(s) URI into a
        //    /resolve link (safety net if the LLM forgot to wrap)
        container.querySelectorAll('a[href^="http://"], a[href^="https://"]').forEach(
            function (a) {
                if (a._gcDecorated) return;
                const raw = a.getAttribute('href');
                a.setAttribute('href', '/resolve?uri=' + encodeURIComponent(raw));
                if (!a.textContent.trim()) a.textContent = _localName(raw);
                _decorateResolveAnchor(a);
            }
        );

        // 3. Auto-linkify bare URIs found in text nodes
        _autoLinkifyBareUris(container);
    }

    // =====================================================
    // Message rendering
    // =====================================================

    function hideWelcome() {
        const w = messagesEl()?.querySelector('.assistant-welcome');
        if (w) w.style.display = 'none';
    }

    function appendMessage(role, text, extra) {
        const container = messagesEl();
        if (!container) return;
        hideWelcome();

        const div = document.createElement('div');
        const isUser = role === 'user';
        div.className = `assistant-msg ${isUser ? 'user-msg' : 'bot-msg'}`;

        const avatar = document.createElement('div');
        avatar.className = 'assistant-msg-avatar';
        avatar.innerHTML = isUser ? '<i class="bi bi-person-fill"></i>' : OB_ICON_SVG;

        const body = document.createElement('div');
        body.className = 'assistant-msg-body';
        if (isUser) {
            body.textContent = text;
        } else {
            body.innerHTML = renderMarkdown(readableMessage(text));
            enhanceEntityLinks(body);
        }

        if (!isUser && Array.isArray(extra?.tools) && extra.tools.length) {
            body.appendChild(buildToolTrace(extra.tools));
        }

        div.appendChild(avatar);
        div.appendChild(body);
        container.appendChild(div);
        container.scrollTop = container.scrollHeight;
    }

    function buildToolTrace(tools) {
        const wrap = document.createElement('div');
        wrap.className = 'graph-chat-tools';

        const title = document.createElement('span');
        title.className = 'graph-chat-tools-title';
        title.innerHTML = '<i class="bi bi-tools"></i> Tools used (' + tools.length + ')';
        title.addEventListener('click', function () { wrap.classList.toggle('open'); });

        const ul = document.createElement('ul');
        ul.className = 'graph-chat-tools-list';
        tools.forEach(function (t) {
            const li = document.createElement('li');
            const code = document.createElement('code');
            code.textContent = t.name || 'tool';
            li.appendChild(code);
            if (t.duration_ms) {
                const span = document.createElement('span');
                span.textContent = ' (' + t.duration_ms + ' ms)';
                li.appendChild(span);
            }
            ul.appendChild(li);
        });

        wrap.appendChild(title);
        wrap.appendChild(ul);
        return wrap;
    }

    // =====================================================
    // Pending action confirm / cancel card
    //
    // Actions are never auto-confirmed and typed chat text (e.g. "yes")
    // is never treated as confirmation -- the only way to invoke a
    // pending action is an explicit click on the Confirm button below,
    // which POSTs the one-time server-minted token.
    // =====================================================

    /**
     * Pure helper: turn a raw `pending_action` payload (from the SSE
     * `done` event) into a small, display-ready model. Returns null for
     * anything that isn't usable (e.g. missing token).
     */
    function buildPendingActionCardModel(pending) {
        if (!pending || typeof pending !== 'object') return null;
        const token = String(pending.token || '').trim();
        if (!token) return null;
        return {
            token: token,
            entityLabel: String(pending.entity_label || pending.entity_uri || 'this entity'),
            action: String(pending.action || 'action'),
            description: pending.description ? String(pending.description) : '',
            expiresInSec: Number.isFinite(pending.expires_in_sec) ? pending.expires_in_sec : null,
        };
    }

    /**
     * Format a `/dtwin/nodes/action/confirm` response as a short markdown
     * message, mirroring the server-side `format_node_action_response`.
     */
    function formatActionResultText(data) {
        if (!data || !data.success) {
            const msg = data && (data.message || (typeof data.error === 'string' ? data.error : null));
            return msg || 'Could not invoke the action.';
        }
        const lines = [
            'Action: ' + (data.action || ''),
            'Entity: ' + (data.entity_local_id || '') + '  (' + (data.class_name || 'Unknown') + ')',
            '',
        ];
        const rows = Array.isArray(data.rows) ? data.rows : [];
        if (!rows.length) {
            lines.push('Completed — no result rows returned.');
            return lines.join('\n');
        }
        lines.push('Result (' + rows.length + ' row' + (rows.length !== 1 ? 's' : '') + '):');
        rows.forEach(function (row) {
            const parts = Object.keys(row).map(function (k) { return k + ': ' + row[k]; });
            lines.push('  ' + parts.join('  |  '));
        });
        return lines.join('\n');
    }

    function _setPendingCardStatus(card, text, kind) {
        const statusEl = card.querySelector('.graph-chat-pending-action-status');
        if (!statusEl) return;
        statusEl.textContent = text;
        statusEl.classList.remove('is-success', 'is-error');
        if (kind) statusEl.classList.add(kind);
    }

    function _disablePendingCardButtons(card) {
        card.querySelectorAll('button').forEach(function (btn) { btn.disabled = true; });
    }

    async function confirmPendingAction(card, model) {
        _disablePendingCardButtons(card);
        _setPendingCardStatus(card, 'Running…');
        card.classList.add('is-resolved');

        let data = {};
        try {
            const resp = await fetch('/dtwin/nodes/action/confirm', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ token: model.token }),
            });
            data = await resp.json().catch(function () { return {}; });
            if (!resp.ok && data.success === undefined) data.success = false;
        } catch (err) {
            data = { success: false, message: 'Network error: ' + err.message };
        }

        const text = formatActionResultText(data);
        appendMessage('assistant', text);
        conversationHistory.push({ role: 'assistant', content: text });

        _setPendingCardStatus(card, data.success ? 'Confirmed' : 'Failed', data.success ? 'is-success' : 'is-error');
    }

    function cancelPendingAction(card, model) {
        card.remove();
        fetch('/dtwin/nodes/action/cancel', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin',
            body: JSON.stringify({ token: model.token }),
        }).catch(function () { /* best effort -- server TTL still expires the token */ });
    }

    /**
     * Build the Confirm / Cancel card DOM for a `pending_action` payload.
     * Returns null when the payload isn't usable (see buildPendingActionCardModel).
     */
    function buildPendingActionCard(pending) {
        const model = buildPendingActionCardModel(pending);
        if (!model) return null;

        const card = document.createElement('div');
        card.className = 'graph-chat-pending-action';
        card.setAttribute('data-token', model.token);

        const header = document.createElement('div');
        header.className = 'graph-chat-pending-action-header';
        header.innerHTML = '<i class="bi bi-exclamation-diamond-fill"></i><span>Confirm action</span>';
        card.appendChild(header);

        const body = document.createElement('div');
        body.className = 'graph-chat-pending-action-body';
        const nameLine = document.createElement('div');
        nameLine.className = 'graph-chat-pending-action-name';
        const code = document.createElement('code');
        code.textContent = model.action;
        nameLine.appendChild(document.createTextNode('Run '));
        nameLine.appendChild(code);
        nameLine.appendChild(document.createTextNode(' on ' + model.entityLabel));
        body.appendChild(nameLine);
        if (model.description) {
            const desc = document.createElement('div');
            desc.className = 'graph-chat-pending-action-desc';
            desc.textContent = model.description;
            body.appendChild(desc);
        }
        card.appendChild(body);

        const actions = document.createElement('div');
        actions.className = 'graph-chat-pending-action-actions';

        const confirmBtn = document.createElement('button');
        confirmBtn.type = 'button';
        confirmBtn.className = 'graph-chat-pending-action-confirm';
        confirmBtn.textContent = 'Confirm';
        confirmBtn.addEventListener('click', function () { confirmPendingAction(card, model); });

        const cancelBtn = document.createElement('button');
        cancelBtn.type = 'button';
        cancelBtn.className = 'graph-chat-pending-action-cancel';
        cancelBtn.textContent = 'Cancel';
        cancelBtn.addEventListener('click', function () { cancelPendingAction(card, model); });

        actions.appendChild(confirmBtn);
        actions.appendChild(cancelBtn);
        card.appendChild(actions);

        const status = document.createElement('div');
        status.className = 'graph-chat-pending-action-status';
        card.appendChild(status);

        return card;
    }

    function appendError(text) {
        const container = messagesEl();
        if (!container) return;
        hideWelcome();

        const div = document.createElement('div');
        div.className = 'assistant-msg bot-msg error-msg';

        const avatar = document.createElement('div');
        avatar.className = 'assistant-msg-avatar';
        avatar.innerHTML = '<i class="bi bi-exclamation-triangle-fill"></i>';
        avatar.style.background = 'var(--bs-danger, #dc3545)';

        const body = document.createElement('div');
        body.className = 'assistant-msg-body';
        body.textContent = text;

        div.appendChild(avatar);
        div.appendChild(body);
        container.appendChild(div);
        container.scrollTop = container.scrollHeight;
    }

    function showThinking() {
        const container = messagesEl();
        if (!container) return;
        const div = document.createElement('div');
        div.className = 'assistant-thinking';
        div.id = 'chatThinking';
        div.innerHTML =
            '<div class="assistant-thinking-dots"><span></span><span></span><span></span></div>' +
            '<span>Thinking…</span>';
        container.appendChild(div);
        container.scrollTop = container.scrollHeight;
    }

    function hideThinking() {
        const t = el('chatThinking');
        if (t) t.remove();
    }

    // =====================================================
    // Streaming bubble helpers
    // =====================================================

    /**
     * Create a bot message bubble in "streaming" mode.
     * Returns an object with { div, stepsEl, bodyEl } for live updates.
     */
    function createStreamingBubble() {
        const container = messagesEl();
        if (!container) return null;
        hideWelcome();

        const div = document.createElement('div');
        div.className = 'assistant-msg bot-msg';
        div.id = 'chatStreamingBubble';

        const avatar = document.createElement('div');
        avatar.className = 'assistant-msg-avatar';
        avatar.innerHTML = OB_ICON_SVG;

        const body = document.createElement('div');
        body.className = 'assistant-msg-body';

        // Live step tracker shown while the agent is running
        const stepsEl = document.createElement('div');
        stepsEl.className = 'graph-chat-stream-steps';
        stepsEl.innerHTML =
            '<div class="assistant-thinking-dots"><span></span><span></span><span></span></div>' +
            '<span class="graph-chat-stream-status">Starting…</span>';
        body.appendChild(stepsEl);

        div.appendChild(avatar);
        div.appendChild(body);
        container.appendChild(div);
        container.scrollTop = container.scrollHeight;

        return { div, stepsEl, bodyEl: body };
    }

    /**
     * Update the streaming bubble with a step event received from the SSE stream.
     */
    function updateStreamingBubble(bubble, event) {
        if (!bubble) return;
        const { stepsEl } = bubble;
        if (!stepsEl) return;
        const container = messagesEl();

        const statusEl = stepsEl.querySelector('.graph-chat-stream-status');
        if (event.step_type === 'tool_call') {
            if (statusEl) statusEl.textContent = 'Calling ' + (event.tool_name || 'tool') + '…';
        } else if (event.step_type === 'tool_result') {
            if (statusEl) statusEl.textContent = (event.tool_name || 'tool') + ' done (' + (event.duration_ms || 0) + ' ms)';
        }
        if (container) container.scrollTop = container.scrollHeight;
    }

    /**
     * Replace the streaming bubble's placeholder with the final rendered reply.
     */
    function finalizeStreamingBubble(bubble, event) {
        if (!bubble) return;
        const { stepsEl, bodyEl } = bubble;

        // Remove the live-steps placeholder
        if (stepsEl && stepsEl.parentNode === bodyEl) {
            bodyEl.removeChild(stepsEl);
        }

        // Render the final markdown reply
        const reply = readableMessage(event.reply);
        bodyEl.innerHTML = renderMarkdown(reply);
        enhanceEntityLinks(bodyEl);

        if (Array.isArray(event.tools) && event.tools.length) {
            bodyEl.appendChild(buildToolTrace(event.tools));
        }

        // Actions are proposed only -- render Confirm/Cancel and wait for
        // an explicit click. Never auto-confirm here.
        if (event.pending_action) {
            const card = buildPendingActionCard(event.pending_action);
            if (card) bodyEl.appendChild(card);
        }

        const container = messagesEl();
        if (container) container.scrollTop = container.scrollHeight;
        return reply;
    }

    /**
     * Turn the streaming bubble into an error state.
     */
    function errorStreamingBubble(bubble, message) {
        if (!bubble) return;
        const { stepsEl, bodyEl, div } = bubble;
        if (stepsEl && stepsEl.parentNode === bodyEl) {
            bodyEl.removeChild(stepsEl);
        }
        div.className = 'assistant-msg bot-msg error-msg';
        div.querySelector('.assistant-msg-avatar').style.background = 'var(--bs-danger, #dc3545)';
        div.querySelector('.assistant-msg-avatar').innerHTML = '<i class="bi bi-exclamation-triangle-fill"></i>';
        bodyEl.textContent = message;
    }

    // =====================================================
    // SSE stream consumer
    // =====================================================

    async function _consumeStream(bubble, response) {
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        try {
            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                buffer += decoder.decode(value, { stream: true });

                // SSE events are delimited by double newlines
                const parts = buffer.split('\n\n');
                buffer = parts.pop(); // last incomplete chunk

                for (const part of parts) {
                    const line = part.trim();
                    if (!line.startsWith('data:')) continue;
                    let event;
                    try {
                        event = JSON.parse(line.slice(5).trim());
                    } catch (_) { continue; }

                    if (event.type === 'step') {
                        updateStreamingBubble(bubble, event);
                    } else if (event.type === 'done') {
                        return event; // caller handles finalization
                    } else if (event.type === 'error') {
                        throw new Error(event.message || 'Agent error');
                    }
                }
            }
        } finally {
            reader.releaseLock();
        }
        return null;
    }

    // =====================================================
    // API call
    // =====================================================

    async function sendMessage(text) {
        if (!text.trim() || isSending) return;
        isSending = true;
        updateSendButton();

        const inp = inputEl();
        if (inp) inp.disabled = true;

        // Snapshot the PRIOR history (exclusive of the current message)
        // so the backend can rebuild its stored transcript without
        // double-counting the message already passed via the "message"
        // field.
        const priorHistory = conversationHistory.slice(-(MAX_HISTORY * 2));

        appendMessage('user', text);
        conversationHistory.push({ role: 'user', content: text });

        const bubble = createStreamingBubble();

        try {
            const response = await fetch('/dtwin/assistant/chat/stream', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    message: text,
                    history: priorHistory,
                    depth: getDepth(),
                }),
                credentials: 'same-origin',
            });

            if (!response.ok || !response.body) {
                const errData = await response.json().catch(() => ({}));
                const msg = (errData && (errData.message || errData.detail))
                    || ('Request failed (' + response.status + ')');
                errorStreamingBubble(bubble, msg);
                return;
            }

            const doneEvent = await _consumeStream(bubble, response);

            if (doneEvent) {
                const reply = finalizeStreamingBubble(bubble, doneEvent);
                conversationHistory.push({
                    role: 'assistant',
                    content: reply,
                });
            } else {
                errorStreamingBubble(bubble, 'Stream ended without a final response.');
            }
        } catch (err) {
            if (bubble) {
                errorStreamingBubble(bubble, 'Network error: ' + err.message);
            } else {
                appendError('Network error: ' + err.message);
            }
        } finally {
            isSending = false;
            const inp2 = inputEl();
            if (inp2) { inp2.disabled = false; inp2.focus(); }
            updateSendButton();
        }
    }

    // =====================================================
    // Clear conversation (local + server-side)
    // =====================================================

    function _resetMessagesDom() {
        const container = messagesEl();
        if (!container) return;
        const welcome = container.querySelector('.assistant-welcome');
        if (welcome) welcome.style.display = '';
        Array.from(container.children).forEach(function (child) {
            if (!child.classList.contains('assistant-welcome')) child.remove();
        });
    }

    async function clearConversation() {
        conversationHistory = [];
        _resetMessagesDom();
        try {
            await fetch('/dtwin/assistant/history', {
                method: 'DELETE',
                credentials: 'same-origin',
            });
        } catch (_) { /* best effort */ }
    }

    // =====================================================
    // Session-persisted history: load + limit management
    // =====================================================

    function _renderHistoryMessages(messages) {
        if (!Array.isArray(messages) || messages.length === 0) return;
        _resetMessagesDom();
        hideWelcome();
        conversationHistory = [];
        messages.forEach(function (m) {
            if (!m || !Object.prototype.hasOwnProperty.call(m, 'content')) return;
            const role = m.role === 'assistant' ? 'assistant' : 'user';
            const content = role === 'assistant'
                ? readableMessage(m.content)
                : String(m.content || '');
            appendMessage(role, content);
            conversationHistory.push({ role: role, content: content });
        });
    }

    async function loadSavedHistory() {
        if (historyLoaded) return;
        try {
            const response = await fetch('/dtwin/assistant/history', {
                credentials: 'same-origin',
            });
            if (!response.ok) return;
            const data = await response.json();
            if (!data || !data.success) return;

            if (typeof data.limit === 'number' && data.limit > 0) {
                MAX_HISTORY = data.limit;
                const sel = limitEl();
                if (sel) {
                    // ensure the returned value is a selectable option
                    if (!Array.from(sel.options).some(o => o.value == String(data.limit))) {
                        const opt = document.createElement('option');
                        opt.value = String(data.limit);
                        opt.textContent = String(data.limit);
                        sel.appendChild(opt);
                    }
                    sel.value = String(data.limit);
                }
            }

            _renderHistoryMessages(data.messages || []);
        } catch (err) {
            console.warn('Graph Chat: failed to load saved history', err);
        } finally {
            historyLoaded = true;
        }
    }

    async function updateHistoryLimit(newLimit) {
        const n = parseInt(newLimit, 10);
        if (!Number.isFinite(n) || n <= 0) return;
        MAX_HISTORY = n;
        try {
            await fetch('/dtwin/assistant/history/limit', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ limit: n }),
            });
        } catch (_) { /* best effort */ }
    }

    // =====================================================
    // Input helpers
    // =====================================================

    function autoResize() {
        const inp = inputEl();
        if (!inp) return;
        inp.style.height = 'auto';
        inp.style.height = Math.min(inp.scrollHeight, 140) + 'px';
    }

    function updateSendButton() {
        const btn = sendBtn();
        const inp = inputEl();
        if (btn && inp) btn.disabled = !inp.value.trim() || isSending;
    }

    function bindSuggestions() {
        const container = messagesEl();
        if (!container) return;
        container.querySelectorAll('.assistant-suggestion').forEach(function (btn) {
            if (btn._bound) return;
            btn._bound = true;
            btn.addEventListener('click', function () {
                const msg = this.getAttribute('data-message');
                if (!msg) return;
                const inp = inputEl();
                if (inp) inp.value = '';
                sendMessage(msg);
                autoResize();
                updateSendButton();
            });
        });
    }

    // =====================================================
    // Initialization (runs on first activation of the chat section)
    // =====================================================

    function init() {
        if (initialized) return;

        const inp = inputEl();
        const sBtn = sendBtn();
        const cBtn = clearBtn();

        if (!inp) return;
        initialized = true;

        inp.addEventListener('input', function () {
            autoResize();
            updateSendButton();
        });
        inp.addEventListener('keydown', function (e) {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                const txt = inp.value.trim();
                if (txt) {
                    sendMessage(txt);
                    inp.value = '';
                    autoResize();
                    updateSendButton();
                }
            }
        });

        if (sBtn) {
            sBtn.addEventListener('click', function () {
                const txt = (inputEl()?.value || '').trim();
                if (txt) {
                    sendMessage(txt);
                    if (inputEl()) inputEl().value = '';
                    autoResize();
                    updateSendButton();
                }
            });
        }

        if (cBtn) cBtn.addEventListener('click', clearConversation);
        const cBtnTop = clearBtnTop();
        if (cBtnTop) cBtnTop.addEventListener('click', clearConversation);

        const lSel = limitEl();
        if (lSel) {
            lSel.addEventListener('change', function () {
                updateHistoryLimit(lSel.value);
            });
        }

        bindSuggestions();
        updateSendButton();
    }

    document.addEventListener('DOMContentLoaded', function () {
        document.addEventListener('sidebarSectionChanged', function (e) {
            if (e?.detail?.section === 'chat') {
                init();
                loadSavedHistory();
                const inp = inputEl();
                if (inp) setTimeout(function () { inp.focus(); }, 50);
            }
        });

        // In case the chat section is already active at load time.
        const chat = document.getElementById('chat-section');
        if (chat && chat.classList.contains('active')) {
            init();
            loadSavedHistory();
        }
    });

    window.initGraphChat = init;
    window.chatSendMessage = function (text) { sendMessage(text); };
    window.appendChatAssistantMessage = function (text) {
        const content = readableMessage(text);
        appendMessage('assistant', content);
        conversationHistory.push({ role: 'assistant', content: content });
    };
})();