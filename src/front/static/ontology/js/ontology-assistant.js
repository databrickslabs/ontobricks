/**
 * OntoBricks – ontology-assistant.js
 * AI chat inside the right-side detail panel (AI Assistant tab) for modifying
 * the ontology via natural language.
 */

(function () {
    'use strict';

    const MAX_HISTORY = 20;

    let conversationHistory = [];
    let isSending = false;
    let initialized = false;

    const OB_ICON_SVG = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32" fill="none" width="16" height="16">'
        + '<g stroke="#fff" stroke-width="1.5"><line x1="16" y1="5" x2="24" y2="9"/><line x1="24" y1="9" x2="26" y2="16"/>'
        + '<line x1="26" y1="16" x2="24" y2="23"/><line x1="24" y1="23" x2="16" y2="27"/>'
        + '<line x1="16" y1="27" x2="8" y2="23"/><line x1="8" y1="23" x2="6" y2="16"/>'
        + '<line x1="6" y1="16" x2="8" y2="9"/><line x1="8" y1="9" x2="16" y2="5"/></g>'
        + '<circle cx="16" cy="5" r="2.5" fill="#FF3621"/><circle cx="24" cy="9" r="2.5" fill="#6366F1"/>'
        + '<circle cx="26" cy="16" r="2.5" fill="#4ECDC4"/><circle cx="24" cy="23" r="2.5" fill="#F59E0B"/>'
        + '<circle cx="16" cy="27" r="2.5" fill="#FF3621"/><circle cx="8" cy="23" r="2.5" fill="#6366F1"/>'
        + '<circle cx="6" cy="16" r="2.5" fill="#4ECDC4"/><circle cx="8" cy="9" r="2.5" fill="#F59E0B"/>'
        + '<g transform="translate(16,16)"><path d="M0-5 L4-2.5 L0 0 L-4-2.5Z" fill="#FF3621"/>'
        + '<path d="M0-2 L4 .5 L0 3 L-4 .5Z" fill="#FF3621" opacity=".85"/>'
        + '<path d="M0 1 L4 3.5 L0 6 L-4 3.5Z" fill="#FF3621" opacity=".7"/></g></svg>';

    // =====================================================
    // DOM helpers
    // =====================================================

    function el(id)          { return document.getElementById(id); }
    function messagesEl()    { return el('assistantMessages'); }
    function inputEl()       { return el('assistantInput'); }
    function sendBtn()       { return el('assistantSendBtn'); }
    function clearBtn()      { return el('assistantClearBtn'); }
    function toggleBtn()     { return el('mapToggleAssistant'); }

    // =====================================================
    // Toggle assistant via toolbar button
    // =====================================================

    function toggleAssistant() {
        if (typeof openAssistantPanel === 'function') {
            openAssistantPanel();
        }
    }

    // =====================================================
    // Markdown rendering
    // =====================================================

    function renderMarkdown(text) {
        if (typeof marked !== 'undefined' && marked.parse) {
            try {
                marked.setOptions({ breaks: true, gfm: true });
                return marked.parse(text);
            } catch (_) { /* fall through */ }
        }
        return text
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/\n/g, '<br>');
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
        avatar.innerHTML = isUser
            ? '<i class="bi bi-person-fill"></i>'
            : OB_ICON_SVG;

        const body = document.createElement('div');
        body.className = 'assistant-msg-body';

        if (isUser) {
            body.textContent = text;
        } else {
            body.innerHTML = renderMarkdown(text);
        }

        if (extra?.ontologyChanged) {
            const badge = document.createElement('div');
            badge.className = 'assistant-changed-badge';
            badge.innerHTML = '<i class="bi bi-check-circle-fill"></i> Ontology updated';
            body.appendChild(badge);
        }

        div.appendChild(avatar);
        div.appendChild(body);
        container.appendChild(div);
        container.scrollTop = container.scrollHeight;
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
        div.id = 'assistantThinking';
        div.innerHTML =
            '<div class="assistant-thinking-dots"><span></span><span></span><span></span></div>' +
            '<span>Thinking…</span>';
        container.appendChild(div);
        container.scrollTop = container.scrollHeight;
    }

    function hideThinking() {
        const t = el('assistantThinking');
        if (t) t.remove();
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

        appendMessage('user', text);
        conversationHistory.push({ role: 'user', content: text });

        showThinking();

        try {
            const response = await fetch('/ontology/assistant/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    message: text,
                    history: conversationHistory.slice(-MAX_HISTORY),
                }),
                credentials: 'same-origin',
            });

            hideThinking();
            const data = await response.json();

            if (data.success) {
                appendMessage('assistant', data.reply, {
                    ontologyChanged: data.ontology_changed,
                });
                conversationHistory.push({ role: 'assistant', content: data.reply });

                if (data.ontology_changed && data.config) {
                    refreshOntologyUI(data.config);
                }
            } else {
                appendError(data.message || 'Unknown error');
            }
        } catch (err) {
            hideThinking();
            appendError('Network error: ' + err.message);
        } finally {
            isSending = false;
            const inp2 = inputEl();
            if (inp2) { inp2.disabled = false; inp2.focus(); }
            updateSendButton();
        }
    }

    // =====================================================
    // Refresh ontology state + map after mutation
    // =====================================================

    function refreshOntologyUI(config) {
        if (typeof OntologyState !== 'undefined') {
            OntologyState.config = config;
            OntologyState.loaded = true;
        }

        if (typeof updateClassesList === 'function') updateClassesList();
        if (typeof updatePropertiesList === 'function') updatePropertiesList();

        if (typeof initOntologyMap === 'function') {
            initOntologyMap();
        }

        if (typeof window.autoValidateOntology === 'function') {
            window.autoValidateOntology();
        }

        console.log('[OntologyAssistant] UI refreshed — classes=%d, properties=%d',
            config.classes?.length || 0, config.properties?.length || 0);
    }

    // =====================================================
    // Clear conversation
    // =====================================================

    function clearConversation() {
        conversationHistory = [];
        const container = messagesEl();
        if (!container) return;
        container.innerHTML = '';
        container.innerHTML = `
            <div class="assistant-welcome">
                <div class="assistant-welcome-icon"><img src="/static/global/img/favicon.svg" alt="OntoBricks" width="40" height="40"></div>
                <p>Modify your ontology with natural language:</p>
                <div class="assistant-suggestions">
                    <button class="btn btn-sm btn-outline-primary assistant-suggestion" data-message="Show me all entities and their attributes">
                        <i class="bi bi-list-ul me-1"></i>List entities
                    </button>
                    <button class="btn btn-sm btn-outline-primary assistant-suggestion" data-message="Show me all relationships">
                        <i class="bi bi-arrow-left-right me-1"></i>List relationships
                    </button>
                    <button class="btn btn-sm btn-outline-danger assistant-suggestion" data-message="Remove all the entities that have no relationship and no inheritance">
                        <i class="bi bi-trash me-1"></i>Clean orphans
                    </button>
                </div>
            </div>
        `;
        bindSuggestions();
    }

    // =====================================================
    // Input helpers
    // =====================================================

    function autoResize() {
        const inp = inputEl();
        if (!inp) return;
        inp.style.height = 'auto';
        inp.style.height = Math.min(inp.scrollHeight, 100) + 'px';
    }

    function updateSendButton() {
        const btn = sendBtn();
        const inp = inputEl();
        if (btn && inp) btn.disabled = !inp.value.trim() || isSending;
    }

    function bindSuggestions() {
        document.querySelectorAll('.assistant-suggestion').forEach(btn => {
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
    // Initialization (called once per panel creation)
    // =====================================================

    function init() {
        if (initialized) return;

        const inp = inputEl();
        const sBtn = sendBtn();
        const cBtn = clearBtn();
        const tBtn = toggleBtn();

        if (!inp) return;  // Assistant tab not yet in the DOM
        initialized = true;

        inp.addEventListener('input', () => { autoResize(); updateSendButton(); });
        inp.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                const txt = inp.value.trim();
                if (txt) { sendMessage(txt); inp.value = ''; autoResize(); updateSendButton(); }
            }
        });

        if (sBtn) {
            sBtn.addEventListener('click', () => {
                const txt = (inputEl()?.value || '').trim();
                if (txt) { sendMessage(txt); if (inputEl()) inputEl().value = ''; autoResize(); updateSendButton(); }
            });
        }

        if (cBtn) cBtn.addEventListener('click', clearConversation);

        bindSuggestions();
        updateSendButton();
    }

    // Wire up the toggle button as soon as the DOM is ready
    document.addEventListener('DOMContentLoaded', () => {
        const tBtn = toggleBtn();
        if (tBtn) tBtn.addEventListener('click', toggleAssistant);
    });

    // Expose globally so the shared panel can call it after creating the assistant tab
    window.initOntologyAssistant = init;
})();
