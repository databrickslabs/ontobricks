/**
 * OntoBricks – ontology-assistant.js
 * AI chat inside a floating popup (bottom-left of the canvas) for modifying
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
    function popupEl()       { return el('assistantPopup'); }
    function messagesEl()    { return el('assistantMessages'); }
    function inputEl()       { return el('assistantInput'); }
    function sendBtn()       { return el('assistantSendBtn'); }
    function clearBtn()      { return el('assistantClearBtn'); }
    function toggleBtn()     { return el('mapToggleAssistant'); }
    function closeBtn()      { return el('assistantPopupClose'); }

    // =====================================================
    // Toggle floating popup
    // =====================================================

    function toggleAssistant() {
        const popup = popupEl();
        if (!popup) return;

        const isVisible = popup.style.display !== 'none';
        popup.style.display = isVisible ? 'none' : 'flex';

        const btn = toggleBtn();
        if (btn) btn.classList.toggle('active', !isVisible);

        if (!isVisible) {
            init();
            initDrag(popup);
            const inp = inputEl();
            if (inp) inp.focus();
        }
    }

    function closeAssistant() {
        const popup = popupEl();
        if (popup) popup.style.display = 'none';

        const btn = toggleBtn();
        if (btn) btn.classList.remove('active');
    }

    // =====================================================
    // Drag-to-move
    // =====================================================

    function initDrag(popup) {
        const header = popup.querySelector('.assistant-popup-header');
        if (!header || header._dragInit) return;
        header._dragInit = true;

        let dragging = false;
        let startX, startY, origLeft, origTop;

        header.addEventListener('mousedown', (e) => {
            if (e.target.closest('button')) return;
            dragging = true;
            startX = e.clientX;
            startY = e.clientY;

            const parent = popup.offsetParent || document.body;
            const rect = popup.getBoundingClientRect();
            const parentRect = parent.getBoundingClientRect();
            origLeft = rect.left - parentRect.left;
            origTop = rect.top - parentRect.top;

            popup.style.left = origLeft + 'px';
            popup.style.top = origTop + 'px';
            popup.style.bottom = 'auto';
            popup.style.right = 'auto';

            header.style.cursor = 'grabbing';
            e.preventDefault();
        });

        document.addEventListener('mousemove', (e) => {
            if (!dragging) return;
            const parent = popup.offsetParent || document.body;
            const parentRect = parent.getBoundingClientRect();

            let newLeft = origLeft + (e.clientX - startX);
            let newTop = origTop + (e.clientY - startY);

            newLeft = Math.max(0, Math.min(newLeft, parentRect.width - popup.offsetWidth));
            newTop = Math.max(0, Math.min(newTop, parentRect.height - popup.offsetHeight));

            popup.style.left = newLeft + 'px';
            popup.style.top = newTop + 'px';
        });

        document.addEventListener('mouseup', () => {
            if (!dragging) return;
            dragging = false;
            header.style.cursor = '';
        });
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
        const welcome = container.querySelector('.assistant-welcome');
        if (welcome) {
            welcome.style.display = '';
        }
        Array.from(container.children).forEach(child => {
            if (!child.classList.contains('assistant-welcome')) child.remove();
        });
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
    // Initialization (called once)
    // =====================================================

    function init() {
        if (initialized) return;

        const inp = inputEl();
        const sBtn = sendBtn();
        const cBtn = clearBtn();

        if (!inp) return;
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

    // =====================================================
    // Keep FAB/popup clear of the right detail panel
    // =====================================================

    function initPanelOffsetWatcher() {
        const mapContainer = document.getElementById('ontology-map-container');
        if (!mapContainer) return;

        const cardBody = mapContainer.closest('.card-body');
        if (!cardBody) return;

        function updateOffset() {
            const panel = mapContainer.querySelector('.shared-detail-panel');
            const handle = mapContainer.querySelector('.detail-panel-resize-handle');
            if (mapContainer.classList.contains('panel-open') && panel) {
                const pw = panel.offsetWidth || 0;
                const hw = (handle && handle.offsetWidth) || 0;
                cardBody.style.setProperty('--ob-panel-offset', (pw + hw) + 'px');
            } else {
                cardBody.style.setProperty('--ob-panel-offset', '0px');
            }
        }

        new MutationObserver(updateOffset).observe(mapContainer, {
            attributes: true, attributeFilter: ['class']
        });

        new MutationObserver(() => {
            const panel = mapContainer.querySelector('.shared-detail-panel');
            if (panel && !panel._resizeObs) {
                panel._resizeObs = new ResizeObserver(updateOffset);
                panel._resizeObs.observe(panel);
            }
            updateOffset();
        }).observe(mapContainer, { childList: true });

        updateOffset();
    }

    document.addEventListener('DOMContentLoaded', () => {
        const tBtn = toggleBtn();
        if (tBtn) {
            if (window.isActiveVersion === false) {
                tBtn.style.display = 'none';
            } else {
                tBtn.addEventListener('click', toggleAssistant);
            }
        }

        const cBtn = closeBtn();
        if (cBtn) cBtn.addEventListener('click', closeAssistant);

        initPanelOffsetWatcher();
    });

    window.initOntologyAssistant = init;
})();
