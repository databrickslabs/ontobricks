/**
 * Shared searchable picker for AI Gateway model services and legacy serving endpoints.
 */
(function () {
    'use strict';

    let endpoints = [];
    let selected = { name: '', kind: '' };
    let resolver = null;
    let onPickerKeydown = null;

    function elements() {
        return {
            modal: document.getElementById('llmEndpointPickerModal'),
            search: document.getElementById('llmEndpointSearch'),
            refresh: document.getElementById('llmEndpointRefresh'),
            clear: document.getElementById('llmEndpointClear'),
            status: document.getElementById('llmEndpointPickerStatus'),
            gateway: document.getElementById('llmGatewayList'),
            serving: document.getElementById('llmServingList')
        };
    }

    function emptyRow(message) {
        const row = document.createElement('div');
        row.className = 'llm-picker-empty small';
        row.textContent = message;
        return row;
    }

    function renderGroup(container, items) {
        container.replaceChildren();
        if (items.length === 0) {
            container.appendChild(emptyRow('No matching models'));
            return;
        }
        items.forEach((endpoint) => {
            const button = document.createElement('button');
            button.type = 'button';
            button.className = 'list-group-item list-group-item-action llm-picker-item';
            if (endpoint.name === selected.name && endpoint.kind === selected.kind) {
                button.classList.add('is-selected');
            }

            const name = document.createElement('span');
            name.className = 'd-block fw-semibold llm-picker-item-name';
            name.textContent = endpoint.name;
            button.appendChild(name);

            if (endpoint.comment) {
                const comment = document.createElement('span');
                comment.className = 'd-block small text-muted mt-1';
                comment.textContent = endpoint.comment;
                button.appendChild(comment);
            }
            button.addEventListener('click', () => choose(endpoint));
            container.appendChild(button);
        });
    }

    function render() {
        const el = elements();
        const query = (el.search.value || '').trim().toLowerCase();
        const visible = endpoints.filter((endpoint) => {
            const haystack = `${endpoint.name} ${endpoint.comment || ''}`.toLowerCase();
            return haystack.includes(query);
        });
        renderGroup(
            el.gateway,
            visible.filter((endpoint) => endpoint.kind === 'ai_gateway')
        );
        renderGroup(
            el.serving,
            visible.filter((endpoint) => endpoint.kind === 'serving')
        );
    }

    function restoreParentModalState() {
        if (document.querySelector('.modal.show')) {
            document.body.classList.add('modal-open');
        }
    }

    function finish(value) {
        const el = elements();
        bootstrap.Modal.getOrCreateInstance(el.modal).hide();
        if (resolver) {
            const resolve = resolver;
            resolver = null;
            resolve(value);
        }
    }

    function choose(endpoint) {
        finish({ name: endpoint.name, kind: endpoint.kind });
    }

    async function loadEndpoints() {
        const el = elements();
        el.status.textContent = 'Loading available models…';
        el.refresh.disabled = true;
        try {
            const response = await fetch('/mapping/wizard/llm-endpoints', {
                credentials: 'same-origin'
            });
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            const data = await response.json();
            endpoints = data.success && Array.isArray(data.endpoints)
                ? data.endpoints
                : [];
            el.status.textContent = `${endpoints.length} available model${endpoints.length === 1 ? '' : 's'}`;
            render();
        } catch (error) {
            endpoints = [];
            el.status.textContent = 'Could not load available models.';
            render();
            console.error('LLM endpoint picker failed:', error);
        } finally {
            el.refresh.disabled = false;
        }
    }

    function init() {
        const el = elements();
        if (!el.modal || el.modal.dataset.initialized === 'true') return;
        el.modal.dataset.initialized = 'true';
        el.search.addEventListener('input', render);
        el.refresh.addEventListener('click', loadEndpoints);
        el.clear.addEventListener('click', () => finish({ name: '', kind: '' }));
        el.modal.addEventListener('hidden.bs.modal', () => {
            if (onPickerKeydown) {
                document.removeEventListener('keydown', onPickerKeydown, true);
                onPickerKeydown = null;
            }
            restoreParentModalState();
            if (resolver) {
                const resolve = resolver;
                resolver = null;
                resolve(null);
            }
        });
    }

    window.openLlmEndpointPicker = function (current = {}) {
        init();
        const el = elements();
        selected = {
            name: current.name || '',
            kind: current.kind || ''
        };
        el.search.value = '';
        const modal = bootstrap.Modal.getOrCreateInstance(el.modal);
        onPickerKeydown = (event) => {
            if (event.key !== 'Escape') return;
            event.preventDefault();
            event.stopImmediatePropagation();
            modal.hide();
        };
        document.addEventListener('keydown', onPickerKeydown, true);
        modal.show();
        const backdrops = document.querySelectorAll('.modal-backdrop');
        backdrops[backdrops.length - 1]?.classList.add(
            'llm-endpoint-picker-backdrop'
        );
        loadEndpoints();
        return new Promise((resolve) => {
            resolver = resolve;
        });
    };
})();
