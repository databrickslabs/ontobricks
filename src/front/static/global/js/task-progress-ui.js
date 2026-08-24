/**
 * OntoBricks - task-progress-ui.js
 * Shared step checklist + activity log for async task overlays (wizard, auto-map, …).
 */
(function () {
    'use strict';

    const SPINNER_SVG = `
        <svg class="ob-spinner-svg" viewBox="0 0 80 80" fill="none">
            <g class="ob-ring">
                <g stroke="#CBD5E1" stroke-width="1.2" opacity="0.5">
                    <line x1="40" y1="10" x2="61" y2="19"/><line x1="61" y1="19" x2="70" y2="40"/>
                    <line x1="70" y1="40" x2="61" y2="61"/><line x1="61" y1="61" x2="40" y2="70"/>
                    <line x1="40" y1="70" x2="19" y2="61"/><line x1="19" y1="61" x2="10" y2="40"/>
                    <line x1="10" y1="40" x2="19" y2="19"/><line x1="19" y1="19" x2="40" y2="10"/>
                </g>
                <circle cx="40" cy="10" r="5" fill="#FF3621"/><circle cx="61" cy="19" r="5" fill="#6366F1"/>
                <circle cx="70" cy="40" r="5" fill="#4ECDC4"/><circle cx="61" cy="61" r="5" fill="#F59E0B"/>
                <circle cx="40" cy="70" r="5" fill="#FF3621"/><circle cx="19" cy="61" r="5" fill="#6366F1"/>
                <circle cx="10" cy="40" r="5" fill="#4ECDC4"/><circle cx="19" cy="19" r="5" fill="#F59E0B"/>
            </g>
            <g transform="translate(40,40)">
                <g class="ob-center">
                    <path d="M0-12 L10-6 L0 0 L-10-6Z" fill="#FF3621"/>
                    <path d="M0-5 L10 1 L0 7 L-10 1Z" fill="#FF3621" opacity="0.85"/>
                    <path d="M0 2 L10 8 L0 14 L-10 8Z" fill="#FF3621" opacity="0.7"/>
                </g>
            </g>
        </svg>`;

    function escHtml(s) {
        return String(s || '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }

    function stepIcon(status) {
        if (status === 'completed') return '<i class="bi bi-check-circle-fill text-success me-2"></i>';
        if (status === 'running') {
            return '<span class="spinner-border spinner-border-sm text-primary me-2" role="status"></span>';
        }
        if (status === 'failed') return '<i class="bi bi-x-circle-fill text-danger me-2"></i>';
        if (status === 'skipped') return '<i class="bi bi-dash-circle text-muted me-2"></i>';
        return '<i class="bi bi-circle text-muted me-2"></i>';
    }

    /**
     * @param {HTMLElement|null} listEl
     * @param {object} task
     * @param {{ skipMessagePrefixes?: string[] }} [opts]
     */
    function renderStepLog(listEl, task, opts) {
        if (!listEl || !task || !Array.isArray(task.steps)) return;
        const skipPrefixes = (opts && opts.skipMessagePrefixes) || [];

        const rows = task.steps.map((step) => {
            let detail = '';
            const msg = task.message || '';
            const showDetail = step.status === 'running' && msg
                && !skipPrefixes.some((p) => msg.startsWith(p));
            if (showDetail) {
                detail = `<div class="small text-muted ms-4">${escHtml(msg)}</div>`;
            } else if (step.status === 'failed' && task.error) {
                detail = `<div class="small text-danger ms-4">${escHtml(task.error)}</div>`;
            }
            return `<li class="list-group-item bg-transparent px-0 py-1 border-0">` +
                `<div class="d-flex align-items-center">${stepIcon(step.status)}` +
                `<span>${escHtml(step.description || step.name)}</span></div>${detail}</li>`;
        });
        listEl.innerHTML = rows.join('');
    }

    /**
     * @param {HTMLElement|null} containerEl
     * @param {object} task
     */
    function renderActivityLog(containerEl, task) {
        if (!containerEl || !task) return;

        const entries = task.log_entries || [];
        const isActive = task.status === 'running' || task.status === 'pending';

        if (!entries.length) {
            containerEl.innerHTML = '<div class="small text-muted">Waiting for activity…</div>';
            return;
        }

        const rows = entries.map((entry, idx) => {
            const isLast = idx === entries.length - 1;
            const running = isActive && isLast;
            const icon = running
                ? '<span class="spinner-border spinner-border-sm text-primary flex-shrink-0" role="status" style="width:1rem;height:1rem;margin-top:2px;"></span>'
                : '<i class="bi bi-check-circle-fill text-success flex-shrink-0 mt-1"></i>';
            return `<div class="task-activity-row d-flex align-items-start gap-2 mb-1 small">` +
                `${icon}<span class="flex-grow-1">${escHtml(entry.message)}</span></div>`;
        });
        containerEl.innerHTML = rows.join('');
        containerEl.scrollTop = containerEl.scrollHeight;
    }

    /**
     * @param {object[]} steps
     * @returns {string}
     */
    function renderAgentStepsLogHtml(steps) {
        if (!steps || !steps.length) return '';

        const iconFor = {
            tool_call: 'bi-wrench text-primary',
            tool_result: 'bi-arrow-return-right text-success',
            output: 'bi-file-earmark-code text-dark',
        };

        const truncate = (typeof window.truncate === 'function')
            ? window.truncate
            : (str, max) => {
                const s = String(str || '');
                return s.length > max ? s.slice(0, max) + '…' : s;
            };

        const rows = steps.map((s) => {
            const icon = iconFor[s.type] || 'bi-dot';
            const label = s.type === 'tool_call'
                ? `<strong>${escHtml(s.tool)}</strong>(${escHtml(s.content || '')})`
                : s.type === 'tool_result'
                    ? `<span class="text-muted">${escHtml(s.tool)} → ${escHtml(truncate(s.content, 80))}</span>`
                    : '<em>Output produced</em>';
            const dur = s.ms ? `<span class="text-muted">${s.ms}ms</span>` : '';
            return `<div class="d-flex align-items-start gap-2 py-1" style="font-size:0.82rem;">` +
                `<i class="bi ${icon}" style="margin-top:2px;"></i>` +
                `<span class="flex-grow-1 text-truncate">${label}</span>${dur}</div>`;
        });

        return `
            <details class="mt-2">
                <summary class="small text-muted" style="cursor:pointer;">
                    <i class="bi bi-robot me-1"></i>Agent activity log (${steps.length} steps)
                </summary>
                <div class="border rounded p-2 mt-1" style="max-height:180px; overflow-y:auto; background:#ffffff;">
                    ${rows.join('')}
                </div>
            </details>`;
    }

    /**
     * Mount (or refresh) the agent step log.
     *
     * Called on every poll tick while a task runs, so an existing mount is
     * re-rendered in place rather than skipped — otherwise the log would
     * freeze on the first batch of steps. The <details> open/closed state is
     * carried over so a refresh never collapses the panel under the user.
     *
     * @param {HTMLElement|null} panelEl
     * @param {object[]} steps
     * @param {string} mountId
     */
    function mountAgentStepsLog(panelEl, steps, mountId) {
        if (!panelEl || !steps || !steps.length) return;
        let mount = panelEl.querySelector('#' + mountId);
        let wasOpen = true;
        if (mount) {
            const prev = mount.querySelector('details');
            if (prev) wasOpen = prev.open;
        } else {
            mount = document.createElement('div');
            mount.id = mountId;
            mount.className = 'task-agent-steps-mount';
            panelEl.appendChild(mount);
        }
        mount.innerHTML = renderAgentStepsLogHtml(steps);
        const details = mount.querySelector('details');
        if (details) details.open = wasOpen;
    }

    /**
     * Build overlay markup for a long-running async task.
     * @param {object} cfg
     */
    function buildOverlayHtml(cfg) {
        const c = cfg || {};
        return `
            <div class="text-center task-progress-panel" style="max-width: 560px; width: 100%;">
                <div class="ob-loading-spinner">
                    ${SPINNER_SVG}
                    <span class="ob-spinner-label" id="${c.titleId || ''}">${escHtml(c.title || 'Working…')}</span>
                </div>
                <p id="${c.messageId || ''}" class="text-muted mt-2 mb-2 small">${escHtml(c.subtitle || '')}</p>
                <div class="progress mb-3" style="height: 6px; max-width: 300px; margin: 0 auto;">
                    <div id="${c.progressBarId || ''}" class="progress-bar progress-bar-striped progress-bar-animated" style="width: 0%"></div>
                </div>
                <div class="task-step-log-panel text-start mb-3">
                    <p class="small fw-semibold text-muted mb-1"><i class="bi bi-list-check me-1"></i>Progress</p>
                    <ul id="${c.stepLogId || ''}" class="list-group list-group-flush small mb-0"></ul>
                </div>
                <div id="${c.activityPanelId || ''}" class="task-activity-log-panel text-start">
                    <p class="small fw-semibold text-muted mb-1"><i class="bi bi-terminal me-1"></i>Activity</p>
                    <div id="${c.activityLogId || ''}" class="task-activity-log"></div>
                </div>
            </div>`;
    }

    /**
     * Show or hide a task progress overlay on a host element.
     * @param {object} cfg
     * @param {boolean} visible
     */
    function setOverlayVisible(cfg, visible) {
        const host = document.getElementById(cfg.hostId);
        if (!host) return;

        let overlay = document.getElementById(cfg.overlayId);
        if (visible) {
            if (!overlay) {
                overlay = document.createElement('div');
                overlay.id = cfg.overlayId;
                overlay.className = cfg.overlayClass || 'task-progress-overlay';
                overlay.innerHTML = buildOverlayHtml(cfg);
                if (getComputedStyle(host).position === 'static') {
                    host.style.position = 'relative';
                }
                host.appendChild(overlay);
            }
            overlay.style.display = 'flex';
        } else if (overlay) {
            overlay.style.display = 'none';
        }
    }

    /**
     * @param {object} cfg — element ids from buildOverlayHtml / setOverlayVisible
     * @param {object} task
     * @param {{ skipMessagePrefixes?: string[] }} [opts]
     */
    function updateFromTask(cfg, task, opts) {
        if (!task) return;
        const progressBar = document.getElementById(cfg.progressBarId);
        const messageEl = document.getElementById(cfg.messageId);
        const stepLog = document.getElementById(cfg.stepLogId);
        const activityLog = document.getElementById(cfg.activityLogId);
        const activityPanel = document.getElementById(cfg.activityPanelId);

        if (progressBar) progressBar.style.width = (task.progress || 0) + '%';

        const skipPrefixes = (opts && opts.skipMessagePrefixes) || [];
        const msg = task.message || '';
        const isStructured = skipPrefixes.some((p) => msg.startsWith(p));

        renderStepLog(stepLog, task, opts);
        if (!isStructured) {
            renderActivityLog(activityLog, task);
        }

        if (messageEl && !isStructured) {
            messageEl.textContent = msg || 'Processing...';
        }

        // Render agent steps as soon as the worker publishes them, not only at
        // completion: the auto-map task republishes its cumulative step log on
        // ``result.agent_steps`` after each chunk, which is what makes the
        // agent's per-entity reasoning visible live.
        if (task.result && task.result.agent_steps) {
            mountAgentStepsLog(activityPanel, task.result.agent_steps, cfg.agentMountId);
        }
    }

    function clearPanels(cfg) {
        const stepLog = document.getElementById(cfg.stepLogId);
        const activityLog = document.getElementById(cfg.activityLogId);
        const agentMount = document.getElementById(cfg.agentMountId);
        if (stepLog) stepLog.innerHTML = '';
        if (activityLog) activityLog.innerHTML = '';
        if (agentMount) agentMount.remove();
    }

    window.TaskProgressUI = {
        escHtml,
        renderStepLog,
        renderActivityLog,
        renderAgentStepsLogHtml,
        mountAgentStepsLog,
        buildOverlayHtml,
        setOverlayVisible,
        updateFromTask,
        clearPanels,
    };
})();
