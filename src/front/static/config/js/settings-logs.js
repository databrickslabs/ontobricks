/**
 * OntoBricks - settings-logs.js
 *
 * Settings → Logs tab (admin only).
 * Calls GET /settings/logs?lines=N and renders a colorised, filterable
 * terminal-style view of the rotating application log file — identical
 * to what the Databricks Apps console shows.
 *
 * Lazy-loaded on first sidebar navigation to the "logs" section.
 */
document.addEventListener('DOMContentLoaded', function () {

    // ── Log-level colour scheme (dark-terminal palette) ──────────────────
    const LEVEL_COLOR = {
        DEBUG:   'color: #6c757d;',   // muted grey
        INFO:    'color: #63c2de;',   // cyan-blue
        WARNING: 'color: #f9a825;',   // amber
        ERROR:   'color: #f44336;',   // red
        CRITICAL:'color: #ff5722;',   // deep-orange
    };

    // Regex that matches the text-format log lines produced by LogManager:
    //   2026-06-25T08:30:00+0000 | INFO     | <logger> | <module>:<line> | <msg>
    // Also handles JSON-format lines: {"ts":…,"level":…,…}
    const TEXT_RE = /\|\s*(DEBUG|INFO|WARNING|ERROR|CRITICAL)\s*\|/;

    // ── DOM refs ──────────────────────────────────────────────────────────
    const console_       = document.getElementById('logsConsole');
    const placeholder    = document.getElementById('logsLoadingPlaceholder');
    const wrap           = document.getElementById('logsConsoleWrap');
    const statusText     = document.getElementById('logsStatusText');
    const matchCount     = document.getElementById('logsMatchCount');
    const footer         = document.getElementById('logsFooter');
    const lineCountSel   = document.getElementById('logsLineCount');
    const searchInput    = document.getElementById('logsSearchInput');
    const clearSearchBtn = document.getElementById('btnClearLogsSearch');
    const refreshBtn     = document.getElementById('btnRefreshLogs');
    const autoRefreshChk = document.getElementById('logsAutoRefresh');
    const levelFilter    = document.getElementById('logsLevelFilter');

    if (!console_) return;   // panel not in the DOM (non-admin user)

    // ── State ─────────────────────────────────────────────────────────────
    let rawLines         = [];         // full list returned by the API
    let activeLevel      = 'ALL';
    let autoRefreshTimer = null;
    let logsLoaded       = false;

    // ── Lazy-load on first sidebar navigation ─────────────────────────────
    document.addEventListener('sidebarSectionChanged', (e) => {
        if (e.detail?.section === 'logs' && !logsLoaded) {
            loadLogs();
        }
    });

    // ── Controls ──────────────────────────────────────────────────────────
    if (refreshBtn)     refreshBtn.addEventListener('click', loadLogs);
    if (clearSearchBtn) clearSearchBtn.addEventListener('click', () => {
        if (searchInput) searchInput.value = '';
        renderLines();
    });
    if (searchInput)    searchInput.addEventListener('input', renderLines);
    if (lineCountSel)   lineCountSel.addEventListener('change', loadLogs);

    if (autoRefreshChk) {
        autoRefreshChk.addEventListener('change', () => {
            if (autoRefreshChk.checked) {
                scheduleAutoRefresh();
            } else {
                clearAutoRefresh();
            }
        });
    }

    if (levelFilter) {
        levelFilter.querySelectorAll('button[data-level]').forEach(btn => {
            btn.addEventListener('click', () => {
                levelFilter.querySelectorAll('button').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                activeLevel = btn.dataset.level;
                renderLines();
            });
        });
    }

    // ── Auto-refresh ──────────────────────────────────────────────────────
    function scheduleAutoRefresh() {
        clearAutoRefresh();
        autoRefreshTimer = setInterval(loadLogs, 10_000);
    }

    function clearAutoRefresh() {
        if (autoRefreshTimer !== null) {
            clearInterval(autoRefreshTimer);
            autoRefreshTimer = null;
        }
    }

    // ── API call ──────────────────────────────────────────────────────────
    async function loadLogs() {
        const n = lineCountSel ? parseInt(lineCountSel.value, 10) : 200;

        if (placeholder) placeholder.style.display = '';
        if (console_)    console_.style.display = 'none';
        if (statusText)  statusText.textContent = 'Loading…';
        if (matchCount)  matchCount.textContent = '';

        let data;
        try {
            const resp = await fetch(`/settings/logs?lines=${n}`, { credentials: 'same-origin' });
            if (!resp.ok) {
                const errText = await resp.text().catch(() => resp.statusText);
                throw new Error(`HTTP ${resp.status}: ${errText}`);
            }
            data = await resp.json();
        } catch (err) {
            console.error('Error loading /settings/logs:', err);
            if (placeholder) placeholder.innerHTML =
                '<div class="alert alert-danger small m-3">' +
                'Could not load logs: ' + escapeHtml(String(err)) +
                '</div>';
            if (statusText) statusText.textContent = 'Error';
            return;
        }

        logsLoaded = true;
        rawLines = Array.isArray(data.lines) ? data.lines : [];

        if (footer) {
            const path  = data.log_path  ? escapeHtml(data.log_path)  : '(unknown)';
            const level = data.log_level ? escapeHtml(data.log_level) : '—';
            footer.innerHTML =
                `<i class="bi bi-file-text me-1"></i>` +
                `<code class="small">${path}</code> &nbsp;·&nbsp; ` +
                `Level: <code>${level}</code> &nbsp;·&nbsp; ` +
                `${data.total_lines ?? rawLines.length} total line(s) in file`;
        }

        if (placeholder) placeholder.style.display = 'none';
        if (console_)    console_.style.display = '';

        renderLines();

        // Start auto-refresh after first successful load if the toggle is on
        if (autoRefreshChk && autoRefreshChk.checked && autoRefreshTimer === null) {
            scheduleAutoRefresh();
        }
    }

    // ── Render filtered lines into the <pre> ──────────────────────────────
    function renderLines() {
        if (!console_) return;

        const needle = searchInput ? searchInput.value.trim().toLowerCase() : '';

        let visible = rawLines;

        // Level filter
        if (activeLevel !== 'ALL') {
            visible = visible.filter(l => extractLevel(l) === activeLevel);
        }

        // Text search
        if (needle) {
            visible = visible.filter(l => l.toLowerCase().includes(needle));
        }

        if (statusText) {
            statusText.textContent =
                `Showing ${visible.length} of ${rawLines.length} line(s)`;
        }
        if (matchCount) {
            matchCount.textContent = needle
                ? `${visible.length} match${visible.length !== 1 ? 'es' : ''}`
                : '';
        }

        if (visible.length === 0) {
            console_.innerHTML =
                '<span style="color:#6c757d;">(no matching log entries)</span>';
            return;
        }

        // Build HTML with coloured level tokens
        const html = visible.map(line => coloriseLine(line)).join('\n');
        console_.innerHTML = html;

        // Scroll to bottom so newest entries are visible
        if (wrap) wrap.scrollTop = wrap.scrollHeight;
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    /**
     * Extract the log level from a text-format or JSON-format line.
     * Returns one of DEBUG / INFO / WARNING / ERROR / CRITICAL or null.
     */
    function extractLevel(line) {
        // JSON format: {"level":"INFO",…}
        if (line.trimStart().startsWith('{')) {
            try {
                const obj = JSON.parse(line);
                return (obj.level || '').toUpperCase() || null;
            } catch { /* fall through */ }
        }
        // Text format: … | INFO     | …
        const m = TEXT_RE.exec(line);
        return m ? m[1] : null;
    }

    /**
     * Wrap the level token in a <span> with the appropriate colour.
     * The rest of the line is escaped but unstyled (white on dark bg).
     */
    function coloriseLine(line) {
        const level = extractLevel(line);
        if (!level || !LEVEL_COLOR[level]) {
            return '<span>' + escapeHtml(line) + '</span>';
        }
        const style  = LEVEL_COLOR[level];
        const re     = new RegExp('(\\|\\s*' + level + '\\s*\\|)');
        const parts  = line.split(re);
        if (parts.length < 3) {
            return `<span style="${style}">${escapeHtml(line)}</span>`;
        }
        return (
            '<span>' + escapeHtml(parts[0]) + '</span>' +
            `<span style="${style};font-weight:600;">${escapeHtml(parts[1])}</span>` +
            '<span>' + escapeHtml(parts.slice(2).join('')) + '</span>'
        );
    }

    function escapeHtml(text) {
        if (text == null) return '';
        const div = document.createElement('div');
        div.textContent = String(text);
        return div.innerHTML;
    }
});
