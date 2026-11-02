/**
 * Incremental status card: slot pipeline state, the _data / _slots commits and
 * freshness lag, the Lakebase synced table and the last build's strategy.
 * Fills every [data-incremental-status] container from GET /dtwin/incremental/status;
 * hidden for a full-mode domain or when the status cannot be read.
 */

function _incrementalLag(seconds) {
    if (seconds === null || seconds === undefined) return 'unknown';
    if (seconds < 60) return seconds + 's';
    if (seconds < 3600) return Math.floor(seconds / 60) + 'm';
    var minutes = Math.floor((seconds % 3600) / 60);
    return Math.floor(seconds / 3600) + 'h' + (minutes < 10 ? '0' : '') + minutes + 'm';
}

function _incrementalRelation(rel) {
    if (!rel || rel.kind === 'absent') return 'absent';
    if (rel.kind === 'unknown') return 'unknown';
    return rel.kind + (rel.version === null || rel.version === undefined ? '' : ' v' + rel.version);
}

function _incrementalLines(status) {
    var lines = [];
    var pipeline = status.slot_pipeline || {};
    lines.push(['Slot pipeline', pipeline.state || 'unknown', pipeline.name || '']);
    lines.push(['_data', _incrementalRelation(status.data) + ' · lag ' + _incrementalLag(status.freshness_lag_s),
        (status.data || {}).table || '']);
    lines.push(['_slots', _incrementalRelation(status.slots), (status.slots || {}).table || '']);
    if (status.lakebase_sync) {
        lines.push(['Synced table', status.lakebase_sync.state || 'UNKNOWN', status.lakebase_sync.synced_table || '']);
    }
    var last = status.last_build;
    if (last && !last.error) {
        var facts = last.materialization || {};
        var how = [facts.strategy, facts.apply_mode].filter(Boolean).join(' / ');
        var text = (last.status || '?') + (last.build_kind ? ' (' + last.build_kind + ')' : '') + (how ? ' · ' + how : '');
        if (facts.reconcile_drift) text += ' · drift: full refresh on the next build';
        lines.push(['Last build', text, last.finished_at || '']);
    }
    return lines;
}

function renderIncrementalStatus(status) {
    var cards = document.querySelectorAll('[data-incremental-status]');
    cards.forEach(function (card) {
        var list = card.querySelector('[data-incremental-status-list]');
        if (!status || !status.success || status.mode_requested === 'full' || !list) {
            card.classList.add('d-none');
            return;
        }
        list.replaceChildren();
        _incrementalLines(status).forEach(function (line) {
            var item = document.createElement('li');
            var label = document.createElement('span');
            label.className = 'text-muted me-1';
            label.textContent = line[0] + ':';
            var value = document.createElement('span');
            value.textContent = line[1];
            if (line[2]) value.title = line[2];
            item.append(label, value);
            list.appendChild(item);
        });
        card.classList.remove('d-none');
    });
}

async function loadIncrementalStatus() {
    if (!document.querySelector('[data-incremental-status]')) return;
    try {
        var response = await fetch('/dtwin/incremental/status', { credentials: 'same-origin' });
        renderIncrementalStatus(response.ok ? await response.json() : null);
    } catch (e) {
        console.warn('[Incremental] status unavailable:', e);
        renderIncrementalStatus(null);
    }
}
