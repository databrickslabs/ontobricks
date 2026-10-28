/**
 * Shared Knowledge Graph source picker (live inferred, live asserted, sandbox).
 *
 * Used by Explorer, Query, and Chat so SPARQL/GraphQL/chat tools hit the
 * same overlay table as the canvas.
 */
const KgGraphSource = (function () {
    'use strict';

    var STORAGE_KEY = 'ontobricks_kg_graph_source';
    var LIVE_INFERRED = 'live:inferred';
    var LIVE_ASSERTED = 'live';
    var CHANGE_EVENT = 'kg-graph-source-change';
    var SELECTOR = '.kg-graph-source';
    var _loading = false;

    function _esc(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function parse(raw) {
        var value = (raw == null || raw === '') ? LIVE_INFERRED : String(raw);
        if (value === LIVE_INFERRED) {
            return { value: LIVE_INFERRED, simulationId: '', includeInferred: true };
        }
        if (value === LIVE_ASSERTED) {
            return { value: LIVE_ASSERTED, simulationId: '', includeInferred: false };
        }
        var sid = value.replace(/:asserted$/, '');
        return { value: sid, simulationId: sid, includeInferred: true };
    }

    function readStored() {
        try {
            return sessionStorage.getItem(STORAGE_KEY) || '';
        } catch (_) {
            return '';
        }
    }

    function writeStored(value) {
        try {
            sessionStorage.setItem(STORAGE_KEY, value);
        } catch (_) { /* private mode */ }
    }

    function wantedFromUrl() {
        try {
            return new URLSearchParams(window.location.search || '').get('simulation') || '';
        } catch (_) {
            return '';
        }
    }

    function wanted(explicit) {
        if (explicit) return explicit;
        var stored = readStored() || LIVE_INFERRED;
        if (!stored || stored === LIVE_INFERRED) {
            var fromUrl = wantedFromUrl();
            if (fromUrl) return fromUrl;
        }
        return stored;
    }

    function selects() {
        return Array.prototype.slice.call(document.querySelectorAll(SELECTOR));
    }

    function liveOptionsHtml() {
        return '<option value="' + LIVE_INFERRED + '">Live graph (inferred)</option>'
            + '<option value="' + LIVE_ASSERTED + '">Live graph</option>';
    }

    function sandboxOptionHtml(sim) {
        return '<option value="' + _esc(sim.id) + '">' + _esc(sim.label || sim.id) + '</option>';
    }

    function resolveChoice(wantedValue, previous, ready) {
        var ids = {};
        (ready || []).forEach(function (row) { ids[row.id] = true; });
        function normalize(value) {
            if (!value) return '';
            if (value === LIVE_INFERRED || value === LIVE_ASSERTED) return value;
            return String(value).replace(/:asserted$/, '');
        }
        function available(value) {
            if (!value) return false;
            if (value === LIVE_INFERRED || value === LIVE_ASSERTED) return true;
            return !!ids[value];
        }
        wantedValue = normalize(wantedValue);
        previous = normalize(previous);
        if (available(wantedValue)) return wantedValue;
        if (available(previous)) return previous;
        return LIVE_INFERRED;
    }

    function current() {
        var sel = selects()[0];
        var raw = (sel && sel.value) ? sel.value : (readStored() || LIVE_INFERRED);
        return parse(raw);
    }

    function payload() {
        var source = current();
        var body = { include_inferred: source.includeInferred };
        if (source.simulationId) body.simulation_id = source.simulationId;
        return body;
    }

    function applyToBody(body) {
        var src = payload();
        body = body || {};
        body.include_inferred = src.include_inferred;
        if (src.simulation_id) body.simulation_id = src.simulation_id;
        else delete body.simulation_id;
        return body;
    }

    function applyToParams(params) {
        var src = payload();
        if (params && typeof params.set === 'function') {
            params.set('include_inferred', src.include_inferred ? 'true' : 'false');
            if (src.simulation_id) params.set('simulation_id', src.simulation_id);
            else params.delete('simulation_id');
            return params;
        }
        params = params || {};
        params.include_inferred = src.include_inferred ? 'true' : 'false';
        if (src.simulation_id) params.simulation_id = src.simulation_id;
        else delete params.simulation_id;
        return params;
    }

    function syncSelects(value, except) {
        selects().forEach(function (sel) {
            if (sel === except) return;
            if (sel.value !== value) sel.value = value;
        });
    }

    function persist(value, except) {
        var parsed = parse(value);
        writeStored(parsed.value);
        syncSelects(parsed.value, except);
        document.dispatchEvent(new CustomEvent(CHANGE_EVENT, { detail: parsed }));
    }

    async function populate(opts) {
        opts = opts || {};
        var nodes = selects();
        if (!nodes.length) return;
        var previous = nodes[0].value || readStored();
        var target = wanted(opts.simulationId || opts.wanted);
        var html = liveOptionsHtml();
        try {
            var resp = await fetch('/simulation/list', { credentials: 'same-origin' });
            var data = await resp.json();
            var rows = (data && data.simulations) || [];
            var ready = rows.filter(function (row) { return row && row.status === 'ready' && row.id; });
            ready.forEach(function (row) { html += sandboxOptionHtml(row); });
            var chosen = resolveChoice(target, previous, ready);
            _loading = true;
            nodes.forEach(function (sel) {
                sel.innerHTML = html;
                sel.value = chosen;
            });
            writeStored(chosen);
        } catch (err) {
            console.warn('[KgGraphSource] could not load simulations', err);
        } finally {
            _loading = false;
        }
    }

    function onSelectChange(ev) {
        if (_loading) return;
        var sel = ev.target;
        if (!sel || !sel.classList || !sel.classList.contains('kg-graph-source')) return;
        persist(sel.value, sel);
    }

    document.addEventListener('change', onSelectChange);

    return {
        LIVE_INFERRED: LIVE_INFERRED,
        LIVE_ASSERTED: LIVE_ASSERTED,
        CHANGE_EVENT: CHANGE_EVENT,
        parse: parse,
        current: current,
        payload: payload,
        applyToBody: applyToBody,
        applyToParams: applyToParams,
        populate: populate,
        persist: persist,
        isLoading: function () { return _loading; }
    };
})();
