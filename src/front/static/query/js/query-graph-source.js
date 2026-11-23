/**
 * Shared Knowledge Graph source picker (Original Graph, Certified Graph, sandbox).
 *
 * Used by Explorer, Query, and Chat so SPARQL/GraphQL/chat tools hit the
 * same relation as the canvas. The Original / Certified choice travels as the
 * ``simulation_id`` token the backend maps to a graph variant; only a real
 * sandbox sets ``sandboxId``.
 */
const KgGraphSource = (function () {
    'use strict';

    var STORAGE_KEY = 'ontobricks_kg_graph_source';
    var ORIGINAL = 'original';
    var CERTIFIED = 'certified';
    // Values stored before the two-phase graph; they read the Original Graph.
    var LEGACY_LIVE = ['live', 'live:inferred'];
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

    function _normalize(raw) {
        var value = (raw == null) ? '' : String(raw);
        if (!value || LEGACY_LIVE.indexOf(value) !== -1) return ORIGINAL;
        return value.replace(/:asserted$/, '');
    }

    function _isVariant(value) {
        return value === ORIGINAL || value === CERTIFIED;
    }

    function parse(raw) {
        var value = _normalize(raw);
        if (value === ORIGINAL) {
            return { value: ORIGINAL, simulationId: ORIGINAL, sandboxId: '', includeInferred: true };
        }
        if (value === CERTIFIED) {
            return { value: CERTIFIED, simulationId: CERTIFIED, sandboxId: '', includeInferred: true };
        }
        return { value: value, simulationId: value, sandboxId: value, includeInferred: true };
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

    function wanted(explicit, forceDefault) {
        if (explicit) return explicit;
        // Explorer open: the Original Graph, not the leftover ``?simulation=``
        // or the last stored Query/Chat source. Inbound links pass ``simulationId``.
        if (forceDefault) return ORIGINAL;
        var fromUrl = wantedFromUrl();
        if (fromUrl) return fromUrl;
        return readStored() || ORIGINAL;
    }

    function selects() {
        return Array.prototype.slice.call(document.querySelectorAll(SELECTOR));
    }

    function liveOptionsHtml() {
        return '<option value="' + ORIGINAL + '">Original graph</option>'
            + '<option value="' + CERTIFIED + '">Certified graph</option>';
    }

    function sandboxOptionHtml(sim) {
        return '<option value="' + _esc(sim.id) + '">' + _esc(sim.label || sim.id) + '</option>';
    }

    function resolveChoice(wantedValue, previous, ready) {
        var ids = {};
        (ready || []).forEach(function (row) { ids[row.id] = true; });
        function available(value) {
            return _isVariant(value) || !!ids[value];
        }
        wantedValue = _normalize(wantedValue);
        previous = _normalize(previous);
        if (available(wantedValue)) return wantedValue;
        if (available(previous)) return previous;
        return ORIGINAL;
    }

    function current() {
        var sel = selects()[0];
        var raw = (sel && sel.value) ? sel.value : (readStored() || ORIGINAL);
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
        var target = wanted(opts.simulationId || opts.wanted, opts.forceDefault);
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
        ORIGINAL: ORIGINAL,
        CERTIFIED: CERTIFIED,
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
