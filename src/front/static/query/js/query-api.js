/**
 * Digital Twin API documentation page logic.
 *
 * - Detects the base URL from the current window location
 * - Wires the "Try it" buttons to fetch live data from GET endpoints
 * - Provides expand/collapse for endpoint cards
 *
 * External programmatic surface uses ``/api/v1/graphql`` (same as ``/graphql`` on the main app).
 */

const GRAPHQL_EXTERNAL_PREFIX = '/api/v1/graphql';

/* ------------------------------------------------------------------ */
/* Initialisation                                                      */
/* ------------------------------------------------------------------ */

document.addEventListener('DOMContentLoaded', () => {
    const baseEl = document.getElementById('apiBaseUrl');
    if (baseEl) {
        baseEl.textContent = window.location.origin + '/api/v1';
    }
    loadApiDomains();

    const select = document.getElementById('apiDomainName');
    if (select) {
        select.addEventListener('change', () => loadApiVersions(select.value));
    }

    bindQueryApiInteractions();
});

/**
 * Wire API reference UI from ``data-action`` / ``data-try-endpoint`` (no inline handlers).
 */
function bindQueryApiInteractions() {
    document.querySelectorAll('[data-action]').forEach((el) => {
        const action = el.dataset.action;
        if (action === 'toggle-api-card') {
            el.addEventListener('click', () => toggleApiCard(el));
        } else if (action === 'copy-base-url') {
            el.addEventListener('click', () => copyApiBaseUrl());
        } else if (action === 'find-run') {
            el.addEventListener('click', () => tryFindEndpoint(el));
        } else if (action === 'find-clear') {
            el.addEventListener('click', () => clearFindForm());
        } else if (action === 'triples-run') {
            el.addEventListener('click', () => tryTriplesEndpoint(el));
        } else if (action === 'triples-clear') {
            el.addEventListener('click', () => clearTriplesForm());
        } else if (action === 'graphiql-open') {
            el.addEventListener('click', () => openGraphiQL());
        } else if (action === 'graphql-run') {
            el.addEventListener('click', () => tryGraphqlQuery(el));
        } else if (action === 'graphql-schema') {
            el.addEventListener('click', () => tryGraphqlSchema(el));
        }
    });

    document.querySelectorAll('[data-try-endpoint]').forEach((btn) => {
        const path = btn.getAttribute('data-try-endpoint');
        if (!path) return;
        btn.addEventListener('click', () => tryApiEndpoint(path, btn));
    });
}

async function loadApiDomains() {
    const select = document.getElementById('apiDomainName');
    if (!select) return;

    let currentDomainSlug = '';
    try {
        const infoResp = await fetch('/domain/info', { credentials: 'same-origin' });
        const infoData = await infoResp.json();
        const name = infoData?.info?.name || '';
        currentDomainSlug = name ? name.toLowerCase().replace(/\s+/g, '_') : '';
    } catch (_) { /* ignore */ }

    try {
        const resp = await fetch('/settings/registry/domains', { credentials: 'same-origin' });
        const data = await resp.json();

        select.innerHTML = '<option value="">Select a domain...</option>';
        const domainRows = data.domains || data.projects || [];
        if (data.success && domainRows.length) {
            for (const p of domainRows) {
                const opt = document.createElement('option');
                opt.value = p.name;
                opt.textContent = p.name;
                if (p.name === currentDomainSlug) opt.selected = true;
                select.appendChild(opt);
            }
        }

        const chosen = select.value;
        if (chosen) {
            await loadApiVersions(chosen);
        }
    } catch (_) {
        select.innerHTML = '<option value="">Could not load domains</option>';
    }
}

async function loadApiVersions(domainSlug) {
    const select = document.getElementById('apiDomainVersion');
    if (!select) return;

    select.innerHTML = '<option value="">latest</option>';
    if (!domainSlug) return;

    try {
        const resp = await fetch(
            '/api/v1/domain/versions?domain_name=' + encodeURIComponent(domainSlug),
            { credentials: 'same-origin' }
        );
        const data = await resp.json();
        if (!data.success || !data.versions?.length) return;

        select.innerHTML = '<option value="">latest (v' + escHtml(data.latest_version) + ')</option>';
        for (const v of data.versions) {
            const opt = document.createElement('option');
            opt.value = v.version;
            opt.textContent = 'v' + v.version + (v.is_latest ? ' (latest)' : '');
            select.appendChild(opt);
        }
    } catch (_) {
        // keep the "latest" default
    }
}

function getApiDomainParam() {
    const val = document.getElementById('apiDomainName')?.value?.trim();
    return val ? 'domain_name=' + encodeURIComponent(val) : '';
}

function getApiVersionParam() {
    const val = document.getElementById('apiDomainVersion')?.value;
    return val ? 'domain_version=' + encodeURIComponent(val) : '';
}

function appendDomainParam(path) {
    const parts = [getApiDomainParam(), getApiVersionParam()].filter(Boolean);
    if (!parts.length) return path;
    const sep = path.includes('?') ? '&' : '?';
    return path + sep + parts.join('&');
}

/* ------------------------------------------------------------------ */
/* Expand / Collapse                                                   */
/* ------------------------------------------------------------------ */

function toggleApiCard(headerEl) {
    const card = headerEl.closest('.ob-api-card');
    const body = card.querySelector('.card-body');
    const isOpen = card.classList.toggle('open');
    body.classList.toggle('d-none', !isOpen);
}

/* ------------------------------------------------------------------ */
/* Copy base URL                                                       */
/* ------------------------------------------------------------------ */

function copyApiBaseUrl() {
    const url = document.getElementById('apiBaseUrl')?.textContent;
    if (!url) return;
    const btn = document.getElementById('api-copy-base-url-btn');
    navigator.clipboard.writeText(url).then(() => {
        if (btn) {
            const icon = btn.querySelector('i');
            if (icon) {
                icon.className = 'bi bi-check2';
                setTimeout(() => { icon.className = 'bi bi-clipboard'; }, 1500);
            }
        }
    });
}

/* ------------------------------------------------------------------ */
/* Try-it functionality                                                */
/* ------------------------------------------------------------------ */

async function tryApiEndpoint(path, btnEl) {
    const card = btnEl.closest('.card-body');
    const responseDiv = card.querySelector('.ob-api-response');
    if (!responseDiv) return;

    btnEl.disabled = true;
    btnEl.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Loading…';
    responseDiv.classList.remove('d-none');
    responseDiv.innerHTML = '';

    const fullPath = appendDomainParam(path);

    try {
        const t0 = performance.now();
        const resp = await fetch(fullPath);
        const elapsed = ((performance.now() - t0) / 1000).toFixed(2);
        const data = await resp.json();
        const jsonText = JSON.stringify(data, null, 2);

        const statusColor = resp.ok ? '#22c55e' : '#ef4444';
        responseDiv.innerHTML =
            `<div class="d-flex align-items-center gap-2 mb-1 small">` +
            `<span style="color:${statusColor};font-weight:600">${resp.status} ${resp.statusText}</span>` +
            `<span class="text-muted">${elapsed}s</span>` +
            `<button class="btn btn-sm btn-outline-secondary ms-auto ob-api-copy-btn" title="Copy to clipboard">` +
            `<i class="bi bi-clipboard"></i> Copy</button></div>` +
            `<pre>${escHtml(jsonText)}</pre>`;

        responseDiv.querySelector('.ob-api-copy-btn').addEventListener('click', function () {
            copyApiResult(this, jsonText);
        });
    } catch (err) {
        responseDiv.innerHTML =
            `<div class="text-danger small mb-1">Request failed</div>` +
            `<pre>${escHtml(String(err))}</pre>`;
    } finally {
        btnEl.disabled = false;
        btnEl.innerHTML = '<i class="bi bi-play-fill"></i> Try it';
    }
}

/* ------------------------------------------------------------------ */
/* Find & traverse form                                                */
/* ------------------------------------------------------------------ */

async function tryFindEndpoint(btnEl) {
    const params = new URLSearchParams();
    const fields = {
        entity_type: 'apiFindEntityType',
        search:      'apiFindSearch',
        depth:       'apiFindDepth',
        limit:       'apiFindLimit',
        offset:      'apiFindOffset',
    };

    for (const [key, id] of Object.entries(fields)) {
        const val = document.getElementById(id)?.value?.trim();
        if (val) params.set(key, val);
    }

    if (!params.has('entity_type') && !params.has('search')) {
        const card = btnEl.closest('.card-body');
        const responseDiv = card.querySelector('.ob-api-response');
        if (responseDiv) {
            responseDiv.classList.remove('d-none');
            responseDiv.innerHTML = '<div class="text-warning small">Provide at least Entity type or Search value</div>';
        }
        return;
    }

    const path = '/api/v1/digitaltwin/triples/find?' + params.toString();
    await tryApiEndpoint(path, btnEl);
    btnEl.innerHTML = '<i class="bi bi-play-fill"></i> Run search';
}

function clearFindForm() {
    ['apiFindEntityType', 'apiFindSearch'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.value = '';
    });
    const depthEl = document.getElementById('apiFindDepth');
    if (depthEl) depthEl.value = '1';
    const limitEl = document.getElementById('apiFindLimit');
    if (limitEl) limitEl.value = '1000';
    const offsetEl = document.getElementById('apiFindOffset');
    if (offsetEl) offsetEl.value = '0';
}

/* ------------------------------------------------------------------ */
/* Triples filter form                                                 */
/* ------------------------------------------------------------------ */

async function tryTriplesEndpoint(btnEl) {
    const params = new URLSearchParams();
    const fields = {
        subject:     'apiTripleSubject',
        predicate:   'apiTriplePredicate',
        object:      'apiTripleObject',
        entity_type: 'apiTripleEntityType',
        search:      'apiTripleSearch',
        limit:       'apiTripleLimit',
        offset:      'apiTripleOffset',
    };

    for (const [key, id] of Object.entries(fields)) {
        const val = document.getElementById(id)?.value?.trim();
        if (val) params.set(key, val);
    }

    const qs = params.toString();
    const path = '/api/v1/digitaltwin/triples' + (qs ? '?' + qs : '');
    await tryApiEndpoint(path, btnEl);
    btnEl.innerHTML = '<i class="bi bi-play-fill"></i> Run query';
}

function clearTriplesForm() {
    ['apiTripleSubject', 'apiTriplePredicate', 'apiTripleObject',
     'apiTripleEntityType', 'apiTripleSearch'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.value = '';
    });
    const limitEl = document.getElementById('apiTripleLimit');
    if (limitEl) limitEl.value = '100';
    const offsetEl = document.getElementById('apiTripleOffset');
    if (offsetEl) offsetEl.value = '0';
}

/* ------------------------------------------------------------------ */
/* GraphQL endpoints                                                   */
/* ------------------------------------------------------------------ */

function getGraphqlDomainSlug() {
    const val = document.getElementById('apiDomainName')?.value?.trim();
    return val || '';
}

function openGraphiQL() {
    const domainSlug = getGraphqlDomainSlug();
    if (!domainSlug) {
        showNotification('Enter a domain name first.', 'warning');
        return;
    }
    window.open(GRAPHQL_EXTERNAL_PREFIX + '/' + encodeURIComponent(domainSlug), '_blank');
}

async function tryGraphqlQuery(btnEl) {
    const domainSlug = getGraphqlDomainSlug();
    if (!domainSlug) {
        showNotification('Enter a domain name first.', 'warning');
        return;
    }

    const queryText = document.getElementById('apiGraphqlQuery')?.value?.trim();
    if (!queryText) {
        showNotification('Enter a GraphQL query.', 'warning');
        return;
    }

    const card = btnEl.closest('.card-body');
    const responseDiv = card.querySelector('.ob-api-response');
    if (!responseDiv) return;

    btnEl.disabled = true;
    btnEl.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Running…';
    responseDiv.classList.remove('d-none');
    responseDiv.innerHTML = '';

    const url = GRAPHQL_EXTERNAL_PREFIX + '/' + encodeURIComponent(domainSlug);

    try {
        const t0 = performance.now();
        const resp = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin',
            body: JSON.stringify({ query: queryText }),
        });
        const elapsed = ((performance.now() - t0) / 1000).toFixed(2);
        const data = await resp.json();
        const jsonText = JSON.stringify(data, null, 2);

        const statusColor = resp.ok ? '#22c55e' : '#ef4444';
        responseDiv.innerHTML =
            `<div class="d-flex align-items-center gap-2 mb-1 small">` +
            `<span style="color:${statusColor};font-weight:600">${resp.status} ${resp.statusText}</span>` +
            `<span class="text-muted">${elapsed}s</span>` +
            `<button class="btn btn-sm btn-outline-secondary ms-auto ob-api-copy-btn" title="Copy to clipboard">` +
            `<i class="bi bi-clipboard"></i> Copy</button></div>` +
            `<pre>${escHtml(jsonText)}</pre>`;

        responseDiv.querySelector('.ob-api-copy-btn').addEventListener('click', function () {
            copyApiResult(this, jsonText);
        });
    } catch (err) {
        responseDiv.innerHTML =
            `<div class="text-danger small mb-1">Request failed</div>` +
            `<pre>${escHtml(String(err))}</pre>`;
    } finally {
        btnEl.disabled = false;
        btnEl.innerHTML = '<i class="bi bi-play-fill"></i> Run query';
    }
}

async function tryGraphqlSchema(btnEl) {
    const domainSlug = getGraphqlDomainSlug();
    if (!domainSlug) {
        showNotification('Enter a domain name first.', 'warning');
        return;
    }
    const path = GRAPHQL_EXTERNAL_PREFIX + '/' + encodeURIComponent(domainSlug) + '/schema';

    const card = btnEl.closest('.card-body');
    const responseDiv = card.querySelector('.ob-api-response');
    if (!responseDiv) return;

    btnEl.disabled = true;
    btnEl.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Loading…';
    responseDiv.classList.remove('d-none');
    responseDiv.innerHTML = '';

    try {
        const t0 = performance.now();
        const resp = await fetch(path, { credentials: 'same-origin' });
        const elapsed = ((performance.now() - t0) / 1000).toFixed(2);
        const data = await resp.json();

        const statusColor = resp.ok ? '#22c55e' : '#ef4444';
        const sdl = data.sdl || JSON.stringify(data, null, 2);
        responseDiv.innerHTML =
            `<div class="d-flex align-items-center gap-2 mb-1 small">` +
            `<span style="color:${statusColor};font-weight:600">${resp.status} ${resp.statusText}</span>` +
            `<span class="text-muted">${elapsed}s</span>` +
            `<button class="btn btn-sm btn-outline-secondary ms-auto ob-api-copy-btn" title="Copy to clipboard">` +
            `<i class="bi bi-clipboard"></i> Copy</button></div>` +
            `<pre>${escHtml(sdl)}</pre>`;

        responseDiv.querySelector('.ob-api-copy-btn').addEventListener('click', function () {
            copyApiResult(this, sdl);
        });
    } catch (err) {
        responseDiv.innerHTML =
            `<div class="text-danger small mb-1">Request failed</div>` +
            `<pre>${escHtml(String(err))}</pre>`;
    } finally {
        btnEl.disabled = false;
        btnEl.innerHTML = '<i class="bi bi-play-fill"></i> View schema';
    }
}

/* ------------------------------------------------------------------ */
/* Utilities                                                           */
/* ------------------------------------------------------------------ */

function copyApiResult(btnEl, text) {
    navigator.clipboard.writeText(text).then(() => {
        const icon = btnEl.querySelector('i');
        const origHtml = btnEl.innerHTML;
        icon.className = 'bi bi-check2';
        btnEl.innerHTML = '<i class="bi bi-check2"></i> Copied';
        setTimeout(() => { btnEl.innerHTML = origHtml; }, 1500);
    });
}

function escHtml(s) {
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
}
