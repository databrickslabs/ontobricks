/**
 * OntoBricks - home.js
 *
 * Business-user home page:
 *   - KPI / health band for the current domain (entities, relationships,
 *     mappings, quality, status, version)
 *   - "All Domains" gateway grid (the open door to every domain)
 *   - demoted builder workflow status (Step 1/2/3 cards)
 */


/* ──────────────────────────────────────────────────────────────────────────
   Current-domain KPI band + workflow status
   ────────────────────────────────────────────────────────────────────────── */
async function loadHomeStatus() {
    try {
        const [infoResp, sessionResp] = await Promise.all([
            fetch('/domain/info', { credentials: 'same-origin' }),
            fetch('/session-status', { credentials: 'same-origin' }),
        ]);
        const info = await infoResp.json();
        const session = await sessionResp.json();

        const domainName = session.domain_name || info.name || 'NewDomain';
        const nameEl = document.getElementById('homeDomainName');
        if (nameEl) nameEl.textContent = domainName;

        const stats = (info && info.stats) || {};
        const meta = (info && info.info) || {};

        const entityCount = stats.entities != null
            ? stats.entities
            : (session.class_count || 0);
        const relationshipCount = stats.relationships != null
            ? stats.relationships
            : (session.property_count || 0);
        const mappingCount = (stats.entity_mappings || 0) + (stats.relationship_mappings || 0)
            || ((session.entities || 0) + (session.relationships || 0));

        renderKpis({
            entities: entityCount,
            relationships: relationshipCount,
            mappings: mappingCount,
            precision: info ? info.precision_score : null,
            status: meta.status || 'DRAFT',
            version: meta.version || session.version || '1',
        });
    } catch (error) {
        console.error('Error loading home status:', error);
    }
}

function renderKpis(kpi) {
    setKpiValue('kpiEntities', 'kpiEntitiesTile', kpi.entities, kpi.entities > 0);
    setKpiValue('kpiRelationships', 'kpiRelationshipsTile', kpi.relationships, kpi.relationships > 0);
    setKpiValue('kpiMappings', 'kpiMappingsTile', kpi.mappings, kpi.mappings > 0);
    renderQualityKpi(kpi.precision);
    renderStatusKpi(kpi.status);
    renderVersionKpi(kpi.version);
}

function setKpiValue(valueId, tileId, value, active) {
    const valueEl = document.getElementById(valueId);
    const tileEl = document.getElementById(tileId);
    if (valueEl) valueEl.textContent = value != null ? value : '-';
    if (tileEl) tileEl.className = 'ob-kpi-tile ' + (active ? 'tile-success' : 'tile-muted');
}

function renderQualityKpi(score) {
    const valueEl = document.getElementById('kpiQuality');
    const tileEl = document.getElementById('kpiQualityTile');
    if (!valueEl || !tileEl) return;

    if (score == null) {
        valueEl.textContent = '-';
        tileEl.className = 'ob-kpi-tile tile-muted';
        return;
    }
    valueEl.textContent = score;
    let variant = 'tile-danger';
    if (score >= 80) variant = 'tile-success';
    else if (score >= 50) variant = 'tile-warning';
    tileEl.className = 'ob-kpi-tile ' + variant;
}

function renderStatusKpi(status) {
    const valueEl = document.getElementById('kpiStatus');
    const tileEl = document.getElementById('kpiStatusTile');
    if (!valueEl || !tileEl) return;
    valueEl.innerHTML = statusBadge(status);
    tileEl.className = 'ob-kpi-tile tile-muted';
}

function renderVersionKpi(version) {
    const valueEl = document.getElementById('kpiVersion');
    const tileEl = document.getElementById('kpiVersionTile');
    if (!valueEl || !tileEl) return;
    valueEl.textContent = 'v' + version;
    tileEl.className = 'ob-kpi-tile tile-muted';
}

function statusBadge(status) {
    const map = {
        'DRAFT': 'bg-warning-subtle text-dark border-warning',
        'IN-REVIEW': 'bg-info-subtle text-dark border-info',
        'PUBLISHED': 'bg-success-subtle text-dark border-success',
    };
    const key = (status || 'DRAFT').toUpperCase();
    const cls = map[key] || map['DRAFT'];
    const label = key === 'IN-REVIEW'
        ? 'In Review'
        : (key.charAt(0) + key.slice(1).toLowerCase());
    return '<span class="badge border ' + cls + '">' + escapeHtml(label) + '</span>';
}

/* ──────────────────────────────────────────────────────────────────────────
   All Domains gateway — the open door
   ────────────────────────────────────────────────────────────────────────── */
async function loadDomainGateway() {
    const container = document.getElementById('domainGateway');
    if (!container) return;

    try {
        const [listResp, sessionResp] = await Promise.all([
            fetch('/domain/list-projects', { credentials: 'same-origin' }),
            fetch('/session-status', { credentials: 'same-origin' }),
        ]);
        const data = await listResp.json();
        const session = await sessionResp.json();
        const current = session.domain_name || '';

        const domains = (data && data.success && Array.isArray(data.domains))
            ? data.domains
            : [];

        if (!domains.length) {
            container.innerHTML =
                '<div class="ob-domain-grid-empty text-muted">' +
                '<div><i class="bi bi-folder2-open fs-3 d-block mb-2"></i>No domains yet.</div>' +
                '<button type="button" class="ob-domain-empty-cta" data-action="newDomain">' +
                '<i class="bi bi-file-earmark-plus"></i> Create your first domain</button>' +
                '</div>';
            return;
        }

        container.innerHTML = domains.map((name) => domainCard(name, name === current)).join('');

        container.querySelectorAll('.ob-domain-card[data-domain]').forEach((card) => {
            const domain = card.dataset.domain;
            const select = card.querySelector('.ob-domain-version');
            const openBtn = card.querySelector('.ob-domain-card-open');

            // Lazy-load versions on first hover / focus so we don't fire one
            // (slow) registry call per domain up front.
            const ensureVersions = () => loadVersionsInto(select, domain);
            card.addEventListener('mouseenter', ensureVersions);
            if (select) {
                select.addEventListener('focus', ensureVersions);
                select.addEventListener('mousedown', ensureVersions);
            }
            if (openBtn) {
                openBtn.addEventListener('click', () => {
                    openDomain(domain, select ? select.value : '');
                });
            }
        });
    } catch (err) {
        console.error('Error loading domain gateway:', err);
        container.innerHTML =
            '<div class="ob-domain-grid-empty text-muted">' +
            '<i class="bi bi-exclamation-triangle"></i> Could not load domains.</div>';
    }
}

function domainCard(name, isCurrent) {
    return '<div class="ob-domain-card' + (isCurrent ? ' is-current' : '') + '" ' +
        'data-domain="' + escapeAttr(name) + '">' +
        '<div class="ob-domain-card-top">' +
        '<span class="ob-domain-card-icon"><i class="bi bi-diagram-3"></i></span>' +
        '<span class="ob-domain-card-name">' + escapeHtml(name) + '</span>' +
        (isCurrent ? '<span class="ob-domain-card-current-badge">Current</span>' : '') +
        '</div>' +
        '<div class="ob-domain-card-footer">' +
        '<select class="ob-domain-version form-select form-select-sm" ' +
        'aria-label="Version for ' + escapeAttr(name) + '">' +
        '<option value="">Latest</option>' +
        '</select>' +
        '<button type="button" class="ob-domain-card-open" title="Open this domain">' +
        'Open <i class="bi bi-arrow-right"></i></button>' +
        '</div>' +
        '</div>';
}

async function loadVersionsInto(select, domain) {
    if (!select || select.dataset.loaded === '1' || select.dataset.loading === '1') return;
    select.dataset.loading = '1';
    const fallback = select.innerHTML;
    select.innerHTML = '<option value="">Loading…</option>';
    try {
        const resp = await fetch(
            '/domain/list-versions?domain_name=' + encodeURIComponent(domain),
            { credentials: 'same-origin' }
        );
        const data = await resp.json();
        const versions = (data && data.success && Array.isArray(data.versions)) ? data.versions : [];
        if (versions.length) {
            const statuses = data.version_status || {};
            const sorted = versions.slice().sort(
                (a, b) => b.localeCompare(a, undefined, { numeric: true })
            );
            select.innerHTML = sorted.map((v) =>
                '<option value="' + escapeAttr(v) + '">v' + escapeHtml(v) +
                ' — ' + escapeHtml(versionStatusLabel(statuses[v])) + '</option>'
            ).join('');
            select.value = sorted[0];
            select.dataset.loaded = '1';
        } else {
            select.innerHTML = fallback;
        }
    } catch (err) {
        console.error('Error loading versions for', domain, err);
        select.innerHTML = fallback;
    } finally {
        select.dataset.loading = '';
    }
}

function versionStatusLabel(status) {
    switch ((status || 'DRAFT').toUpperCase()) {
        case 'PUBLISHED': return 'Published';
        case 'IN-REVIEW': return 'In Review';
        default: return 'Draft';
    }
}

async function openDomain(name, version) {
    if (!name) return;
    try {
        const verLabel = version ? ' v' + version : '';
        showNotification('Opening ' + name + verLabel + '…', 'info', 4000);
        const body = { domain: name };
        if (version) body.version = version;
        const resp = await fetch('/domain/load-from-uc', {
            method: 'POST',
            credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        const data = await resp.json();
        if (!data.success) {
            showNotification('Error: ' + (data.message || 'Failed to open domain'), 'error');
            return;
        }
        if (typeof invalidateDomainCaches === 'function') invalidateDomainCaches();
        window.location.href = '/domain/';
    } catch (err) {
        showNotification('Error opening domain: ' + err.message, 'error');
    }
}

/* ──────────────────────────────────────────────────────────────────────────
   Domain management actions
   ────────────────────────────────────────────────────────────────────────── */
async function newDomain() {
    const confirmed = await showConfirmDialog({
        title: 'New Domain',
        message: 'Create a new domain? This will clear all current ontology and mapping data.',
        confirmText: 'Create New',
        confirmClass: 'btn-warning',
        icon: 'file-earmark-plus'
    });
    if (!confirmed) return;

    try {
        showDomainStatus('Creating new domain...', 'info');
        const response = await fetch('/reset-session', { method: 'POST', credentials: 'same-origin' });
        const result = await response.json();

        if (result.success) {
            showDomainStatus('New domain created', 'success');
            if (typeof invalidateDomainCaches === 'function') invalidateDomainCaches();
            setTimeout(() => window.location.reload(), 1000);
        } else {
            showDomainStatus('Error: ' + result.message, 'error');
        }
    } catch (error) {
        showDomainStatus('Error: ' + error.message, 'error');
    }
}

function showDomainStatus(message, type) {
    const statusEl = document.getElementById('domainStatus');
    if (!statusEl) return;
    statusEl.className = 'domain-status ' + type;
    statusEl.innerHTML = `<i class="bi bi-${type === 'success' ? 'check-circle' : type === 'error' ? 'x-circle' : type === 'warning' ? 'exclamation-triangle' : 'hourglass-split'}"></i> ${message}`;
    statusEl.classList.remove('hidden-initial');

    if (type === 'success') {
        setTimeout(() => statusEl.classList.add('hidden-initial'), 5000);
    }
}

/* ──────────────────────────────────────────────────────────────────────────
   Init + event wiring
   ────────────────────────────────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', function () {
    loadHomeStatus();
    loadDomainGateway();
    initHomeActionButtons();
});

/**
 * Delegate home `data-action` clicks from the document root. The only remaining
 * action is the gateway empty-state "Create your first domain" CTA.
 */
function initHomeActionButtons() {
    document.addEventListener('click', onHomePanelClick);
}

function onHomePanelClick(e) {
    const el = e.target.closest('[data-action]');
    if (!el) return;
    if (el.getAttribute('data-action') === 'newDomain') {
        e.preventDefault();
        newDomain();
    }
}

function escapeHtml(text) {
    if (text == null) return '';
    const div = document.createElement('div');
    div.textContent = String(text);
    return div.innerHTML;
}

function escapeAttr(text) {
    return escapeHtml(text).replace(/"/g, '&quot;');
}
