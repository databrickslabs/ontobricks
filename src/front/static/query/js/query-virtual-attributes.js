/**
 * Virtual attributes in the Graph Explorer node detail panel.
 *
 * A virtual attribute is not stored in the graph: a Unity Catalog function
 * computes it from the entity's ID. The declarations come with the node
 * context, so the section renders immediately with empty values and the user
 * decides when to pay for the warehouse round-trip.
 *
 * Computed values are cached per entity for the lifetime of the page, so
 * revisiting a node shows what was already computed instead of silently
 * recomputing it.
 */

// { "<entityUri>|<fullName>": { values: {...}, error: "", message: "" } }
const _vaCache = {};

function _vaCacheKey(entityUri, fullName) {
    return entityUri + '|' + fullName;
}

function _vaGroupFullName(group) {
    return group.fullName
        || [group.catalog, group.schema, group.function].filter(Boolean).join('.');
}

/**
 * Render the Virtual Attributes section body for a node.
 *
 * @param {string} entityUri Instance URI of the node.
 * @param {Array} groups Declarations from the node context / ontology class.
 * @returns {string} HTML for the section body.
 */
function renderVirtualAttributeSection(entityUri, groups) {
    const esc = (typeof escapeHtml === 'function') ? escapeHtml : function (s) { return String(s == null ? '' : s); };
    const safeUri = esc(entityUri).replace(/'/g, "\\'");
    let html = '';

    (groups || []).forEach(function (group, gIdx) {
        const fullName = _vaGroupFullName(group);
        const cached = _vaCache[_vaCacheKey(entityUri, fullName)] || null;
        const safeFullName = esc(fullName).replace(/'/g, "\\'");
        const attrs = group.attributes || [];

        html += '<div class="va-node-group" id="vaGroup-' + gIdx + '" data-va-function="' + esc(fullName) + '">';
        attrs.forEach(function (attr, aIdx) {
            const value = cached && cached.values ? cached.values[attr.name] : undefined;
            html += '<div class="entity-detail-item">' +
                '<span class="detail-key"><i class="bi bi-magic text-primary"></i> ' +
                esc(attr.label || attr.name) + '</span>' +
                '<span class="detail-value" id="vaValue-' + gIdx + '-' + aIdx + '">' +
                _vaFormatValue(value, esc) + '</span></div>';
        });
        html += '<div class="entity-detail-item"><button type="button" ' +
            'class="btn btn-sm btn-outline-primary w-100" id="vaBtn-' + gIdx + '" ' +
            'onclick="computeVirtualAttributes(\'' + safeUri + '\', \'' + safeFullName + '\', ' + gIdx + ')" ' +
            'title="' + esc(group.description || ('Compute using ' + fullName)) + '">' +
            '<i class="bi bi-calculator me-1"></i>' + (cached ? 'Recompute' : 'Compute') + '</button></div>';
        html += '<div class="va-node-status small" id="vaStatus-' + gIdx + '">' +
            _vaStatusHtml(cached, esc) + '</div>';
        html += '</div>';
    });

    if ((groups || []).length > 1) {
        html += '<div class="entity-detail-item"><button type="button" ' +
            'class="btn btn-sm btn-outline-secondary w-100" ' +
            'onclick="computeVirtualAttributes(\'' + safeUri + '\')" ' +
            'title="Run every virtual attribute function on this entity">' +
            '<i class="bi bi-calculator-fill me-1"></i>Compute all</button></div>';
    }
    return html;
}

function _vaFormatValue(value, esc) {
    if (value === undefined) return '<span class="text-muted">not computed</span>';
    if (value === null || value === '') return '<span class="text-muted">null</span>';
    return esc(String(value));
}

function _vaStatusHtml(entry, esc) {
    if (!entry) return '';
    if (entry.error) {
        return '<span class="text-danger"><i class="bi bi-exclamation-triangle me-1"></i>' +
            esc(entry.error) + '</span>';
    }
    if (entry.message) {
        return '<span class="text-warning">' + esc(entry.message) + '</span>';
    }
    return '';
}

/**
 * Compute one group's virtual attributes, or every group when *fullName* is
 * omitted, and patch the values into the open detail panel.
 *
 * @param {string} entityUri Instance URI of the node.
 * @param {string} [fullName] Restrict to this Unity Catalog function.
 * @param {number} [groupIndex] Index of the group's DOM block, for the button
 *     spinner. Omitted by "Compute all", which busies every button.
 */
async function computeVirtualAttributes(entityUri, fullName, groupIndex) {
    if (!entityUri) return;
    const targets = (groupIndex === undefined)
        ? Array.prototype.slice.call(document.querySelectorAll('[id^="vaBtn-"]'))
        : [document.getElementById('vaBtn-' + groupIndex)].filter(Boolean);
    targets.forEach(function (btn) {
        btn.disabled = true;
        btn.dataset.vaLabel = btn.innerHTML;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Computing…';
    });

    let url = '/dtwin/nodes/virtual-attributes?entity_uri=' + encodeURIComponent(entityUri);
    if (fullName) url += '&function=' + encodeURIComponent(fullName);

    try {
        const resp = await fetch(url, { credentials: 'same-origin' });
        const data = await resp.json();
        if (!resp.ok || !data.success) {
            throw new Error(data.message || data.detail || ('HTTP ' + resp.status));
        }
        _vaApplyResults(entityUri, data.virtual_attributes || []);
    } catch (err) {
        console.error('[VirtualAttributes] Computation failed:', err);
        if (typeof showNotification === 'function') {
            showNotification('Could not compute virtual attributes: ' + err.message, 'error', 5000);
        }
    } finally {
        targets.forEach(function (btn) {
            btn.disabled = false;
            btn.innerHTML = btn.dataset.vaLabel || '<i class="bi bi-calculator me-1"></i>Compute';
        });
    }
}

/**
 * Cache the computed groups and write them into the panel.
 *
 * Groups are located by their function name rather than by position: a
 * single-group computation returns one entry, which would otherwise land on
 * the first group's rows.
 */
function _vaApplyResults(entityUri, groups) {
    const esc = (typeof escapeHtml === 'function') ? escapeHtml : function (s) { return String(s == null ? '' : s); };
    let failures = 0;

    groups.forEach(function (group) {
        const fullName = _vaGroupFullName(group);
        _vaCache[_vaCacheKey(entityUri, fullName)] = {
            values: group.values || {},
            error: group.error || '',
            message: group.message || ''
        };
        if (group.error) failures += 1;

        const block = document.querySelector('[data-va-function="' + fullName.replace(/"/g, '\\"') + '"]');
        if (!block) return;
        const gIdx = block.id.replace('vaGroup-', '');
        (group.attributes || []).forEach(function (attr, aIdx) {
            const cell = document.getElementById('vaValue-' + gIdx + '-' + aIdx);
            if (cell) cell.innerHTML = _vaFormatValue((group.values || {})[attr.name], esc);
        });
        const status = document.getElementById('vaStatus-' + gIdx);
        if (status) status.innerHTML = _vaStatusHtml(_vaCache[_vaCacheKey(entityUri, fullName)], esc);
        const btn = document.getElementById('vaBtn-' + gIdx);
        if (btn) btn.dataset.vaLabel = '<i class="bi bi-calculator me-1"></i>Recompute';
    });

    if (typeof showNotification === 'function') {
        if (failures) {
            showNotification(
                failures + ' virtual attribute function' + (failures === 1 ? '' : 's') + ' failed',
                'warning', 4000
            );
        } else if (groups.length) {
            showNotification('Virtual attributes computed', 'success', 2000);
        }
    }
}

/**
 * Open a node's detail panel and compute all its virtual attributes.
 *
 * Entry point for the graph context menu: right-clicking a node does not open
 * the panel, so the values would have nowhere to land.
 */
async function computeVirtualAttributesForNode(entityUri) {
    if (!entityUri) return;
    if (typeof SigmaGraph !== 'undefined' && typeof SigmaGraph.selectEntity === 'function') {
        SigmaGraph.selectEntity(entityUri);
    }
    await _vaWaitForSection();
    return computeVirtualAttributes(entityUri);
}

/**
 * Resolve once the panel has rendered its Compute buttons, or after ~1s.
 * Giving up is safe: the results are cached, so the section picks them up on
 * its next render.
 */
function _vaWaitForSection() {
    return new Promise(function (resolve) {
        (function poll(attemptsLeft) {
            if (document.querySelector('[id^="vaBtn-"]') || attemptsLeft <= 0) return resolve();
            setTimeout(function () { poll(attemptsLeft - 1); }, 50);
        })(20);
    });
}

window.renderVirtualAttributeSection = renderVirtualAttributeSection;
window.computeVirtualAttributes = computeVirtualAttributes;
window.computeVirtualAttributesForNode = computeVirtualAttributesForNode;
