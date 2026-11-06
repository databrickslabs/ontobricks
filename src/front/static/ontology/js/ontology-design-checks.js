/**
 * OntoBricks — ontology-design-checks.js
 * Advisory ontology design lint for Ontology → Sanity Checks.
 * Auto-fix: propose → confirm modal → apply.
 */
window.DesignChecksModule = {
    RULE_TITLES: {
        'DC-E01': 'Broken parent',
        'DC-E02': 'Broken domain',
        'DC-E03': 'Broken range',
        'DC-E04': 'Broken group member',
        'DC-E05': 'Hierarchy cycle',
        'DC-E06': 'Duplicate URI',
        'DC-W01': 'Orphan class',
        'DC-W02': 'Missing label',
        'DC-W03': 'Missing domain',
        'DC-W04': 'Missing range',
        'DC-W05': 'Duplicate name',
        'DC-I01': 'Missing description',
        'DC-I02': 'Class naming',
        'DC-I03': 'Property naming',
        'DC-I04': 'Near-duplicate name',
        'DC-I05': 'Deep hierarchy',
    },

    // Mirror of backend RULE_PLANNER
    AUTOFIX_RULES: {
        'DC-E01': 'rule', 'DC-E02': 'rule', 'DC-E03': 'rule',
        'DC-E04': 'rule', 'DC-E05': 'rule', 'DC-E06': 'none',
        'DC-W01': 'ai', 'DC-W02': 'ai', 'DC-W03': 'ai',
        'DC-W04': 'ai', 'DC-W05': 'ai',
        'DC-I01': 'ai', 'DC-I02': 'rule', 'DC-I03': 'rule',
        'DC-I04': 'ai', 'DC-I05': 'none',
    },

    // AI rules that still offer a No-LLM fallback
    AUTOFIX_FALLBACK_RULES: { 'DC-W02': true, 'DC-W03': true },

    _lastLlmAvailable: null,
    _pendingPlan: null,

    init: function () {
        this.refresh();
    },

    refresh: async function () {
        const lists = {
            error: document.getElementById('dcListError'),
            warning: document.getElementById('dcListWarning'),
            info: document.getElementById('dcListInfo'),
        };
        Object.values(lists).forEach(function (el) {
            if (el) {
                el.innerHTML = '<div class="dc-empty">Running checks…</div>';
            }
        });
        try {
            const resp = await fetch('/ontology/design-checks', { credentials: 'same-origin' });
            const data = await resp.json();
            if (!resp.ok || data.success === false) {
                throw new Error((data && data.detail) || 'Design checks failed');
            }
            this._render(data);
        } catch (err) {
            Object.values(lists).forEach(function (el) {
                if (el) {
                    el.innerHTML = '<div class="dc-empty">Could not load design checks.</div>';
                }
            });
        }
    },

    _render: function (data) {
        const summary = data.summary || { error: 0, warning: 0, info: 0 };
        const setBadge = function (id, count, label) {
            const el = document.getElementById(id);
            if (el) el.textContent = count + ' ' + label;
        };
        setBadge('dcBadgeError', summary.error || 0, 'errors');
        setBadge('dcBadgeWarning', summary.warning || 0, 'warnings');
        setBadge('dcBadgeInfo', summary.info || 0, 'info');

        const grouped = { error: [], warning: [], info: [] };
        (data.findings || []).forEach(function (finding) {
            const bucket = grouped[finding.severity];
            if (bucket) bucket.push(finding);
        });
        this._fillList('dcListError', grouped.error);
        this._fillList('dcListWarning', grouped.warning);
        this._fillList('dcListInfo', grouped.info);
    },

    _canEdit: function () {
        return !(window.OB && typeof window.OB.canEditOntology === 'function'
            && !window.OB.canEditOntology());
    },

    _llmAvailable: function () {
        if (this._lastLlmAvailable !== null) return this._lastLlmAvailable;
        try {
            const ep = (window.OB && window.OB.domainInfo && window.OB.domainInfo.llm_endpoint)
                || (window.NavbarState && window.NavbarState.llmEndpoint)
                || '';
            return !!(ep && String(ep).trim());
        } catch (e) {
            return false;
        }
    },

    _canAutofix: function (rule) {
        const planner = this.AUTOFIX_RULES[rule];
        if (!planner || planner === 'none') return false;
        if (!this._canEdit()) return false;
        if (planner === 'rule') return true;
        if (this._llmAvailable()) return true;
        return !!this.AUTOFIX_FALLBACK_RULES[rule];
    },

    _fillList: function (listId, findings) {
        const el = document.getElementById(listId);
        if (!el) return;
        if (!findings.length) {
            el.innerHTML = '<div class="dc-empty">No findings in this category.</div>';
            return;
        }
        const byRule = {};
        findings.forEach(function (finding) {
            const key = finding.rule || 'unknown';
            if (!byRule[key]) byRule[key] = [];
            byRule[key].push(finding);
        });
        const self = this;
        el.innerHTML = Object.keys(byRule).map(function (rule) {
            const title = self.RULE_TITLES[rule] || rule;
            const items = byRule[rule];
            const fixable = items.filter(function (f) {
                return self._canAutofix(f.rule);
            });
            const fixAllBtn = (fixable.length >= 2)
                ? '<button type="button" class="btn btn-sm btn-outline-primary ms-2" data-dc-fix-all="' +
                  escapeHtml(rule) + '">Fix all (' + fixable.length + ')</button>'
                : '';
            const rows = items.map(function (finding) {
                return self._findingHtml(finding);
            }).join('');
            return '<div class="mb-3">' +
                '<div class="small fw-semibold mb-2 d-flex align-items-center flex-wrap">' +
                '<span>' + escapeHtml(title) +
                ' <span class="dc-finding-rule">' + escapeHtml(rule) + '</span></span>' +
                fixAllBtn + '</div>' + rows + '</div>';
        }).join('');

        el.querySelectorAll('[data-dc-kind]').forEach(function (btn) {
            btn.addEventListener('click', function () {
                self._openTarget(
                    btn.getAttribute('data-dc-kind'),
                    btn.getAttribute('data-dc-name'),
                    btn.getAttribute('data-dc-domain')
                );
            });
        });
        el.querySelectorAll('[data-dc-autofix]').forEach(function (btn) {
            btn.addEventListener('click', function () {
                const finding = {
                    rule: btn.getAttribute('data-dc-rule') || '',
                    kind: btn.getAttribute('data-dc-kind-af') || '',
                    name: btn.getAttribute('data-dc-name-af') || '',
                    domain: btn.getAttribute('data-dc-domain-af') || '',
                    member: btn.getAttribute('data-dc-member') || '',
                };
                if (!finding.domain) delete finding.domain;
                if (!finding.member) delete finding.member;
                self._openAutofixModal([finding]);
            });
        });
        el.querySelectorAll('[data-dc-fix-all]').forEach(function (btn) {
            btn.addEventListener('click', function () {
                const rule = btn.getAttribute('data-dc-fix-all');
                const batch = (byRule[rule] || []).filter(function (f) {
                    return self._canAutofix(f.rule);
                });
                self._openAutofixModal(batch);
            });
        });
    },

    _findingHtml: function (finding) {
        const name = finding.name || finding.uri || '(unnamed)';
        const canFix = this._canAutofix(finding.rule);
        const autofixBtn = canFix
            ? '<button type="button" class="btn btn-sm btn-outline-primary dc-autofix-btn" ' +
              'data-dc-autofix="1" data-dc-rule="' + escapeHtml(finding.rule || '') + '" ' +
              'data-dc-kind-af="' + escapeHtml(finding.kind || '') + '" ' +
              'data-dc-name-af="' + escapeHtml(finding.name || '') + '" ' +
              'data-dc-domain-af="' + escapeHtml(finding.domain || '') + '" ' +
              'data-dc-member="' + escapeHtml(finding.member || '') + '">' +
              '<i class="bi bi-magic me-1"></i>Auto-fix</button>'
            : '';
        return '<div class="dc-finding d-flex align-items-start gap-2">' +
            '<button type="button" class="dc-finding-link flex-grow-1" data-dc-kind="' +
            escapeHtml(finding.kind || '') + '" data-dc-name="' +
            escapeHtml(finding.name || '') + '" data-dc-domain="' +
            escapeHtml(finding.domain || '') + '">' +
            '<div class="fw-semibold">' + escapeHtml(name) + '</div>' +
            '<div class="small text-muted">' + escapeHtml(finding.message || '') + '</div>' +
            '</button>' + autofixBtn + '</div>';
    },

    _openTarget: function (kind, name, domain) {
        // DatatypeProperties are attributes on entities, not Relationships.
        if (kind === 'attribute') {
            if (domain) {
                const params = new URLSearchParams({ section: 'entities', select: domain });
                window.location.href = '/ontology/?' + params.toString();
                return;
            }
            if (this._canEdit()) {
                showNotification(
                    '"' + name + '" is not attached to any entity. Use Auto-fix to attach or remove it.',
                    'warning'
                );
            } else {
                showNotification('"' + name + '" is not attached to any entity.', 'warning');
            }
            return;
        }
        if (!name) return;
        let section = 'entities';
        if (kind === 'property') section = 'relationships';
        else if (kind === 'group') section = 'groups';
        const params = new URLSearchParams({ section: section, select: name });
        window.location.href = '/ontology/?' + params.toString();
    },

    _openAutofixModal: async function (findings) {
        if (!findings || !findings.length) return;
        if (!this._canEdit()) {
            showNotification('Ontology is read-only', 'warning');
            return;
        }
        const rule = findings[0].rule;
        const title = this.RULE_TITLES[rule] || rule;
        this._showModalShell(title, 'Preparing Auto-fix…', true);
        try {
            const resp = await fetch('/ontology/design-checks/autofix/propose', {
                method: 'POST',
                credentials: 'same-origin',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ findings: findings }),
            });
            const data = await resp.json();
            this._lastLlmAvailable = !!data.llm_available;
            if (!resp.ok || data.success === false) {
                this._showModalShell(
                    title,
                    '<div class="text-danger">' +
                    escapeHtml((data && data.error) || 'Could not propose a fix.') +
                    '</div>',
                    false,
                    true
                );
                return;
            }
            this._pendingPlan = data.plan || {};
            this._renderPlanModal(title, data.plan || {}, !!data.llm_available);
        } catch (err) {
            this._showModalShell(
                title,
                '<div class="text-danger">Could not propose a fix.</div>',
                false,
                true
            );
        }
    },

    _showModalShell: function (title, bodyHtml, busy, hideConfirm) {
        const modalId = 'dcAutofixModal';
        let modalEl = document.getElementById(modalId);
        if (!modalEl) {
            document.body.insertAdjacentHTML('beforeend',
                '<div class="modal fade" id="' + modalId + '" tabindex="-1">' +
                '<div class="modal-dialog modal-dialog-centered modal-lg">' +
                '<div class="modal-content">' +
                '<div class="modal-header">' +
                '<h5 class="modal-title" id="' + modalId + '_title"></h5>' +
                '<button type="button" class="btn-close" data-bs-dismiss="modal"></button>' +
                '</div>' +
                '<div class="modal-body" id="' + modalId + '_body"></div>' +
                '<div class="modal-footer">' +
                '<button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>' +
                '<button type="button" class="btn btn-primary" id="' + modalId + '_confirm">Confirm</button>' +
                '</div></div></div></div>');
            modalEl = document.getElementById(modalId);
            modalEl.addEventListener('hidden.bs.modal', function () {
                // keep element for reuse
            });
        }
        document.getElementById(modalId + '_title').innerHTML =
            '<i class="bi bi-magic me-2"></i>Auto-fix — ' + escapeHtml(title);
        document.getElementById(modalId + '_body').innerHTML = bodyHtml;
        const confirmBtn = document.getElementById(modalId + '_confirm');
        confirmBtn.disabled = !!busy || !!hideConfirm;
        confirmBtn.style.display = hideConfirm ? 'none' : '';
        confirmBtn.onclick = null;
        bootstrap.Modal.getOrCreateInstance(modalEl).show();
    },

    _entityOptionsHtml: function (selected) {
        const classes = ((typeof OntologyState !== 'undefined' && OntologyState.config
            && OntologyState.config.classes) || []).filter(function (c) { return c.name; });
        return classes.map(function (c) {
            const label = c.label && c.label !== c.name ? (c.label + ' (' + c.name + ')') : c.name;
            const sel = c.name === selected ? ' selected' : '';
            return '<option value="' + escapeHtml(c.name) + '"' + sel + '>' +
                escapeHtml(label) + '</option>';
        }).join('');
    },

    _renderPlanModal: function (title, plan, llmAvailable) {
        const self = this;
        const badge = llmAvailable
            ? ''
            : ' <span class="badge text-bg-secondary">No LLM</span>';
        const summary = '<p class="mb-3">' + escapeHtml(plan.summary || '') + badge + '</p>';
        const ops = plan.operations || [];
        const alts = plan.alternatives || [];
        const byGroup = {};
        const independent = [];

        function bucket(op, isAlt) {
            if (op.group) {
                if (!byGroup[op.group]) byGroup[op.group] = [];
                byGroup[op.group].push({ op: op, isAlt: !!isAlt });
            } else if (!isAlt) {
                independent.push(op);
            } else {
                // alternatives without group: show as optional radios under a synthetic group
                const g = 'alt-' + (op.finding_id || op.id || independent.length);
                if (!byGroup[g]) byGroup[g] = [];
                byGroup[g].push({ op: op, isAlt: true });
            }
        }
        ops.forEach(function (op) { bucket(op, false); });
        alts.forEach(function (op) { bucket(op, true); });

        // Move grouped primary ops into byGroup if they share a group
        Object.keys(byGroup).forEach(function (g) {
            // ensure selected default among group
            const rows = byGroup[g];
            if (!rows.some(function (r) { return r.op.selected; }) && rows.length) {
                rows[0].op.selected = true;
            }
        });

        let html = summary;
        Object.keys(byGroup).forEach(function (g) {
            const rows = byGroup[g];
            html += '<div class="mb-3 dc-autofix-group" data-group="' + escapeHtml(g) + '">';
            rows.forEach(function (row) {
                const op = row.op;
                const id = 'dc_af_' + escapeHtml(op.id || g);
                const checked = op.selected ? ' checked' : '';
                html += '<div class="form-check mb-2">' +
                    '<input class="form-check-input" type="radio" name="' + escapeHtml(g) +
                    '" id="' + id + '" data-op-id="' + escapeHtml(op.id || '') + '"' + checked + '>' +
                    '<label class="form-check-label" for="' + id + '">' +
                    '<strong>' + escapeHtml(op.op) + '</strong> — ' +
                    escapeHtml(op.rationale || '') +
                    self._opParamsHtml(op, plan) +
                    '</label></div>';
            });
            html += '</div>';
        });

        independent.forEach(function (op) {
            const id = 'dc_af_' + escapeHtml(op.id || op.op);
            const checked = op.selected === false ? '' : ' checked';
            html += '<div class="form-check mb-2">' +
                '<input class="form-check-input" type="checkbox" id="' + id +
                '" data-op-id="' + escapeHtml(op.id || '') + '"' + checked + '>' +
                '<label class="form-check-label" for="' + id + '">' +
                '<strong>' + escapeHtml(op.op) + '</strong> — ' +
                escapeHtml(op.rationale || '') +
                self._opParamsHtml(op, plan) +
                '</label></div>';
        });

        this._showModalShell(title, html || '<p class="text-muted">No operations.</p>', false, false);
        const confirmBtn = document.getElementById('dcAutofixModal_confirm');
        confirmBtn.onclick = function () {
            self._confirmAutofix(plan);
        };
    },

    _opParamsHtml: function (op, plan) {
        if (op.op === 'attach_attribute' || (plan && plan.needs_entity_picker && op.op === 'attach_attribute')) {
            const selected = (op.params && op.params.entity) || '';
            return '<div class="mt-1">' +
                '<select class="form-select form-select-sm dc-af-entity" data-op-id="' +
                escapeHtml(op.id || '') + '">' +
                '<option value="">Choose an entity…</option>' +
                this._entityOptionsHtml(selected) +
                '</select></div>';
        }
        if (op.params && Object.keys(op.params).length) {
            const bits = Object.keys(op.params).map(function (k) {
                return k + '=' + String(op.params[k]);
            }).join(', ');
            return '<div class="small text-muted">' + escapeHtml(bits) + '</div>';
        }
        return '';
    },

    _collectSelectedOps: function (plan) {
        const body = document.getElementById('dcAutofixModal_body');
        if (!body) return [];
        const all = [].concat(plan.operations || [], plan.alternatives || []);
        const byId = {};
        all.forEach(function (op) { if (op.id) byId[op.id] = op; });

        const selected = [];
        body.querySelectorAll('input[type="radio"]:checked, input[type="checkbox"]:checked').forEach(function (input) {
            const id = input.getAttribute('data-op-id');
            const op = byId[id];
            if (!op) return;
            const copy = JSON.parse(JSON.stringify(op));
            const sel = body.querySelector('select.dc-af-entity[data-op-id="' + id + '"]');
            if (sel && copy.op === 'attach_attribute') {
                copy.params = copy.params || {};
                copy.params.entity = sel.value || '';
            }
            selected.push(copy);
        });
        return selected;
    },

    _confirmAutofix: async function (plan) {
        const ops = this._collectSelectedOps(plan);
        if (!ops.length) {
            showNotification('Select at least one change', 'warning');
            return;
        }
        for (let i = 0; i < ops.length; i++) {
            if (ops[i].op === 'attach_attribute' && !(ops[i].params && ops[i].params.entity)) {
                showNotification('Choose an entity for attach', 'warning');
                return;
            }
        }
        const confirmBtn = document.getElementById('dcAutofixModal_confirm');
        if (confirmBtn) confirmBtn.disabled = true;
        try {
            const resp = await fetch('/ontology/design-checks/autofix/apply', {
                method: 'POST',
                credentials: 'same-origin',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ operations: ops }),
            });
            const data = await resp.json();
            if (!resp.ok || data.success === false) {
                showNotification((data && data.error) || 'Auto-fix failed', 'danger');
                if (confirmBtn) confirmBtn.disabled = false;
                return;
            }
            if (typeof window.applyStoredUris === 'function') {
                window.applyStoredUris(data);
            }
            // Refresh local OntologyState from session if a helper exists
            if (typeof window.loadConfigFromSession === 'function') {
                try { await window.loadConfigFromSession(); } catch (e) { /* ignore */ }
            }
            if (typeof autoGenerateOwl === 'function') {
                try { await autoGenerateOwl(); } catch (e) { /* ignore */ }
            }
            showNotification(
                'Applied ' + (data.applied || ops.length) + ' Auto-fix change(s)',
                'success',
                2500
            );
            const modalEl = document.getElementById('dcAutofixModal');
            if (modalEl) bootstrap.Modal.getOrCreateInstance(modalEl).hide();
            await this.refresh();
        } catch (err) {
            showNotification('Auto-fix failed', 'danger');
            if (confirmBtn) confirmBtn.disabled = false;
        }
    },
};
