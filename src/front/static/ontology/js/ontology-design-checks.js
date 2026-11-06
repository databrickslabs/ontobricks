/**
 * OntoBricks — ontology-design-checks.js
 * Advisory ontology design lint for Ontology → Sanity Checks.
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
            const items = byRule[rule].map(function (finding) {
                return self._findingHtml(finding);
            }).join('');
            return '<div class="mb-3">' +
                '<div class="small fw-semibold mb-2">' + escapeHtml(title) +
                ' <span class="dc-finding-rule">' + escapeHtml(rule) + '</span></div>' +
                items + '</div>';
        }).join('');
        el.querySelectorAll('[data-dc-kind]').forEach(function (btn) {
            btn.addEventListener('click', function () {
                self._openTarget(btn.getAttribute('data-dc-kind'), btn.getAttribute('data-dc-name'));
            });
        });
    },

    _findingHtml: function (finding) {
        const name = finding.name || finding.uri || '(unnamed)';
        return '<div class="dc-finding">' +
            '<button type="button" class="dc-finding-link" data-dc-kind="' +
            escapeHtml(finding.kind || '') + '" data-dc-name="' +
            escapeHtml(finding.name || '') + '">' +
            '<div class="fw-semibold">' + escapeHtml(name) + '</div>' +
            '<div class="small text-muted">' + escapeHtml(finding.message || '') + '</div>' +
            '</button></div>';
    },

    _openTarget: function (kind, name) {
        if (!name) return;
        let section = 'entities';
        if (kind === 'property') section = 'relationships';
        else if (kind === 'group') section = 'groups';
        const params = new URLSearchParams({ section: section, select: name });
        window.location.href = '/ontology/?' + params.toString();
    },
};
