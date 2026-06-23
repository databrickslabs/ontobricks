/*
 * PGE Run-Visualizer
 * -------------------
 * Renders the Planner → Generator → Evaluator → Critic loop from a completed
 * auto-map task result. Consumes the PGE artifacts surfaced on `task.result`:
 *   - pge_scorecard        : intrinsic-eval scorecard (verdict + gate tiers + metrics)
 *   - source_model         : the Planner's output (table roles, canonical ids, joins, plan)
 *   - mapping_run_log[]     : per-item attempt-by-attempt trace
 *   - mapping_evaluations{} : per-item final EvalReport (metrics + failures)
 *
 * Entirely defensive — any field may be missing (legacy engine, partial run).
 * If there is nothing PGE-specific to show, render() hides the container.
 *
 * Public API:  PgeVisualizer.render(taskResult, containerId)
 */
const PgeVisualizer = (function () {
    'use strict';

    // ---- small helpers -------------------------------------------------
    function esc(s) {
        if (s === null || s === undefined) return '';
        return String(s)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }

    function humanize(key) {
        return String(key)
            .replace(/_/g, ' ')
            .replace(/\bpct\b/gi, '%')
            .replace(/\b(\w)/g, (m) => m.toUpperCase());
    }

    // Format a known ratio metric as a percentage (so an exact 1.0 reads
    // "100%", not "1" — JS treats 1.0 as an integer, so fmtMetric alone can't).
    function fmtRatio(v) {
        if (typeof v !== 'number') return fmtMetric(v);
        return (v * 100).toFixed(1).replace(/\.0$/, '') + '%';
    }

    // Format a metric value: floats in [0,1] become percentages.
    function fmtMetric(v) {
        if (typeof v === 'number') {
            if (Number.isInteger(v)) return String(v);
            if (v >= 0 && v <= 1) return (v * 100).toFixed(1) + '%';
            return v.toFixed(3);
        }
        if (Array.isArray(v)) return v.length ? v.join(', ') : '—';
        if (v === null || v === undefined) return '—';
        return esc(v);
    }

    function shortUri(uri) {
        if (!uri) return '';
        const s = String(uri);
        const hashIdx = s.lastIndexOf('#');
        const slashIdx = s.lastIndexOf('/');
        const cut = Math.max(hashIdx, slashIdx);
        return cut >= 0 && cut < s.length - 1 ? s.slice(cut + 1) : s;
    }

    function statusBadge(status) {
        const map = {
            PASS: ['bg-success', 'check-circle-fill', 'Pass'],
            PRESEEDED: ['bg-info', 'bookmark-check-fill', 'Pre-seeded'],
            SKIPPED: ['bg-secondary', 'dash-circle-fill', 'Skipped'],
            FAIL_BUDGET: ['bg-danger', 'x-circle-fill', 'Failed (budget)'],
            FAIL_BUBBLE: ['bg-danger', 'x-circle-fill', 'Failed (re-plan)'],
        };
        const [cls, icon, label] = map[status] || ['bg-secondary', 'question-circle', status || '?'];
        return `<span class="badge ${cls}"><i class="bi bi-${icon} me-1"></i>${esc(label)}</span>`;
    }

    // ---- scorecard: verdict pill (goes inside the header) --------------
    function renderVerdictPill(sc) {
        const verdict = (sc && sc.verdict) || 'N/A';
        const vClass = verdict === 'GREEN' ? 'ob-pge-verdict-green'
            : verdict === 'RED' ? 'ob-pge-verdict-red' : 'ob-pge-verdict-na';
        const vIcon = verdict === 'GREEN' ? 'shield-check'
            : verdict === 'RED' ? 'shield-exclamation' : 'shield';
        const label = sc ? verdict : 'no scorecard';
        return `<span class="ob-pge-verdict ${vClass}"><i class="bi bi-${vIcon} me-1"></i>${esc(label)}</span>`;
    }

    // ---- scorecard: KPI strip + gate tiers (go below the header) -------
    function renderScorecardBody(sc) {
        if (!sc) return '';
        // KPI chips from the mapping stage (the most demo-relevant metrics).
        const mapMetrics = (sc.stages && sc.stages.mapping && sc.stages.mapping.metrics) || {};
        // ratio metrics render as %, count metrics as integers.
        const kpiKeys = [
            { k: 'entity_completeness', ratio: true },
            { k: 'relationship_completeness', ratio: true },
            { k: 'id_integrity', ratio: true },
            { k: 'sql_exec_failures', ratio: false },
        ];
        let kpis = kpiKeys
            .filter((spec) => spec.k in mapMetrics)
            .map((spec) => `
                <div class="ob-pge-kpi">
                    <div class="ob-pge-kpi-val">${spec.ratio ? fmtRatio(mapMetrics[spec.k]) : fmtMetric(mapMetrics[spec.k])}</div>
                    <div class="ob-pge-kpi-lbl">${esc(humanize(spec.k))}</div>
                </div>`).join('');
        // Pipeline coverage-loss is the anti-circularity metric — show it if present.
        const pipe = (sc.stages && sc.stages.pipeline) || {};
        const pipeMetrics = pipe.metrics || pipe;
        if (pipeMetrics && 'coverage_loss' in pipeMetrics) {
            kpis += `
                <div class="ob-pge-kpi">
                    <div class="ob-pge-kpi-val">${fmtRatio(pipeMetrics.coverage_loss)}</div>
                    <div class="ob-pge-kpi-lbl">Coverage Loss</div>
                </div>`;
        }

        const gates = sc.gates || {};
        function gateChip(label, tier) {
            if (!tier) return '';
            const pass = tier.passed;
            const detail = (tier.failures || tier.regressions || tier.warnings || []);
            const cls = pass ? 'ob-pge-gate-pass' : 'ob-pge-gate-fail';
            const icon = pass ? 'check-lg' : 'exclamation-triangle-fill';
            const title = detail.length ? esc(detail.map((d) => (typeof d === 'string' ? d : (d.check || d.metric || JSON.stringify(d)))).join(' · ')) : '';
            return `<span class="ob-pge-gate ${cls}" title="${title}">
                <i class="bi bi-${icon}"></i>${esc(label)}${detail.length ? ` (${detail.length})` : ''}</span>`;
        }
        const gatesHtml = `
            <div class="ob-pge-gates">
                ${gateChip('Tier 1 · absolute', gates.tier1_absolute)}
                ${gateChip('Tier 2 · ratio', gates.tier2_ratio)}
                ${gateChip('Tier 3 · regression', gates.tier3_regression)}
            </div>`;

        return `
            ${kpis ? `<div class="ob-pge-kpis">${kpis}</div>` : ''}
            ${gatesHtml}`;
    }

    // ---- planner source-model panel ------------------------------------
    function renderSourceModel(sm) {
        if (!sm) return '<p class="text-muted small mb-0">No planner source-model captured.</p>';
        let html = '';

        const roles = sm.table_roles || [];
        if (roles.length) {
            html += '<h6 class="mt-1">Table → class candidates</h6>';
            html += '<table class="table table-sm ob-pge-sm-table"><tbody>';
            roles.forEach((r) => {
                const cands = (r.ontology_class_candidates || []).map((c) => {
                    const conf = typeof c.confidence === 'number' ? c.confidence : 0;
                    const w = Math.max(6, Math.round(conf * 40));
                    return `<div title="${esc(c.reason || '')}">
                        <span class="ob-pge-conf-bar" style="width:${w}px"></span>
                        <code>${esc(shortUri(c.uri))}</code>
                        <span class="text-muted small">${(conf * 100).toFixed(0)}%</span></div>`;
                }).join('');
                html += `<tr><td class="text-nowrap"><code>${esc(r.table)}</code></td><td>${cands || '<span class="text-muted">—</span>'}</td></tr>`;
            });
            html += '</tbody></table>';
        }

        const cids = sm.canonical_ids || [];
        if (cids.length) {
            html += '<h6 class="mt-2">Canonical identifiers</h6><ul class="small mb-2">';
            cids.forEach((c) => {
                const perTable = c.canonical_column_per_table || {};
                const cols = Object.entries(perTable)
                    .map(([t, col]) => `<code>${esc(shortUri(t))}</code>→<code>${esc(col)}</code>`).join(', ');
                html += `<li><code>${esc(shortUri(c.ontology_class))}</code>: ${cols || '—'}${c.format_note ? ` <span class="text-muted">(${esc(c.format_note)})</span>` : ''}</li>`;
            });
            html += '</ul>';
        }

        const joins = sm.join_keys || [];
        if (joins.length) {
            html += '<h6 class="mt-2">Join keys</h6><table class="table table-sm ob-pge-sm-table"><thead><tr><th>From</th><th>To</th><th>Kind</th><th>Overlap</th></tr></thead><tbody>';
            joins.forEach((j) => {
                html += `<tr><td><code>${esc(j.from_ref)}</code></td><td><code>${esc(j.to_ref)}</code></td>
                    <td><span class="badge bg-light text-dark">${esc(j.kind)}</span></td>
                    <td>${fmtMetric(j.overlap_pct)}</td></tr>`;
            });
            html += '</tbody></table>';
        }

        const plan = sm.mapping_plan || {};
        const skips = plan.skip || [];
        if (skips.length) {
            html += '<h6 class="mt-2">Planner skipped</h6><ul class="small mb-0">';
            skips.forEach((s) => {
                html += `<li><code>${esc(shortUri(s.item))}</code> — ${esc(s.reason || 'no reason given')}</li>`;
            });
            html += '</ul>';
        }
        return html || '<p class="text-muted small mb-0">Planner produced an empty source-model.</p>';
    }

    // ---- per-item loop trace -------------------------------------------
    function renderAttempt(a) {
        function step(label, status, extraClass) {
            let cls = 'ob-pge-step';
            if (status === 'PASS') cls += ' ob-pge-step-pass';
            else if (status === 'FAIL') cls += ' ob-pge-step-fail';
            else if (status === 'skipped' || status === 'skip') cls += ' ob-pge-step-skip';
            if (extraClass) cls += ' ' + extraClass;
            const label2 = status && status !== 'skipped' ? `${label}: ${status}` : label;
            return `<span class="${cls}">${esc(label2)}</span>`;
        }
        const gen = `<span class="ob-pge-step">Generator</span>`;
        const stage1 = step('Evaluator', a.stage1_status);
        const showCritic = a.critic_status && a.critic_status !== 'skipped';
        const critic = showCritic ? `<span class="ob-pge-arrow">›</span>${step('Critic', a.critic_status)}` : '';
        const bubble = a.bubble ? `<span class="ob-pge-step ob-pge-step-bubble"><i class="bi bi-arrow-up-circle me-1"></i>re-plan</span>` : '';
        const err = a.error ? `<span class="ob-pge-hint"><i class="bi bi-bug me-1"></i>${esc(a.error)}</span>` : '';
        const hint = a.hint ? `<span class="ob-pge-hint"><i class="bi bi-lightbulb me-1"></i>${esc(a.hint)}</span>` : '';
        return `
            <div class="ob-pge-attempt">
                <span class="ob-pge-attempt-num">#${esc(a.attempt)}</span>
                <div class="flex-grow-1">
                    <div class="ob-pge-chain">
                        ${gen}<span class="ob-pge-arrow">›</span>${stage1}${critic}${bubble}
                    </div>
                    ${err}${hint}
                </div>
            </div>`;
    }

    function renderItem(entry, evals, idx) {
        const evalReport = evals[entry.item];
        const metrics = evalReport && evalReport.metrics ? evalReport.metrics : null;
        let metricsInline = '';
        if (metrics) {
            // Long free-text fields (e.g. the critic's reasoning) render as a
            // wrapped block; short scalar metrics render inline.
            const isLongText = (v) => typeof v === 'string' && v.length > 60;
            const longKeys = Object.keys(metrics).filter((k) => isLongText(metrics[k]));
            const scalarKeys = Object.keys(metrics)
                .filter((k) => !Array.isArray(metrics[k]) && typeof metrics[k] !== 'object' && !isLongText(metrics[k]))
                .slice(0, 6);
            if (scalarKeys.length) {
                metricsInline += `<div class="ob-pge-metrics-inline">` +
                    scalarKeys.map((k) => {
                        const val = /pct$|_pct|overlap/i.test(k) ? fmtRatio(metrics[k]) : fmtMetric(metrics[k]);
                        return `<span><span class="text-muted">${esc(humanize(k))}:</span> <strong>${val}</strong></span>`;
                    }).join('') +
                    `</div>`;
            }
            metricsInline += longKeys.map((k) =>
                `<div class="ob-pge-reasoning"><span class="text-muted">${esc(humanize(k))}:</span> ${esc(metrics[k])}</div>`
            ).join('');
        }
        const attempts = entry.attempts || [];
        const attemptsHtml = attempts.length
            ? `<div class="ob-pge-attempts">${attempts.map(renderAttempt).join('')}</div>`
            : '<div class="text-muted small ms-3 mt-1">No generator attempts (pre-seeded or skipped).</div>';
        const kindIcon = entry.kind === 'relationship' ? 'arrow-left-right' : 'box';
        const collId = `obPgeItem${idx}`;
        return `
            <div class="ob-pge-item">
                <div class="ob-pge-item-head" data-bs-toggle="collapse" data-bs-target="#${collId}" aria-expanded="false">
                    <i class="bi bi-${kindIcon} text-muted"></i>
                    <span class="ob-pge-item-name"><code>${esc(shortUri(entry.item))}</code></span>
                    ${attempts.length > 1 ? `<span class="badge bg-light text-dark">${attempts.length} attempts</span>` : ''}
                    ${statusBadge(entry.final_status)}
                    <i class="bi bi-chevron-down text-muted small"></i>
                </div>
                <div class="collapse" id="${collId}">
                    ${attemptsHtml}
                    ${metricsInline}
                </div>
            </div>`;
    }

    function renderTrace(runLog, evals) {
        if (!runLog || !runLog.length) return '<p class="text-muted small mb-0">No per-item run log captured.</p>';
        return runLog.map((e, i) => renderItem(e, evals || {}, i)).join('');
    }

    // ---- main entrypoint -----------------------------------------------
    function render(taskResult, containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;

        const tr = taskResult || {};
        const sc = tr.pge_scorecard || null;
        const sm = tr.source_model || null;
        const runLog = tr.mapping_run_log || null;
        const evals = tr.mapping_evaluations || {};

        // Nothing PGE-specific → keep the container empty/hidden.
        if (!sc && !sm && !(runLog && runLog.length)) {
            container.innerHTML = '';
            container.style.display = 'none';
            return;
        }
        container.style.display = 'block';

        const headerStages = `
            <div class="ob-pge-stages">
                <span class="ob-pge-stage-chip"><i class="bi bi-diagram-2"></i>Planner</span>
                <span class="ob-pge-stage-arrow">›</span>
                <span class="ob-pge-stage-chip"><i class="bi bi-cpu"></i>Generator</span>
                <span class="ob-pge-stage-arrow">›</span>
                <span class="ob-pge-stage-chip"><i class="bi bi-rulers"></i>Evaluator</span>
                <span class="ob-pge-stage-arrow">›</span>
                <span class="ob-pge-stage-chip"><i class="bi bi-search"></i>Critic</span>
            </div>`;

        const itemCount = runLog ? runLog.length : 0;
        const accordion = `
            <div class="accordion accordion-flush" id="obPgeAccordion">
                <div class="accordion-item">
                    <h2 class="accordion-header">
                        <button class="accordion-button" type="button" data-bs-toggle="collapse" data-bs-target="#obPgeTracePanel">
                            <i class="bi bi-list-check me-2"></i>Loop trace${itemCount ? ` · ${itemCount} item${itemCount > 1 ? 's' : ''}` : ''}
                        </button>
                    </h2>
                    <div id="obPgeTracePanel" class="accordion-collapse collapse show" data-bs-parent="#obPgeAccordion">
                        <div class="accordion-body p-0">${renderTrace(runLog, evals)}</div>
                    </div>
                </div>
                <div class="accordion-item">
                    <h2 class="accordion-header">
                        <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#obPgeSmPanel">
                            <i class="bi bi-diagram-2 me-2"></i>Planner source-model
                        </button>
                    </h2>
                    <div id="obPgeSmPanel" class="accordion-collapse collapse" data-bs-parent="#obPgeAccordion">
                        <div class="accordion-body">${renderSourceModel(sm)}</div>
                    </div>
                </div>
            </div>`;

        container.innerHTML = `
            <div class="ob-pge-card mt-3">
                <div class="ob-pge-header">
                    ${headerStages}
                    ${renderVerdictPill(sc)}
                </div>
                ${renderScorecardBody(sc)}
                ${accordion}
            </div>`;
    }

    return { render: render };
})();

// Expose globally (non-module script include).
window.PgeVisualizer = PgeVisualizer;
