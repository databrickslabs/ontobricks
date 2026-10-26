/**
 * OntoBricks - ontology-wizard-review.js
 *
 * Stage 2 (Review Entities) of the staged three-step Generate wizard —
 * plan task 5 of `staged-ontology-generate`
 * (docs/superpowers/specs/2026-09-20-three-stage-ontology-generate-design.md).
 *
 * Renders the durable Generate draft's locked existing anchors (read-only)
 * and editable candidate entities (include/exclude, remove, edit canonical
 * label/description/type hint/evidence/alternate labels), and drives every
 * Stage 2 mutation through `POST /ontology/wizard/generate/draft/update`
 * with optimistic-concurrency revision checks. `GET .../draft` and
 * `POST .../draft/discard` are also called from here. No draft/entity state
 * is ever written to sessionStorage — the server draft is the only source
 * of truth; this module just renders whatever `render(draftView)` is given
 * and re-fetches after every mutation.
 *
 * Orchestration (stage transitions, task polling) lives in
 * ontology-wizard.js, which exposes `window.WizardCore` for the few calls
 * this module needs to make back into it (start Stage 3, return to
 * Configure after a discard, re-run detection from a stale draft).
 */
(function () {
    'use strict';

    let _draft = null; // current draft view (server-authoritative), or null

    const escHtml = (window.TaskProgressUI && window.TaskProgressUI.escHtml)
        ? window.TaskProgressUI.escHtml
        : function (s) { return String(s == null ? '' : s); };

    function escAttr(s) {
        return escHtml(s).replace(/"/g, '&quot;');
    }

    // -------------------------------------------------------------------
    // Server calls
    // -------------------------------------------------------------------

    /**
     * Serializes every Stage 2 mutation through one FIFO queue. Rapid
     * edits (typing across two fields, toggling include while an evidence
     * edit is still in flight, ...) must never read `_draft.draft_revision`
     * at the same instant — each queued call only reads it once its turn
     * arrives, after every earlier call has applied its response to
     * `_draft`. The queue's own continuation (`_updateQueue`) is decoupled
     * from the call's returned promise via `.then(ok, ok)`, so a rejected
     * or server-refused (409) request can never wedge edits queued behind
     * it — the next one still runs, in the exact order callers invoked
     * `postDraftUpdate`.
     */
    let _updateQueue = Promise.resolve();

    function postDraftUpdate(payload) {
        const run = function () { return _applyDraftUpdate(payload); };
        const result = _updateQueue.then(run, run);
        _updateQueue = result.then(function () {}, function () {});
        return result;
    }

    /**
     * Apply one Stage 2 review mutation. `payload` must already carry
     * `op` (and any op-specific fields); the current draft's revision is
     * attached here so every call site stays a plain, readable object
     * literal like `postDraftUpdate({ op: 'exclude', entity_id: id })`.
     * Only ever invoked through the queue above — never call directly.
     */
    async function _applyDraftUpdate(payload) {
        if (!_draft) return null;
        const draftRevision = _draft.draft_revision;
        const body = Object.assign({ revision: draftRevision }, payload);

        let response;
        try {
            response = await fetch('/ontology/wizard/generate/draft/update', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
                credentials: 'same-origin',
            });
        } catch (error) {
            showNotification('Updating the draft failed: ' + error.message, 'error');
            return null;
        }

        if (response.status === 409) {
            showNotification(
                'This draft changed elsewhere. Reloaded the latest version.',
                'warning'
            );
            await reloadDraftFromServer();
            return null;
        }

        const data = await response.json().catch(function () { return {}; });
        if (!response.ok) {
            showNotification(data.message || 'Updating the draft failed', 'error');
            return null;
        }

        // The update route returns draft.to_dict() without a `stale` key
        // (only GET .../draft recomputes it) — our own edit never touches
        // the source selection/ontology, so the previous flag still holds.
        const stale = _draft.stale || false;
        _draft = data.draft;
        _draft.stale = stale;
        render(_draft);
        return _draft;
    }

    async function reloadDraftFromServer() {
        try {
            const response = await fetch('/ontology/wizard/generate/draft', {
                credentials: 'same-origin',
            });
            const data = await response.json();
            if (data.success && data.draft) {
                render(data.draft);
            }
        } catch (error) {
            console.error('[WizardReview] Reload failed:', error);
        }
    }

    async function discardDraft() {
        const confirmed = await showConfirmDialog({
            title: 'Discard Draft',
            message:
                'Discard every detected and manually added candidate entity ' +
                'and start over? This cannot be undone.',
            confirmText: 'Discard',
            confirmClass: 'btn-outline-danger',
            icon: 'trash',
        });
        if (!confirmed) return;

        try {
            const response = await fetch('/ontology/wizard/generate/draft/discard', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
            });
            const data = await response.json().catch(function () { return {}; });
            if (!response.ok) {
                showNotification(data.message || 'Discarding the draft failed', 'error');
                return;
            }
            reset();
            showNotification('Draft discarded', 'info');
            if (window.WizardCore && typeof window.WizardCore.onDraftDiscarded === 'function') {
                window.WizardCore.onDraftDiscarded();
            }
        } catch (error) {
            showNotification('Discarding the draft failed: ' + error.message, 'error');
        }
    }

    // -------------------------------------------------------------------
    // Rendering
    // -------------------------------------------------------------------

    function render(draftView) {
        _draft = draftView || null;
        bindEvents();
        renderStaleBanner();
        renderLockedAnchors();
        renderCandidates();
        updateContinueButtonState();
    }

    function reset() {
        _draft = null;
        const lockedList = document.getElementById('wizardLockedAnchorsList');
        const candidatesList = document.getElementById('wizardCandidatesList');
        if (lockedList) lockedList.innerHTML = '';
        if (candidatesList) candidatesList.innerHTML = '';
        const staleBanner = document.getElementById('wizardStaleBanner');
        if (staleBanner) staleBanner.classList.add('ob-hidden');
        const validation = document.getElementById('wizardReviewValidation');
        if (validation) validation.classList.add('ob-hidden');
        hideAddCandidateForm();
    }

    function renderStaleBanner() {
        const banner = document.getElementById('wizardStaleBanner');
        if (!banner) return;
        banner.classList.toggle('ob-hidden', !(_draft && _draft.stale));
    }

    function renderLockedAnchors() {
        const container = document.getElementById('wizardLockedAnchorsList');
        const empty = document.getElementById('wizardNoLockedAnchors');
        if (!container) return;
        const anchors = (_draft && _draft.existing_anchors) || [];
        if (!anchors.length) {
            container.innerHTML = '';
            if (empty) empty.classList.remove('ob-hidden');
            return;
        }
        if (empty) empty.classList.add('ob-hidden');
        container.innerHTML = anchors.map(renderLockedAnchorRow).join('');
    }

    /** Read-only card for an existing (locked) ontology entity — never
     * wires include/exclude/remove/edit controls, per the design's
     * "existing entities cannot be edited/excluded/removed" invariant. */
    function renderLockedAnchorRow(anchor) {
        const altChips = (anchor.alternate_labels || [])
            .map(function (a) { return '<span class="wizard-chip">' + escHtml(a) + '</span>'; })
            .join('');
        return (
            '<div class="wizard-entity-row wizard-entity-locked" data-entity-id="' +
            escAttr(anchor.id) + '" data-locked="true">' +
            '<div class="d-flex align-items-center">' +
            '<i class="bi bi-lock-fill wizard-lock-icon" title="Locked \u2014 existing ontology entity" aria-hidden="true"></i>' +
            '<span class="wizard-entity-label">' + escHtml(anchor.canonical_label) + '</span>' +
            '<span class="badge text-bg-secondary ms-auto">Entity</span>' +
            '</div>' +
            (altChips ? '<div class="wizard-chip-list">' + altChips + '</div>' : '') +
            '</div>'
        );
    }

    function renderCandidates() {
        const container = document.getElementById('wizardCandidatesList');
        const empty = document.getElementById('wizardNoCandidates');
        if (!container) return;
        const candidates = (_draft && _draft.candidate_entities) || [];
        if (!candidates.length) {
            container.innerHTML = '';
            if (empty) empty.classList.remove('ob-hidden');
            return;
        }
        if (empty) empty.classList.add('ob-hidden');
        container.innerHTML = candidates.map(renderCandidateRow).join('');
    }

    function renderCandidateRow(candidate) {
        const id = escAttr(candidate.id);
        const included = !!candidate.included;
        const evidence = candidate.evidence || [];
        const altLabels = candidate.alternate_labels || [];

        const evidenceRows = evidence.map(function (e, idx) {
            return (
                '<div class="wizard-evidence-row" data-idx="' + idx + '">' +
                '<input type="text" class="form-control form-control-sm wizard-evidence-source" ' +
                'placeholder="Source" value="' + escAttr(e.source || '') + '">' +
                '<input type="text" class="form-control form-control-sm wizard-evidence-excerpt" ' +
                'placeholder="Excerpt" value="' + escAttr(e.excerpt || '') + '">' +
                '<button type="button" class="btn btn-sm btn-outline-danger" ' +
                'data-action="wizard-review-remove-evidence" data-entity-id="' + id + '" ' +
                'data-idx="' + idx + '" title="Remove evidence"><i class="bi bi-x"></i></button>' +
                '</div>'
            );
        }).join('');

        const altChips = altLabels.map(function (label, idx) {
            return (
                '<span class="wizard-chip" data-idx="' + idx + '">' + escHtml(label) +
                '<button type="button" class="wizard-chip-remove" data-action="wizard-review-remove-alt-label" ' +
                'data-entity-id="' + id + '" data-idx="' + idx + '" aria-label="Remove ' + escAttr(label) + '">' +
                '&times;</button></span>'
            );
        }).join('');

        return (
            '<div class="wizard-entity-row wizard-candidate-row' +
            (included ? '' : ' wizard-candidate-excluded') + '" data-entity-id="' + id + '">' +
            '<div class="d-flex align-items-start gap-2">' +
            '<input type="checkbox" class="form-check-input wizard-candidate-include mt-1" ' +
            'data-entity-id="' + id + '" ' + (included ? 'checked' : '') +
            ' title="Include in ontology" aria-label="Include ' + escAttr(candidate.canonical_label) + '">' +
            '<div class="flex-grow-1">' +
            '<input type="text" class="form-control form-control-sm wizard-field-input mb-1" ' +
            'data-entity-id="' + id + '" data-field="canonical_label" ' +
            'value="' + escAttr(candidate.canonical_label) + '" aria-label="Canonical label">' +
            '<textarea class="form-control form-control-sm wizard-field-input mb-1" ' +
            'data-entity-id="' + id + '" data-field="description" rows="2" ' +
            'placeholder="Description" aria-label="Description">' + escHtml(candidate.description || '') +
            '</textarea>' +
            '<div class="wizard-chip-list" data-entity-id="' + id + '">' +
            altChips +
            '<input type="text" class="wizard-chip-input" data-entity-id="' + id + '" ' +
            'placeholder="Add alternate label\u2026" aria-label="Add alternate label">' +
            '</div>' +
            '<details class="wizard-evidence-details mt-1">' +
            '<summary class="small text-muted">Evidence (' + evidence.length + ')</summary>' +
            '<div class="wizard-evidence-list mt-1" data-entity-id="' + id + '">' +
            evidenceRows +
            '<button type="button" class="btn btn-sm btn-outline-secondary mt-1" ' +
            'data-action="wizard-review-add-evidence" data-entity-id="' + id + '">' +
            '<i class="bi bi-plus-lg me-1"></i>Add evidence</button>' +
            '</div>' +
            '</details>' +
            '<span class="badge text-bg-light border mt-1">' + escHtml(candidate.origin || 'detected') + '</span>' +
            '</div>' +
            '<button type="button" class="btn btn-sm btn-outline-danger wizard-candidate-remove" ' +
            'data-action="wizard-review-remove-candidate" data-entity-id="' + id + '" title="Remove candidate">' +
            '<i class="bi bi-x-lg"></i></button>' +
            '</div>' +
            '</div>'
        );
    }

    function updateContinueButtonState() {
        const btn = document.getElementById('wizardReviewContinueBtn');
        const banner = document.getElementById('wizardReviewValidation');
        const anchors = (_draft && _draft.existing_anchors) || [];
        const candidates = (_draft && _draft.candidate_entities) || [];
        const hasEntities = anchors.length > 0 || candidates.some(function (c) { return c.included; });
        const stale = !!(_draft && _draft.stale);

        if (btn) btn.disabled = stale || !hasEntities;

        if (banner) {
            if (!hasEntities) {
                banner.textContent =
                    'At least one entity (existing anchor or included candidate) ' +
                    'is required before you can continue.';
                banner.classList.remove('ob-hidden');
            } else {
                banner.classList.add('ob-hidden');
                banner.textContent = '';
            }
        }
    }

    function findCandidate(entityId) {
        const candidates = (_draft && _draft.candidate_entities) || [];
        return candidates.find(function (c) { return c.id === entityId; }) || null;
    }

    // -------------------------------------------------------------------
    // Add-candidate form
    // -------------------------------------------------------------------

    function showAddCandidateForm() {
        const form = document.getElementById('wizardAddCandidateForm');
        if (form) form.classList.remove('ob-hidden');
    }

    function hideAddCandidateForm() {
        const form = document.getElementById('wizardAddCandidateForm');
        if (!form) return;
        form.classList.add('ob-hidden');
        const label = document.getElementById('wizardNewCandidateLabel');
        const description = document.getElementById('wizardNewCandidateDescription');
        if (label) label.value = '';
        if (description) description.value = '';
    }

    async function submitNewCandidate() {
        const labelEl = document.getElementById('wizardNewCandidateLabel');
        const descriptionEl = document.getElementById('wizardNewCandidateDescription');
        const label = (labelEl && labelEl.value || '').trim();
        if (!label) {
            showNotification('A canonical label is required to add an entity', 'warning');
            return;
        }
        const result = await postDraftUpdate({
            op: 'add',
            entity: {
                canonical_label: label,
                description: (descriptionEl && descriptionEl.value || '').trim(),
                type_hint: 'class',
            },
        });
        if (result) {
            hideAddCandidateForm();
            showNotification('Added candidate entity: ' + label, 'success', 2000);
        }
    }

    // -------------------------------------------------------------------
    // Event delegation (bound once on #wizardReviewPane)
    // -------------------------------------------------------------------

    function bindEvents() {
        const reviewRoot = document.getElementById('wizardReviewPane');
        if (!reviewRoot || reviewRoot.dataset.wizardReviewBound === '1') return;
        reviewRoot.dataset.wizardReviewBound = '1';

        // Review actions live beside the top stepper, outside the Review
        // pane. Delegate clicks from the shared wizard host while keeping
        // field change/keyboard listeners scoped to editable review cards.
        const clickRoot = document.getElementById('wizard-section') || reviewRoot;
        clickRoot.addEventListener('click', onReviewClick);
        reviewRoot.addEventListener('change', onReviewChange);
        reviewRoot.addEventListener('keydown', onReviewKeydown);
    }

    function onReviewClick(event) {
        const el = event.target.closest('[data-action]');
        if (!el) return;
        const action = el.dataset.action;
        const entityId = el.dataset.entityId;

        switch (action) {
            case 'wizard-review-add-candidate-toggle':
                showAddCandidateForm();
                break;
            case 'wizard-review-add-candidate-cancel':
                hideAddCandidateForm();
                break;
            case 'wizard-review-add-candidate-submit':
                submitNewCandidate();
                break;
            case 'wizard-review-remove-candidate':
                postDraftUpdate({ op: 'remove', entity_id: entityId });
                break;
            case 'wizard-review-remove-alt-label':
                removeAltLabel(entityId, parseInt(el.dataset.idx, 10));
                break;
            case 'wizard-review-add-evidence':
                addEvidenceRow(entityId);
                break;
            case 'wizard-review-remove-evidence':
                removeEvidenceRow(entityId, parseInt(el.dataset.idx, 10));
                break;
            case 'wizard-review-continue':
                onContinueClicked();
                break;
            case 'wizard-review-discard':
                discardDraft();
                break;
            case 'wizard-review-restart':
                if (window.WizardCore && typeof window.WizardCore.redetect === 'function') {
                    window.WizardCore.redetect();
                }
                break;
            default:
                break;
        }
    }

    function onReviewChange(event) {
        const target = event.target;
        if (!target) return;

        if (target.classList.contains('wizard-candidate-include')) {
            const entityId = target.dataset.entityId;
            if (target.checked) {
                postDraftUpdate({ op: 'include', entity_id: entityId });
            } else {
                postDraftUpdate({ op: 'exclude', entity_id: entityId });
            }
            return;
        }

        if (target.classList.contains('wizard-field-input')) {
            const entityId = target.dataset.entityId;
            const field = target.dataset.field;
            postDraftUpdate({ op: 'update', entity_id: entityId, updates: wrapFieldUpdate(field, target.value) });
            return;
        }

        if (
            target.classList.contains('wizard-evidence-source') ||
            target.classList.contains('wizard-evidence-excerpt')
        ) {
            const list = target.closest('.wizard-evidence-list');
            if (list) commitEvidence(list.dataset.entityId, list);
        }
    }

    function onReviewKeydown(event) {
        if (event.key !== 'Enter') return;
        const target = event.target;
        if (!target || !target.classList.contains('wizard-chip-input')) return;
        event.preventDefault();
        const value = target.value.trim();
        if (!value) return;
        addAltLabel(target.dataset.entityId, value);
        target.value = '';
    }

    function wrapFieldUpdate(field, value) {
        const updates = {};
        updates[field] = value;
        return updates;
    }

    function addAltLabel(entityId, value) {
        const candidate = findCandidate(entityId);
        if (!candidate) return;
        const labels = (candidate.alternate_labels || []).slice();
        labels.push(value);
        postDraftUpdate({ op: 'update', entity_id: entityId, updates: { alternate_labels: labels } });
    }

    function removeAltLabel(entityId, idx) {
        const candidate = findCandidate(entityId);
        if (!candidate) return;
        const labels = (candidate.alternate_labels || []).slice();
        labels.splice(idx, 1);
        postDraftUpdate({ op: 'update', entity_id: entityId, updates: { alternate_labels: labels } });
    }

    function addEvidenceRow(entityId) {
        const candidate = findCandidate(entityId);
        if (!candidate) return;
        const evidence = (candidate.evidence || []).slice();
        evidence.push({ source: '', excerpt: '' });
        postDraftUpdate({ op: 'update', entity_id: entityId, updates: { evidence: evidence } });
    }

    function removeEvidenceRow(entityId, idx) {
        const candidate = findCandidate(entityId);
        if (!candidate) return;
        const evidence = (candidate.evidence || []).slice();
        evidence.splice(idx, 1);
        postDraftUpdate({ op: 'update', entity_id: entityId, updates: { evidence: evidence } });
    }

    function commitEvidence(entityId, listEl) {
        const rows = listEl.querySelectorAll('.wizard-evidence-row');
        const evidence = Array.from(rows).map(function (row) {
            const source = row.querySelector('.wizard-evidence-source');
            const excerpt = row.querySelector('.wizard-evidence-excerpt');
            return { source: source ? source.value : '', excerpt: excerpt ? excerpt.value : '' };
        });
        postDraftUpdate({ op: 'update', entity_id: entityId, updates: { evidence: evidence } });
    }

    function onContinueClicked() {
        const btn = document.getElementById('wizardReviewContinueBtn');
        if (btn && btn.disabled) return;
        if (window.WizardCore && typeof window.WizardCore.startCompletion === 'function') {
            window.WizardCore.startCompletion();
        }
    }

    // -------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------

    window.WizardReview = {
        render: render,
        reset: reset,
        // Exposed only for the mutation-serialization behavior contract
        // (tests/units/front/test_wizard_draft_update_queue.py). No markup
        // or delegated handler calls this directly — every real call site
        // above already goes through the same queued `postDraftUpdate`.
        postDraftUpdate: postDraftUpdate,
    };
})();
