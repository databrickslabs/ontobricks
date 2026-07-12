// =====================================================
// SINGLE-EDITOR EDIT LOCK
//
// Only one browser may *edit* a given DRAFT (domain, version) at a time.
// The first opener acquires the lock (server-side, in ``load-from-uc``);
// later openers land read-only. The lock is held until the editor explicitly
// *closes* the domain (the "Close" button → ``POST /domain/close``), an
// app-admin takes it over, the version leaves DRAFT, or — when a lease TTL is
// configured (``lease_ttl_s > 0``) — its lease lapses.
//
// This module, loaded on every page:
//
//   1. on load, asks the backend for the lock status of the session's
//      loaded version (``GET /domain/edit-lock``). The backend performs a
//      non-forcing acquire so the editor keeps the lock across reloads /
//      in-app navigation (the lock is keyed by e-mail, so a plain page
//      change never hands it to someone else);
//   2. when this browser is a VIEWER (``mode == "view"``) it stamps
//      ``body.read-only-locked`` — re-using every gate in
//      ``permissions.css`` — and shows a dismissible banner naming the
//      current editor. App-admins additionally get a "Take over editing"
//      button (``POST /domain/edit-lock/acquire`` with ``force``);
//   3. when this browser is the EDITOR (``mode == "edit"``) and a lease TTL
//      is set, a keep-alive timer pings ``POST /domain/edit-lock/renew`` to
//      hold the lease — but only while the user is *active* (mouse / keyboard
//      / scroll / tab re-focus). Once the user has been idle long enough that
//      the lease is about to lapse, a modal warns them that they are about to
//      be disconnected from the project, with a live countdown and a "Keep
//      editing" button that renews on the spot. If they ignore the warning
//      the lease lapses: the lock is *released* server-side (so another user
//      can take it immediately) and this browser flips to read-only with a
//      yellow **Resume editing** button in the L2 subnav (left of Save; the
//      expiry message is a hover tooltip). A per-version sticky flag
//      (sessionStorage) then keeps it read-only across incidental reloads /
//      navigation — the still-free lock is not silently re-grabbed — until the
//      user clicks **Resume editing**. A lease lost to a take-over / stale
//      reclaim flips to read-only the same way (without the sticky, since it
//      is already gone).
//      Hovering the navbar domain badge (#currentDomainName) shows the
//      remaining-lease countdown while editing.
//
// ``window.editLockMode`` ('edit' | 'view' | 'none') is exposed so
// ``permissions.js`` → ``canEditOntology()`` respects the lock too.
//
// Note: the lock is deliberately NOT released on tab close / navigation.
// Because this is a multi-page app, every navigation is a full unload;
// releasing there would briefly free the lock and let another user steal
// it mid-session. The lock is only freed by an explicit Close or by the
// lease lapsing after a full TTL with no renew (a crashed / abandoned tab).
// =====================================================

(function () {
    'use strict';

    // Default permissive until the async check resolves (mirrors
    // ``window.isActiveVersion`` in version-check.js). ``permissions.js``
    // only blocks on an explicit 'view'.
    window.editLockMode = window.editLockMode || 'edit';

    function notify(msg, kind) {
        if (typeof window.showNotification === 'function') {
            window.showNotification(msg, kind || 'info');
        }
    }

    // ----- "editing session expired" sticky state -------------------
    //
    // When a lease lapses through *inactivity* we release the lock and record
    // that this browser gave up editing this exact (folder, version). Because
    // this is a multi-page app, a plain reload / menu navigation would
    // otherwise silently re-acquire the still-free lock (the server grants a
    // free lock to the loader) and hand editing straight back — undoing the
    // timeout. The sticky flag (per version, in sessionStorage so it survives
    // reloads but not a new tab) keeps the browser read-only until the user
    // *explicitly* resumes (the subnav "Resume editing" button).

    var STICKY_KEY = 'ob:editLockExpired';
    // "folder\u0000version" of the loaded lockable version, once known.
    var lockKey = '';

    function keyFor(data) {
        return (data && data.folder && data.version)
            ? data.folder + '\u0000' + data.version
            : '';
    }

    function stickyGet() {
        try { return window.sessionStorage.getItem(STICKY_KEY); }
        catch (e) { return null; }
    }
    function stickySet(v) {
        try { window.sessionStorage.setItem(STICKY_KEY, v); } catch (e) {}
    }
    function stickyClear() {
        try { window.sessionStorage.removeItem(STICKY_KEY); } catch (e) {}
    }

    function releaseLock() {
        try {
            fetch('/domain/edit-lock/release', {
                method: 'POST',
                headers: { 'Accept': 'application/json' },
                credentials: 'same-origin',
                keepalive: true,
            });
        } catch (e) {
            // Best-effort — the lease also lapses on its own after the TTL.
        }
    }

    function holderLabel(data) {
        return (data && (data.holder_name || data.holder_email)) || 'another user';
    }

    function escapeHtml(str) {
        return String(str == null ? '' : str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    // ----- read-only (viewer) presentation --------------------------

    function enterReadOnly(data) {
        window.editLockMode = 'view';
        document.body.classList.add('read-only-locked');
        if (window.OB && typeof window.OB.installReadOnlyContextMenuBlocker === 'function') {
            window.OB.installReadOnlyContextMenuBlocker();
        }
        if (window.OB && typeof window.OB.annotateRoleNavBadge === 'function') {
            window.OB.annotateRoleNavBadge(
                'This version is being edited by ' + holderLabel(data) + ' (read-only).'
            );
        }
    }

    function renderBanner(data) {
        if (document.getElementById('editLockBanner')) return;
        var banner = document.createElement('div');
        banner.id = 'editLockBanner';

        var msg = document.createElement('span');
        msg.className = 'edit-lock-msg';
        msg.innerHTML =
            '<i class="bi bi-lock-fill me-1"></i>' +
            'This version is being edited by <strong>' +
            escapeHtml(holderLabel(data)) +
            '</strong> — you have read-only access until they close the domain.';
        banner.appendChild(msg);

        var actions = document.createElement('span');
        actions.className = 'edit-lock-actions';

        if (data && data.can_take_over) {
            var takeover = document.createElement('button');
            takeover.type = 'button';
            takeover.className = 'btn btn-sm btn-warning';
            takeover.innerHTML = '<i class="bi bi-unlock me-1"></i>Take over editing';
            takeover.addEventListener('click', takeOver);
            actions.appendChild(takeover);
        }

        var dismiss = document.createElement('button');
        dismiss.type = 'button';
        dismiss.className = 'btn btn-sm btn-outline-secondary';
        dismiss.setAttribute('aria-label', 'Dismiss');
        dismiss.innerHTML = '<i class="bi bi-x-lg"></i>';
        dismiss.addEventListener('click', function () {
            banner.remove();
        });
        actions.appendChild(dismiss);

        banner.appendChild(actions);
        document.body.insertBefore(banner, document.body.firstChild);
    }

    var resumeTip = null;

    function expiredTooltipText() {
        return 'Your editing session expired after a period of inactivity — you ' +
            'now have read-only access and saving is disabled. Click Resume ' +
            'editing to reconnect (you regain editing if no one else has taken over).';
    }

    function onResumeEditingClick() {
        // Explicit reconnect: drop the sticky "expired" flag so the reload
        // is allowed to re-acquire the (still-free) lock.
        stickyClear();
        window.location.reload();
    }

    function renderResumeEditingButton() {
        if (document.getElementById('editLockResumeBtn')) return;

        var saveBtn = document.querySelector('.ob-subnav-save-btn');
        if (!saveBtn || !saveBtn.parentNode) return;

        var btn = document.createElement('button');
        btn.type = 'button';
        btn.id = 'editLockResumeBtn';
        btn.className = 'ob-subnav-resume-btn';
        btn.innerHTML =
            '<i class="bi bi-arrow-clockwise"></i> Resume editing';
        btn.setAttribute('aria-label', expiredTooltipText());
        btn.addEventListener('click', onResumeEditingClick);
        saveBtn.parentNode.insertBefore(btn, saveBtn);

        if (window.bootstrap && window.bootstrap.Tooltip) {
            resumeTip = new window.bootstrap.Tooltip(btn, {
                title: expiredTooltipText(),
                trigger: 'hover focus',
                placement: 'bottom',
            });
        }
    }

    // ----- lease keep-alive (renew) ---------------------------------
    //
    // The lease is kept alive by the holder's *activity*, not merely by the
    // tab being open: a keep-alive tick only renews when the user has done
    // something (mouse / keyboard / scroll / tab re-focus) within the last
    // renew interval. Once they go idle the pings stop, the lease ages, and a
    // modal warns them before it lapses that they are about to be disconnected
    // from the project.

    var renewTimer = null;
    var idleTimer = null;
    // Lease TTL (seconds), the local time (ms) of the last confirmed renew
    // (drives the countdown), the last user-activity time, the renew cadence,
    // and how long before expiry to warn.
    var leaseTtlS = 0;
    var lastRenewAt = 0;
    var lastActivityAt = 0;
    var renewEveryMs = 0;
    var warnLeadMs = 0;
    var warnShown = false;

    // Passive signals that the holder is still working. Bound on the capture
    // phase so app handlers cannot swallow them before we see them.
    var ACTIVITY_EVENTS = [
        'mousemove', 'mousedown', 'keydown', 'wheel', 'touchstart', 'scroll',
    ];

    function markActivity() {
        lastActivityAt = Date.now();
        // Acting while the "about to be disconnected" warning is up dismisses
        // it and immediately re-takes the lease.
        if (warnShown) {
            hideIdleWarning();
            doRenew();
        }
    }

    function stopRenew() {
        if (renewTimer) {
            clearInterval(renewTimer);
            renewTimer = null;
        }
        if (idleTimer) {
            clearInterval(idleTimer);
            idleTimer = null;
        }
        document.removeEventListener('visibilitychange', onVisible);
        ACTIVITY_EVENTS.forEach(function (ev) {
            document.removeEventListener(ev, markActivity, true);
        });
        hideIdleWarning();
    }

    function onLeaseLost() {
        stopRenew();
        teardownLeaseTooltip();
        window.editLockMode = 'view';
        enterReadOnly({});
        renderResumeEditingButton();
    }

    // Lease lapsed through inactivity: give the lock up server-side (so another
    // user can take it now, not only after the stale TTL) and remember we did,
    // so a later reload / navigation does not silently re-grab it.
    function expireByIdle() {
        if (lockKey) stickySet(lockKey);
        releaseLock();
        onLeaseLost();
    }

    function leaseRemainingMs() {
        if (!leaseTtlS) return 0;
        return Math.max(0, leaseTtlS * 1000 - (Date.now() - lastRenewAt));
    }

    async function doRenew() {
        try {
            var res = await fetch('/domain/edit-lock/renew', {
                method: 'POST',
                headers: { 'Accept': 'application/json' },
                credentials: 'same-origin',
            });
            var data = await res.json();
            // Only a definitive "not renewed" (success + renewed === false)
            // means the lease was lost. A transient backend blip
            // (success === false) is ignored — the next tick retries.
            if (data && data.success === true) {
                if (data.renewed === false) {
                    onLeaseLost();
                } else {
                    lastRenewAt = Date.now(); // lease clock reset
                }
            }
        } catch (e) {
            // Network hiccup — keep the timer; the next tick retries.
        }
    }

    // Keep-alive tick: renew only while the user has been active recently, so
    // an idle tab lets its lease lapse for the next opener to reclaim.
    function onRenewTick() {
        if (Date.now() - lastActivityAt <= renewEveryMs) doRenew();
    }

    // Idle watcher (1s): drives the "about to be disconnected" warning and the
    // final flip to read-only when the lease actually lapses.
    function onIdleTick() {
        var remainingMs = leaseRemainingMs();
        if (remainingMs <= 0) {
            expireByIdle();
            return;
        }
        if (remainingMs <= warnLeadMs) {
            showIdleWarning();
            updateIdleWarning(Math.ceil(remainingMs / 1000));
        } else if (warnShown) {
            hideIdleWarning();
        }
    }

    function onVisible() {
        if (document.hidden) return;
        // Returning to the tab counts as activity.
        lastActivityAt = Date.now();
        hideIdleWarning();
        doRenew();
    }

    function startRenew(ttlSeconds) {
        var ttl = parseInt(ttlSeconds, 10);
        if (!ttl || ttl <= 0) return; // lease disabled
        leaseTtlS = ttl;
        lastRenewAt = Date.now();
        lastActivityAt = Date.now();
        // Renew at ~a third of the TTL (min 30s) so a couple of missed pings
        // still land inside the lease window.
        renewEveryMs = Math.max(30, Math.floor(ttl / 3)) * 1000;
        // Warn this long before the lease would lapse (≤60s, ≥10s, ≤TTL/3).
        warnLeadMs = Math.max(10, Math.min(60, Math.floor(ttl / 3))) * 1000;
        renewTimer = setInterval(onRenewTick, renewEveryMs);
        idleTimer = setInterval(onIdleTick, 1000);
        document.addEventListener('visibilitychange', onVisible);
        ACTIVITY_EVENTS.forEach(function (ev) {
            document.addEventListener(ev, markActivity, true);
        });
        setupLeaseTooltip();
    }

    // ----- idle disconnect warning modal ----------------------------

    var idleModalEl = null;
    var idleModal = null;

    function buildIdleModal() {
        if (idleModalEl) return;
        var el = document.createElement('div');
        // No `fade`: the activity listener hides this modal the instant the
        // user moves the mouse after it pops. A mid-transition Bootstrap
        // hide() is a no-op, which — with the static backdrop — leaves the
        // page frozen behind an un-dismissable overlay. Synchronous show/hide
        // (no transition) removes that race.
        el.className = 'modal';
        el.id = 'editLockIdleModal';
        el.setAttribute('tabindex', '-1');
        el.setAttribute('aria-hidden', 'true');
        el.setAttribute('data-bs-backdrop', 'static');
        el.setAttribute('data-bs-keyboard', 'false');
        el.innerHTML =
            '<div class="modal-dialog modal-dialog-centered">' +
              '<div class="modal-content">' +
                '<div class="modal-header bg-warning-subtle">' +
                  '<h5 class="modal-title">' +
                    '<i class="bi bi-hourglass-split me-2"></i>Still editing?' +
                  '</h5>' +
                '</div>' +
                '<div class="modal-body">' +
                  '<p class="mb-2">You have been inactive for a while. To avoid ' +
                  'locking other users out of this version, your editing session ' +
                  'is about to be released and you will be <strong>disconnected ' +
                  'from the project</strong> (read-only access).</p>' +
                  '<p class="mb-0">Editing will be released in ' +
                  '<strong id="editLockIdleCountdown">--</strong> unless you ' +
                  'continue.</p>' +
                '</div>' +
                '<div class="modal-footer">' +
                  '<button type="button" class="btn btn-warning" ' +
                    'id="editLockIdleKeep">' +
                    '<i class="bi bi-pencil-square me-1"></i>Keep editing' +
                  '</button>' +
                '</div>' +
              '</div>' +
            '</div>';
        document.body.appendChild(el);
        idleModalEl = el;
        var keep = el.querySelector('#editLockIdleKeep');
        if (keep) keep.addEventListener('click', markActivity);
        if (window.bootstrap && window.bootstrap.Modal) {
            idleModal = new window.bootstrap.Modal(el);
        }
    }

    function showIdleWarning() {
        if (warnShown) return;
        warnShown = true;
        buildIdleModal();
        if (idleModal) {
            idleModal.show();
        } else {
            // No Bootstrap modal available — degrade to a notification.
            notify(
                'You are about to be disconnected from editing due to ' +
                'inactivity. Interact with the page to stay.',
                'warning'
            );
        }
    }

    function updateIdleWarning(seconds) {
        if (!idleModalEl) return;
        var cd = idleModalEl.querySelector('#editLockIdleCountdown');
        if (cd) cd.textContent = fmtMMSS(Math.max(0, seconds));
    }

    function hideIdleWarning() {
        if (!warnShown) return;
        warnShown = false;
        if (idleModal) idleModal.hide();
    }

    // ----- lease countdown tooltip on the navbar domain badge -------
    //
    // While this browser is the editor, hovering the navbar domain badge
    // (#currentDomainName) shows how long the edit lease lasts if the session
    // goes idle. It refreshes live (every second) while the tooltip is open.

    var domainTip = null;
    var tipTimer = null;

    function remainingSeconds() {
        return Math.round(leaseRemainingMs() / 1000);
    }

    function fmtMMSS(sec) {
        var m = Math.floor(sec / 60);
        var s = sec % 60;
        return m + ':' + (s < 10 ? '0' : '') + s;
    }

    function leaseTooltipText() {
        return 'Editing lock — renews while you work. Released in ' +
            fmtMMSS(remainingSeconds()) + ' if you stay inactive.';
    }

    function setupLeaseTooltip() {
        if (domainTip) return;
        var badge = document.getElementById('currentDomainName');
        if (!badge || !window.bootstrap || !window.bootstrap.Tooltip) return;
        domainTip = new window.bootstrap.Tooltip(badge, {
            title: leaseTooltipText, // re-evaluated by Bootstrap on each show
            trigger: 'hover focus',
            placement: 'bottom',
            customClass: 'ob-lease-tooltip',
        });
        badge.addEventListener('shown.bs.tooltip', onTipShown);
        badge.addEventListener('hide.bs.tooltip', onTipHide);
    }

    function onTipShown() {
        if (tipTimer) clearInterval(tipTimer);
        tipTimer = setInterval(function () {
            if (domainTip && typeof domainTip.setContent === 'function') {
                domainTip.setContent({ '.tooltip-inner': leaseTooltipText() });
            }
        }, 1000);
    }

    function onTipHide() {
        if (tipTimer) {
            clearInterval(tipTimer);
            tipTimer = null;
        }
    }

    function teardownLeaseTooltip() {
        onTipHide();
        var badge = document.getElementById('currentDomainName');
        if (badge) {
            badge.removeEventListener('shown.bs.tooltip', onTipShown);
            badge.removeEventListener('hide.bs.tooltip', onTipHide);
        }
        if (domainTip) {
            domainTip.dispose();
            domainTip = null;
        }
    }

    // ----- admin take-over ------------------------------------------

    async function takeOver() {
        try {
            var res = await fetch('/domain/edit-lock/acquire', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ force: true }),
            });
            var data = await res.json();
            if (data && data.mode === 'edit') {
                notify('You are now the editor of this version.', 'success');
                window.location.reload();
            } else {
                notify('Could not take over the editing lock.', 'danger');
            }
        } catch (e) {
            notify('Could not take over the editing lock.', 'danger');
        }
    }

    // ----- bootstrap -------------------------------------------------

    async function initEditLock() {
        var data;
        try {
            var res = await fetch('/domain/edit-lock', {
                headers: { 'Accept': 'application/json' },
                credentials: 'same-origin',
            });
            data = await res.json();
        } catch (e) {
            return; // no lock backend / not loaded — stay permissive
        }
        if (!data || !data.success) return;

        var mode = data.mode || 'none';
        lockKey = keyFor(data);

        if (mode === 'view') {
            // Someone else holds it now — the sticky "expired" state (if any)
            // is moot; a fresh viewer banner tells the real story.
            stickyClear();
            window.editLockMode = 'view';
            enterReadOnly(data);
            renderBanner(data);
        } else if (mode === 'edit') {
            if (lockKey && stickyGet() === lockKey) {
                // We timed out on this exact version earlier and the backend's
                // non-forcing acquire just handed the still-free lock back.
                // Honour the timeout: give it up again and stay read-only until
                // the user explicitly resumes.
                window.editLockMode = 'view';
                releaseLock();
                enterReadOnly({});
                renderResumeEditingButton();
            } else {
                // This browser holds the lock — keep its lease alive.
                window.editLockMode = 'edit';
                startRenew(data.lease_ttl_s);
            }
        } else {
            // mode === 'none' → not a lockable DRAFT version; nothing to do.
            window.editLockMode = 'none';
            stickyClear();
        }
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initEditLock);
    } else {
        initEditLock();
    }
})();
