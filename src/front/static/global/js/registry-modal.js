/**
 * Registry modal — navbar archive icon opens Browse / Bridges.
 */
(function () {
    'use strict';

    const TOGGLE_ID = 'registryModalToggle';
    const MODAL_ID = 'registryModal';

    document.addEventListener('DOMContentLoaded', function () {
        const toggle = document.getElementById(TOGGLE_ID);
        const modalEl = document.getElementById(MODAL_ID);
        if (!toggle || !modalEl || typeof bootstrap === 'undefined') return;

        const modal = bootstrap.Modal.getOrCreateInstance(modalEl);

        toggle.addEventListener('click', function (e) {
            e.preventDefault();
            modal.show();
        });

        document.getElementById('btnRegistryModalNewDomain')?.addEventListener('click', function () {
            if (typeof window.domainNew === 'function') {
                modal.hide();
                window.domainNew();
            }
        });

        modalEl.addEventListener('shown.bs.modal', function () {
            const active = modalEl.querySelector('#registryModalTabs .nav-link.active');
            const section = active?.dataset.registrySection || 'domains';
            document.dispatchEvent(new CustomEvent('sidebarSectionChanged', {
                detail: { section },
            }));
        });

        modalEl.querySelectorAll('#registryModalTabs [data-bs-toggle="tab"]').forEach(function (btn) {
            btn.addEventListener('shown.bs.tab', function () {
                const section = btn.dataset.registrySection;
                if (!section) return;
                document.dispatchEvent(new CustomEvent('sidebarSectionChanged', {
                    detail: { section },
                }));
            });
        });

        // Bookmarks: /?open=registry or legacy /registry/ redirect target
        const params = new URLSearchParams(window.location.search);
        if (params.get('open') === 'registry') {
            modal.show();
            params.delete('open');
            const qs = params.toString();
            const next = window.location.pathname + (qs ? '?' + qs : '') + window.location.hash;
            window.history.replaceState({}, '', next);
        }
    });
})();
