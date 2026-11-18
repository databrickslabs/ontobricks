/**
 * OntoBricks - home.js
 *
 * Home page = Registry landing:
 *   - Three launch tiles open ``#homeRegistryModal`` (80% viewport) on
 *     Contexts / Bridges / My tasks.
 *   - Section state mirrored to ``?section=domains|bridges|tasks``.
 *   - Each section switch dispatches ``sidebarSectionChanged`` (source ``home``).
 *   - "Create a New Context" delegates to ``window.domainNew()``.
 *
 * Panes are toggled manually (not via Bootstrap Tab). The modal header
 * switcher is a ``btn-group``, which is not a valid Tab parent — calling
 * ``bootstrap.Tab.show()`` there throws ``Illegal invocation``.
 */
(function () {
    'use strict';

    const SECTIONS = ['domains', 'bridges', 'tasks'];
    const DEFAULT_SECTION = 'domains';
    const SECTION_TITLES = {
        domains: 'Contexts',
        bridges: 'Bridges',
        tasks: 'My tasks',
    };
    const PANE_IDS = {
        domains: 'home-pane-domains',
        bridges: 'home-pane-bridges',
        tasks: 'home-pane-tasks',
    };

    let modal = null;
    let currentSection = null;

    document.addEventListener('DOMContentLoaded', init);

    function init() {
        const tiles = document.getElementById('homeTabs');
        const modalEl = document.getElementById('homeRegistryModal');
        if (!tiles || !modalEl || typeof bootstrap === 'undefined') return;

        modal = bootstrap.Modal.getOrCreateInstance(modalEl);

        document.getElementById('btnHomeNewDomain')?.addEventListener('click', () => {
            if (typeof window.domainNew === 'function') window.domainNew();
        });

        loadContextCount();
        bindHelpResource();

        tiles.querySelectorAll('[data-home-section]').forEach((btn) => {
            btn.addEventListener('click', () => openSection(btn.dataset.homeSection));
        });

        document.querySelectorAll(
            '#homeRegistryModal .home-registry-switcher [data-home-section]'
        ).forEach((btn) => {
            btn.addEventListener('click', () => openSection(btn.dataset.homeSection));
        });

        modalEl.addEventListener('shown.bs.modal', () => {
            // Bridges graph needs a layout pass once the modal is visible.
            if (currentSection === 'bridges') {
                document.dispatchEvent(new CustomEvent('sidebarSectionChanged', {
                    detail: { section: 'bridges', source: 'home' },
                }));
            }
        });

        modalEl.addEventListener('hidden.bs.modal', () => {
            syncTileActive(null);
            clearSectionFromUrl();
            currentSection = null;
        });

        const requested = new URLSearchParams(window.location.search).get('section');
        if (SECTIONS.includes(requested)) {
            openSection(requested);
        }
    }

    /** Open the 80% modal on *section*. */
    function openSection(section) {
        if (!SECTIONS.includes(section)) section = DEFAULT_SECTION;
        applySection(section);
        if (modal) modal.show();
    }

    function applySection(section) {
        currentSection = section;
        activatePane(section);
        setModalTitle(section);
        syncTileActive(section);
        syncSwitcherActive(section);
        writeSectionToUrl(section);
        document.dispatchEvent(new CustomEvent('sidebarSectionChanged', {
            detail: { section: section, source: 'home' },
        }));
    }

    function activatePane(section) {
        Object.keys(PANE_IDS).forEach((key) => {
            const pane = document.getElementById(PANE_IDS[key]);
            if (!pane) return;
            const on = key === section;
            pane.classList.toggle('show', on);
            pane.classList.toggle('active', on);
        });
    }

    function syncTileActive(section) {
        document.querySelectorAll('#homeTabs [data-home-section]').forEach((btn) => {
            const on = section && btn.dataset.homeSection === section;
            btn.classList.toggle('active', on);
            btn.setAttribute('aria-selected', on ? 'true' : 'false');
        });
    }

    function syncSwitcherActive(section) {
        document.querySelectorAll(
            '#homeRegistryModal .home-registry-switcher [data-home-section]'
        ).forEach((btn) => {
            const on = btn.dataset.homeSection === section;
            btn.classList.toggle('active', on);
            btn.setAttribute('aria-pressed', on ? 'true' : 'false');
        });
    }

    function setModalTitle(section) {
        const el = document.getElementById('homeRegistryModalTitle');
        if (el) el.textContent = SECTION_TITLES[section] || SECTION_TITLES[DEFAULT_SECTION];
    }

    /** Open the in-app Help Center from the Home resources strip. */
    function bindHelpResource() {
        const btn = document.getElementById('homeOpenHelp');
        if (!btn) return;
        btn.addEventListener('click', () => {
            if (window.HelpCenter && typeof window.HelpCenter.open === 'function') {
                window.HelpCenter.open('welcome');
                return;
            }
            document.getElementById('helpCenterToggle')?.click();
        });
    }

    /** Contexts tile count: number of contexts visible in the registry. */
    async function loadContextCount() {
        const el = document.getElementById('homeCountContexts');
        if (!el) return;
        try {
            const resp = await fetch('/domain/list-projects', { credentials: 'same-origin' });
            const data = await resp.json();
            el.textContent = (data && data.success && Array.isArray(data.domains))
                ? String(data.domains.length)
                : '\u2014';
        } catch (err) {
            console.error('home context count error:', err);
            el.textContent = '\u2014';
        }
    }

    function writeSectionToUrl(section) {
        const url = new URL(window.location.href);
        url.searchParams.set('section', section);
        window.history.replaceState({}, '', url.href);
    }

    function clearSectionFromUrl() {
        const url = new URL(window.location.href);
        url.searchParams.delete('section');
        window.history.replaceState({}, '', url.href);
    }
})();
