/**
 * Sidebar Navigation - Reusable Component
 * 
 * Usage:
 * 1. Include this script in your page
 * 2. Use the following HTML structure:
 *    <div class="sidebar-layout">
 *      <div class="sidebar-nav">
 *        <a class="nav-link active" href="#" data-section="section-name">...</a>
 *      </div>
 *      <div class="sidebar-content">
 *        <div id="section-name-section" class="sidebar-section active">...</div>
 *      </div>
 *    </div>
 * 3. Call SidebarNav.init() or let it auto-initialize on DOMContentLoaded
 */

const SidebarNav = {
    _STORAGE_KEY: 'ontobricks-sidebar-collapsed',
    _initialized: false,
    _onSectionChange: null,
    _onBeforeSectionChange: null,

    /**
     * Initialize sidebar navigation
     * @param {Object} options - Configuration options
     * @param {string} options.navSelector - Selector for nav container (default: '.sidebar-nav')
     * @param {string} options.sectionSelector - Selector for section containers (default: '.sidebar-section')
     * @param {Function} options.onSectionChange - Callback when section changes
     */
    init: function(options = {}) {
        const navSelector = options.navSelector || '.sidebar-nav';
        const sectionSelector = options.sectionSelector || '.sidebar-section';
        if (Object.prototype.hasOwnProperty.call(options, 'onSectionChange')) {
            SidebarNav._onSectionChange = options.onSectionChange;
        }
        if (Object.prototype.hasOwnProperty.call(options, 'onBeforeSectionChange')) {
            SidebarNav._onBeforeSectionChange = options.onBeforeSectionChange;
        }

        // Ensure collapse UI is set up (idempotent)
        SidebarNav._setupCollapse();

        // Avoid duplicate listeners if a page re-initializes with updated callbacks.
        if (SidebarNav._initialized) {
            return;
        }
        SidebarNav._initialized = true;

        document.querySelectorAll(`${navSelector} .nav-link[data-section]`).forEach(link => {
            link.addEventListener('click', async function(e) {
                window.bootstrap?.Tooltip?.getInstance(this)?.hide();
                e.preventDefault();
                const section = this.dataset.section;
                
                if (!section) return;
                
                const beforeHook = SidebarNav._onBeforeSectionChange;
                if (beforeHook && typeof beforeHook === 'function') {
                    const shouldProceed = await beforeHook(section);
                    if (shouldProceed === false) {
                        return;
                    }
                }
                
                document.querySelectorAll(`${navSelector} .nav-link`).forEach(l => {
                    l.classList.remove('active');
                });
                this.classList.add('active');
                
                document.querySelectorAll(sectionSelector).forEach(s => {
                    s.classList.remove('active');
                });
                
                const targetSection = document.getElementById(section + '-section');
                if (targetSection) {
                    targetSection.classList.add('active');
                }

                SidebarNav._clearInitialSectionStyle();
                SidebarNav._pushSectionState(section);

                document.dispatchEvent(new CustomEvent('sidebarSectionChanged', {
                    detail: { section, targetSection }
                }));

                const changeHook = SidebarNav._onSectionChange;
                if (changeHook && typeof changeHook === 'function') {
                    changeHook(section, targetSection, { isTrusted: !!e.isTrusted });
                }
            });
        });
    },

    /**
     * Programmatically switch to a section
     * @param {string} sectionName - Name of the section to switch to
     */
    switchTo: function(sectionName) {
        const link = document.querySelector(`.sidebar-nav .nav-link[data-section="${sectionName}"]`);
        if (link) {
            link.click();
            return;
        }
        if (SidebarNav._showSection(sectionName)) {
            SidebarNav._pushSectionState(sectionName);
        }
    },

    /**
     * Get the current active section name
     * @returns {string|null} The name of the active section
     */
    getActiveSection: function() {
        const activeLink = document.querySelector('.sidebar-nav .nav-link.active');
        return activeLink ? activeLink.dataset.section : null;
    },

    /**
     * ``?tab=`` is owned by a specific section (Build/Runs on sync, GraphQL/SPARQL
     * on graphql, …). Drop it when navigating away so URLs do not stick as
     * ``?section=design&tab=build`` after leaving Build.
     */
    _TAB_OWNERS: {
        sync: ['build', 'runs'],
        graphql: ['graphql', 'sparql'],
        analytics: ['analytics', 'runs'],
        cohorts: ['design', 'runs'],
        design: ['informations', 'visual', 'list']
    },

    /**
     * Update the URL query string to reflect the active section without a
     * full page reload, so that Back / Forward and bookmarks work.
     */
    _pushSectionState: function(section) {
        if (!section || !window.history || !window.history.pushState) return;
        const url = new URL(window.location);
        const prevSection = url.searchParams.get('section');
        const tab = url.searchParams.get('tab');
        const ownerTabs = SidebarNav._TAB_OWNERS[section];
        let changed = false;
        if (prevSection !== section) {
            url.searchParams.set('section', section);
            changed = true;
        }
        if (tab && (!ownerTabs || ownerTabs.indexOf(tab) === -1)) {
            url.searchParams.delete('tab');
            changed = true;
        }
        if (!changed) return;
        window.history.pushState({ section: section }, '', url);
    },

    /**
     * Remove the pre-paint flash-prevention style from mapping-init-section.js
     * (and similar). That style uses ``display: … !important`` and would otherwise
     * permanently lock the first URL section on Graph after Mapping was merged in.
     */
    _clearInitialSectionStyle: function() {
        const tempStyle = document.getElementById('initial-section-style');
        if (tempStyle) tempStyle.remove();
    },

    /**
     * Restore the visible section from a popstate event (Back / Forward).
     */
    _onPopState: function(e) {
        const section = (e.state && e.state.section)
            || new URLSearchParams(window.location.search).get('section');
        if (section) {
            SidebarNav._activateSection(section);
        }
    },

    /**
     * Activate a section by id without pushing a new history entry.
     */
    _showSection: function(sectionName) {
        const target = document.getElementById(sectionName + '-section');
        if (!target) return null;
        document.querySelectorAll('.sidebar-nav .nav-link').forEach(l => l.classList.remove('active'));
        const link = document.querySelector(`.sidebar-nav .nav-link[data-section="${sectionName}"]`);
        if (link) link.classList.add('active');
        document.querySelectorAll('.sidebar-section').forEach(s => s.classList.remove('active'));
        target.classList.add('active');
        SidebarNav._clearInitialSectionStyle();
        document.dispatchEvent(new CustomEvent('sidebarSectionChanged', {
            detail: { section: sectionName, targetSection: target }
        }));
        return target;
    },

    _activateSection: function(sectionName) {
        SidebarNav._showSection(sectionName);
    },

    /**
     * Apply collapsed preference to the nav + html class (pre-paint twin).
     */
    _applyCollapsed: function(collapsed) {
        const nav = document.querySelector('.sidebar-nav');
        if (nav) nav.classList.toggle('collapsed', collapsed);
        document.documentElement.classList.toggle('ob-sidebar-collapsed', collapsed);
    },

    /**
     * Toggle the sidebar collapsed state
     */
    toggleCollapse: function() {
        const nav = document.querySelector('.sidebar-nav');
        if (!nav) return;
        const collapsed = !nav.classList.contains('collapsed');
        SidebarNav._applyCollapsed(collapsed);
        try { localStorage.setItem(SidebarNav._STORAGE_KEY, collapsed ? '1' : '0'); } catch (_) { /* ignore */ }
        SidebarNav._updateTooltips();
    },

    /**
     * Update tooltips for collapsed sidebar navigation links.
     * When collapsed, hover shows a tooltip with the feature name to the right.
     * When expanded, tooltips are disabled so they do not duplicate the visible label.
     */
    _updateTooltips: function() {
        const nav = document.querySelector('.sidebar-nav');
        if (!nav) return;
        const isCollapsed = nav.classList.contains('collapsed');
        nav.querySelectorAll('.nav-link').forEach(link => {
            let label = link.getAttribute('data-sidebar-title');
            if (!label) {
                const labelEl = link.querySelector('.nav-label');
                label = labelEl ? labelEl.textContent.trim() : link.textContent.trim();
                if (label) link.setAttribute('data-sidebar-title', label);
            }
            if (!label) return;

            let tip = window.bootstrap?.Tooltip?.getInstance(link);
            if (!tip && window.bootstrap?.Tooltip) {
                tip = new window.bootstrap.Tooltip(link, {
                    title: () => link.getAttribute('data-sidebar-title') || '',
                    placement: 'right',
                    trigger: 'hover',
                    container: 'body',
                });
            }

            if (tip) {
                if (isCollapsed) {
                    tip.enable();
                } else {
                    tip.hide();
                    tip.disable();
                }
            } else {
                if (isCollapsed) {
                    link.setAttribute('title', label);
                } else {
                    link.removeAttribute('title');
                }
            }
        });
    },

    /**
     * Inject the toggle button and wrap nav-link text nodes in <span class="nav-label">
     * so they can be hidden when the sidebar is collapsed.
     */
    _setupCollapse: function() {
        const nav = document.querySelector('.sidebar-nav');
        if (!nav) return;

        // Wrap bare text nodes inside nav-links with <span class="nav-label">
        nav.querySelectorAll('.nav-link').forEach(link => {
            if (link.querySelector('.nav-label')) return;
            const children = Array.from(link.childNodes);
            let labelSpan = null;
            children.forEach(node => {
                if (node.nodeType === Node.TEXT_NODE && node.textContent.trim()) {
                    if (!labelSpan) {
                        labelSpan = document.createElement('span');
                        labelSpan.className = 'nav-label';
                    }
                    labelSpan.appendChild(node);
                }
            });
            if (labelSpan) {
                // Insert after the <i> icon if present, otherwise append
                const icon = link.querySelector('i');
                if (icon && icon.nextSibling) {
                    link.insertBefore(labelSpan, icon.nextSibling);
                } else {
                    link.appendChild(labelSpan);
                }
            }
            // Also wrap badges
            const badge = link.querySelector('.badge');
            if (badge && !badge.closest('.nav-label')) {
                let existing = link.querySelector('.nav-label');
                if (existing) {
                    existing.appendChild(document.createTextNode(' '));
                    existing.appendChild(badge);
                }
            }
        });

        // Inject toggle button at the top of the sidebar
        if (!nav.querySelector('.sidebar-toggle')) {
            const btn = document.createElement('button');
            btn.className = 'sidebar-toggle';
            btn.type = 'button';
            btn.title = 'Collapse / Expand sidebar';
            btn.innerHTML = '<i class="bi bi-layout-sidebar-inset"></i>';
            btn.addEventListener('click', () => SidebarNav.toggleCollapse());
            nav.insertBefore(btn, nav.firstChild);
        }

        // Restore saved state (default: collapsed). Prefer the html class already
        // set by sidebar-collapse-init.js so first paint and JS stay aligned.
        let collapsed = document.documentElement.classList.contains('ob-sidebar-collapsed');
        try {
            const saved = localStorage.getItem(SidebarNav._STORAGE_KEY);
            collapsed = saved !== '0';
        } catch (_) {
            collapsed = true;
        }
        SidebarNav._applyCollapsed(collapsed);
        document.documentElement.classList.add('ob-sidebar-ready');

        SidebarNav._updateTooltips();
    }
};

// Auto-initialize when DOM is ready (can be disabled by setting window.SIDEBAR_NAV_MANUAL_INIT = true)
if (typeof window !== 'undefined' && !window.SIDEBAR_NAV_MANUAL_INIT) {
    document.addEventListener('DOMContentLoaded', function() {
        if (document.querySelector('.sidebar-nav')) {
            SidebarNav._setupCollapse();
            SidebarNav.init();

            window.addEventListener('popstate', SidebarNav._onPopState);

            const params = new URLSearchParams(window.location.search);
            const target = params.get('section') || window.location.hash.substring(1);
            if (target && document.getElementById(target + '-section')) {
                setTimeout(() => {
                    SidebarNav._activateSection(target);
                    window.history.replaceState({ section: target }, '', window.location.href);
                }, 100);
            }
        }
    });
}
