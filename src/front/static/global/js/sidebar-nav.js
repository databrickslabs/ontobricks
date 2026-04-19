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
        const onSectionChange = options.onSectionChange || null;
        const onBeforeSectionChange = options.onBeforeSectionChange || null;

        // Ensure collapse UI is set up (idempotent)
        SidebarNav._setupCollapse();

        document.querySelectorAll(`${navSelector} .nav-link[data-section]`).forEach(link => {
            link.addEventListener('click', async function(e) {
                e.preventDefault();
                const section = this.dataset.section;
                
                if (!section) return;
                
                if (onBeforeSectionChange && typeof onBeforeSectionChange === 'function') {
                    const shouldProceed = await onBeforeSectionChange(section);
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
                
                SidebarNav._pushSectionState(section);

                document.dispatchEvent(new CustomEvent('sidebarSectionChanged', {
                    detail: { section, targetSection }
                }));

                if (onSectionChange && typeof onSectionChange === 'function') {
                    onSectionChange(section, targetSection);
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
     * Update the URL query string to reflect the active section without a
     * full page reload, so that Back / Forward and bookmarks work.
     */
    _pushSectionState: function(section) {
        if (!section || !window.history || !window.history.pushState) return;
        const url = new URL(window.location);
        if (url.searchParams.get('section') === section) return;
        url.searchParams.set('section', section);
        window.history.pushState({ section: section }, '', url);
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
    _activateSection: function(sectionName) {
        const link = document.querySelector(`.sidebar-nav .nav-link[data-section="${sectionName}"]`);
        if (!link) return;
        document.querySelectorAll('.sidebar-nav .nav-link').forEach(l => l.classList.remove('active'));
        link.classList.add('active');
        document.querySelectorAll('.sidebar-section').forEach(s => s.classList.remove('active'));
        const target = document.getElementById(sectionName + '-section');
        if (target) target.classList.add('active');
        document.dispatchEvent(new CustomEvent('sidebarSectionChanged', {
            detail: { section: sectionName, targetSection: target }
        }));
    },

    /**
     * Toggle the sidebar collapsed state
     */
    toggleCollapse: function() {
        const nav = document.querySelector('.sidebar-nav');
        if (!nav) return;
        nav.classList.toggle('collapsed');
        const collapsed = nav.classList.contains('collapsed');
        try { localStorage.setItem(SidebarNav._STORAGE_KEY, collapsed ? '1' : '0'); } catch (_) { /* ignore */ }
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
        if (nav.querySelector('.sidebar-toggle')) return;
        const btn = document.createElement('button');
        btn.className = 'sidebar-toggle';
        btn.type = 'button';
        btn.title = 'Collapse / Expand sidebar';
        btn.innerHTML = '<i class="bi bi-layout-sidebar-inset"></i>';
        btn.addEventListener('click', () => SidebarNav.toggleCollapse());
        nav.insertBefore(btn, nav.firstChild);

        // Restore saved state (default: collapsed)
        try {
            const saved = localStorage.getItem(SidebarNav._STORAGE_KEY);
            if (saved !== '0') {
                nav.classList.add('collapsed');
            }
        } catch (_) {
            nav.classList.add('collapsed');
        }
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
            if (target) {
                const link = document.querySelector(`.sidebar-nav .nav-link[data-section="${target}"]`);
                if (link) {
                    setTimeout(() => {
                        SidebarNav._activateSection(target);
                        window.history.replaceState({ section: target }, '', window.location.href);
                    }, 100);
                }
            }
        }
    });
}
