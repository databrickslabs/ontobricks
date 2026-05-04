/**
 * Breadcrumb — auto-populated from the current URL path, loaded domain
 * name, and active sidebar section.
 */

const Breadcrumb = {
    // Icons mirror the top-level entries in src/front/config/menu_config.json
    // so the breadcrumb visually matches the navbar/sidebar menus.
    _ROUTE_MAP: {
        '/registry/': { label: 'Registry',     icon: 'bi-archive' },
        '/domain/':   { label: 'Domain',       icon: 'bi-folder2' },
        '/ontology/': { label: 'Ontology',     icon: 'bi-bezier2' },
        '/mapping/':  { label: 'Mapping',      icon: 'bi-shuffle' },
        '/dtwin/':    { label: 'Digital Twin', icon: 'bi-box-fill' },
        '/settings':  { label: 'Settings',     icon: 'bi-gear-fill' },
    },

    _HIERARCHY: ['/registry/', '/domain/', '/ontology/', '/mapping/', '/dtwin/'],

    init() {
        const nav = document.getElementById('obBreadcrumb');
        const list = document.getElementById('obBreadcrumbList');
        if (!nav || !list) return;

        const path = window.location.pathname;
        const crumbs = this._buildCrumbs(path);
        if (crumbs.length <= 1) return;

        list.innerHTML = crumbs.map((c, i) => {
            const isLast = i === crumbs.length - 1;
            if (isLast) {
                return '<li class="breadcrumb-item active" aria-current="page">' +
                    '<i class="bi ' + (c.icon || '') + ' me-1"></i>' + c.label + '</li>';
            }
            return '<li class="breadcrumb-item">' +
                '<a href="' + c.href + '"><i class="bi ' + (c.icon || '') + ' me-1"></i>' +
                c.label + '</a></li>';
        }).join('');

        nav.classList.remove('d-none');
        this._updateChromeHeight();

        document.addEventListener('sidebarSectionChanged', (e) => this._updateSection(e.detail.section));

        const params = new URLSearchParams(window.location.search);
        const section = params.get('section');
        if (section) this._updateSection(section);
    },

    _buildCrumbs(path) {
        const crumbs = [];

        const matched = this._ROUTE_MAP[path] || this._ROUTE_MAP[path + '/'];
        if (!matched) return crumbs;

        const idx = this._HIERARCHY.indexOf(path.endsWith('/') ? path : path + '/');

        if (idx > 0) {
            crumbs.push({ label: 'Registry', icon: 'bi-folder2-open', href: '/registry/' });
        }
        if (idx > 1) {
            const domainName = this._getDomainName();
            crumbs.push({
                label: domainName || 'Domain',
                icon: 'bi-folder2',
                href: '/domain/'
            });
        }

        crumbs.push({ label: matched.label, icon: matched.icon, href: path });

        return crumbs;
    },

    _getDomainName() {
        const el = document.getElementById('currentDomainName');
        if (!el) return '';
        const text = el.textContent.trim();
        return (text && text !== 'Domain') ? text : '';
    },

    _updateChromeHeight() {
        const nav = document.getElementById('obBreadcrumb');
        if (!nav || nav.classList.contains('d-none')) return;
        // Chrome = navbar (60px) + breadcrumb. Read-only no longer adds
        // a banner — the indicator is now a navbar pill — so the same
        // height applies whether or not the user is in read-only mode.
        const bcHeight = nav.offsetHeight;
        document.documentElement.style.setProperty('--ob-chrome-height', (60 + bcHeight) + 'px');
    },

    _updateSection(sectionName) {
        const list = document.getElementById('obBreadcrumbList');
        if (!list) return;

        const existing = list.querySelector('.breadcrumb-section');
        if (existing) existing.remove();

        if (!sectionName) return;

        const activeLink = document.querySelector(
            '.sidebar-nav .nav-link[data-section="' + sectionName + '"]'
        );
        if (!activeLink) return;

        const labelEl = activeLink.querySelector('.nav-label');
        const label = labelEl ? labelEl.textContent.trim() : activeLink.textContent.trim();

        // Pick up the sidebar item's bi-* icon so the section crumb mirrors
        // the menu (driven by menu_config.json).
        const iconEl = activeLink.querySelector('i.bi');
        let iconClass = '';
        if (iconEl) {
            iconClass = Array.from(iconEl.classList)
                .find(c => c.startsWith('bi-')) || '';
        }

        const last = list.querySelector('.breadcrumb-item.active');
        if (last) last.classList.remove('active');

        const li = document.createElement('li');
        li.className = 'breadcrumb-item active breadcrumb-section';
        li.setAttribute('aria-current', 'page');
        if (iconClass) {
            const iconHtml = '<i class="bi ' + iconClass + ' me-1"></i>';
            li.innerHTML = iconHtml + this._escapeHtml(label);
        } else {
            li.textContent = label;
        }
        list.appendChild(li);
    },

    _escapeHtml(s) {
        return String(s).replace(/[&<>"']/g, (ch) => (
            { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]
        ));
    }
};

document.addEventListener('DOMContentLoaded', () => Breadcrumb.init());
