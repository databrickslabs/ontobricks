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
        '/dtwin/':    { label: 'Knowledge Graph', icon: 'bi-box-fill' },
        '/settings':  { label: 'Settings',     icon: 'bi-gear-fill' },
    },

    _HIERARCHY: ['/registry/', '/domain/', '/ontology/', '/mapping/', '/dtwin/'],

    init() {
        const wrap = document.getElementById('obBreadcrumbWrap');
        const list = document.getElementById('obBreadcrumbList');

        // Always compute chrome height so sidebar-layout uses the correct
        // offset even on pages where the breadcrumb stays hidden.
        this._updateChromeHeight();

        if (!wrap || !list) return;

        const path = window.location.pathname;
        const crumbs = this._buildCrumbs(path);
        if (crumbs.length <= 1) return;

        list.innerHTML = crumbs.map((c, i) => {
            const isLast = i === crumbs.length - 1;
            if (isLast) {
                return '<li class="breadcrumb-item active" aria-current="page">' + c.label + '</li>';
            }
            return '<li class="breadcrumb-item"><a href="' + c.href + '">' + c.label + '</a></li>';
        }).join('');

        wrap.classList.remove('d-none');
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
        const navbar  = document.querySelector('nav.navbar');
        const subnav  = document.getElementById('obSubnav');
        const navH    = navbar ? navbar.offsetHeight : 60;
        // Breadcrumb is now inside the subnav row, so subnav height covers both.
        const subnavH = (subnav && !subnav.classList.contains('d-none')) ? subnav.offsetHeight : 0;
        document.documentElement.style.setProperty(
            '--ob-chrome-height', (navH + subnavH) + 'px'
        );
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
        li.textContent = label;
        list.appendChild(li);
    },

    _escapeHtml(s) {
        return String(s).replace(/[&<>"']/g, (ch) => (
            { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]
        ));
    }
};

window.OBBreadcrumb = Breadcrumb;
document.addEventListener('DOMContentLoaded', () => Breadcrumb.init());
