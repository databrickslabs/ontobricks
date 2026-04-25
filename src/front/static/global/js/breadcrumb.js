/**
 * Breadcrumb — auto-populated from the current URL path, loaded domain
 * name, and active sidebar section.
 */

const Breadcrumb = {
    _ROUTE_MAP: {
        '/registry/': { label: 'Registry', icon: 'bi-folder2-open' },
        '/domain/':   { label: 'Domain',   icon: 'bi-box' },
        '/ontology/': { label: 'Ontology', icon: 'bi-diagram-3' },
        '/mapping/':  { label: 'Mapping',  icon: 'bi-link-45deg' },
        '/dtwin/':    { label: 'Digital Twin', icon: 'bi-share' },
        '/settings':  { label: 'Settings', icon: 'bi-gear' },
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
                icon: 'bi-box',
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
        const bcHeight = nav.offsetHeight;
        const base = document.body.classList.contains('read-only-version') ? 100 : 60;
        document.documentElement.style.setProperty('--ob-chrome-height', (base + bcHeight) + 'px');
        document.documentElement.style.setProperty('--ob-chrome-height-ro', (100 + bcHeight) + 'px');
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

        const last = list.querySelector('.breadcrumb-item.active');
        if (last) last.classList.remove('active');

        const li = document.createElement('li');
        li.className = 'breadcrumb-item active breadcrumb-section';
        li.setAttribute('aria-current', 'page');
        li.textContent = label;
        list.appendChild(li);
    }
};

document.addEventListener('DOMContentLoaded', () => Breadcrumb.init());
