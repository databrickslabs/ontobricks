/**
 * L2 chrome-height bookkeeping.
 *
 * The inline breadcrumb that used to sit in the L2 subnav row was removed.
 * This module only keeps the --ob-chrome-height CSS variable in sync with the
 * navbar + subnav heights, which sidebar-layout uses for its offset.
 */

const Breadcrumb = {
    init() {
        this._updateChromeHeight();
    },

    _updateChromeHeight() {
        const navbar  = document.querySelector('nav.navbar');
        const subnav  = document.getElementById('obSubnav');
        const navH    = navbar ? navbar.offsetHeight : 60;
        const subnavH = (subnav && !subnav.classList.contains('d-none')) ? subnav.offsetHeight : 0;
        document.documentElement.style.setProperty(
            '--ob-chrome-height', (navH + subnavH) + 'px'
        );
    }
};

window.OBBreadcrumb = Breadcrumb;
document.addEventListener('DOMContentLoaded', () => Breadcrumb.init());
