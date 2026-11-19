/**
 * App-chrome height bookkeeping.
 *
 * Keeps --ob-chrome-height in sync with the single top bar (#obSubnav), which
 * sidebar-layout uses for its offset. Former L1+L2 heights are merged into
 * one measurement.
 */

const Breadcrumb = {
    init() {
        this._updateChromeHeight();
    },

    _updateChromeHeight() {
        const chrome = document.getElementById('obSubnav')
            || document.querySelector('nav.navbar');
        const h = chrome ? chrome.offsetHeight : 48;
        document.documentElement.style.setProperty('--ob-chrome-height', h + 'px');
    }
};

window.OBBreadcrumb = Breadcrumb;
document.addEventListener('DOMContentLoaded', () => Breadcrumb.init());
