/**
 * OntoBricks Utility Functions
 * Common JavaScript utilities for the application
 */

// ========== NOTIFICATION CENTER ==========

const NotificationCenter = {
    messages: [],
    unreadCount: 0,
    MAX_MESSAGES: parseInt(document.documentElement.dataset.maxNotifications, 10) || 10,

    TYPE_CONFIG: {
        'success': { icon: 'bi-check-circle-fill', color: '#198754', label: 'Success' },
        'error':   { icon: 'bi-exclamation-triangle-fill', color: '#dc3545', label: 'Error' },
        'warning': { icon: 'bi-exclamation-circle-fill', color: '#ffc107', label: 'Warning' },
        'info':    { icon: 'bi-info-circle-fill', color: '#0d6efd', label: 'Info' }
    },

    add(message, type) {
        const cfg = this.TYPE_CONFIG[type] || this.TYPE_CONFIG.info;
        this.messages.unshift({
            id: Date.now() + Math.random(),
            message,
            type,
            icon: cfg.icon,
            color: cfg.color,
            time: new Date()
        });
        if (this.messages.length > this.MAX_MESSAGES) this.messages.pop();

        this.unreadCount++;
        this._updateBadge();
        this._renderList();
        this._flashIcon();
    },

    markAllRead() {
        this.unreadCount = 0;
        this._updateBadge();
    },

    clearAll() {
        this.messages = [];
        this.unreadCount = 0;
        this._updateBadge();
        this._renderList();
    },

    toggle(event) {
        if (event) { event.preventDefault(); event.stopPropagation(); }
        const dropdown = document.getElementById('notifCenterDropdown');
        const toggle = document.getElementById('notifCenterToggle');
        if (!dropdown) return;
        const isOpen = dropdown.classList.contains('show');
        // Close task-tracker if open
        const taskDd = document.getElementById('taskTrackerDropdown');
        if (taskDd) taskDd.classList.remove('show');

        if (isOpen) {
            dropdown.classList.remove('show');
        } else {
            this._positionDropdown(dropdown, toggle);
            dropdown.classList.add('show');
            this.markAllRead();
        }
    },

    _positionDropdown(dropdown, toggle) {
        if (!toggle) return;
        const rect = toggle.getBoundingClientRect();
        dropdown.style.position = 'fixed';
        dropdown.style.top = (rect.bottom + 4) + 'px';
        dropdown.style.right = (window.innerWidth - rect.right) + 'px';
        dropdown.style.left = 'auto';
    },

    _updateBadge() {
        const badge = document.getElementById('notifCenterBadge');
        if (!badge) return;
        if (this.unreadCount > 0) {
            badge.textContent = this.unreadCount > 99 ? '99+' : this.unreadCount;
            badge.style.display = '';
        } else {
            badge.style.display = 'none';
        }
    },

    _flashIcon() {
        const icon = document.getElementById('notifCenterIcon');
        if (!icon) return;
        icon.classList.add('notif-flash');
        setTimeout(() => icon.classList.remove('notif-flash'), 600);
    },

    _timeAgo(date) {
        const secs = Math.floor((Date.now() - date.getTime()) / 1000);
        if (secs < 5) return 'just now';
        if (secs < 60) return secs + 's ago';
        const mins = Math.floor(secs / 60);
        if (mins < 60) return mins + 'm ago';
        const hrs = Math.floor(mins / 60);
        if (hrs < 24) return hrs + 'h ago';
        return date.toLocaleDateString();
    },

    _renderList() {
        const list = document.getElementById('notifCenterList');
        if (!list) return;

        if (this.messages.length === 0) {
            list.innerHTML =
                '<div class="text-center text-muted py-4">' +
                '<i class="bi bi-bell-slash fs-3 d-block mb-2"></i>' +
                '<small>No notifications</small></div>';
            return;
        }

        list.innerHTML = this.messages.map(m =>
            '<div class="notif-item d-flex align-items-start px-3 py-2 border-bottom">' +
            '<i class="bi ' + m.icon + ' me-2 mt-1" style="color:' + m.color + ';"></i>' +
            '<div class="flex-grow-1 small" style="word-break:break-word;">' + m.message +
            '<div class="text-muted" style="font-size:0.7rem;">' + this._timeAgo(m.time) + '</div>' +
            '</div></div>'
        ).join('');
    }
};

/**
 * Show a notification message (pushes into the Notification Center).
 * Errors also display a temporary floating toast so they are immediately visible.
 * @param {string} message - The message to display
 * @param {string} type - 'success', 'error', 'warning', 'info'
 * @param {number} duration - For errors: toast display time in ms (default 6000)
 */
function showNotification(message, type = 'info', duration) {
    var isProgress = typeof message === 'string' && /\.\.\.\s*$/.test(message);
    if (!isProgress) {
        NotificationCenter.add(message, type);
    }

    var TOAST_CONF = {
        error:   { css: 'error-toast-popup',   icon: 'bi-exclamation-triangle-fill', closeWhite: true,  defDur: 6000 },
        warning: { css: 'warning-toast-popup',  icon: 'bi-exclamation-circle-fill',   closeWhite: false, defDur: 4000 },
        success: { css: 'success-toast-popup',  icon: 'bi-check-circle-fill',         closeWhite: true,  defDur: 3000 },
    };
    var conf = TOAST_CONF[type];
    if (conf) {
        _showToast(message, conf.css, conf.icon, conf.closeWhite, duration || conf.defDur);
    }
}

function _showToast(message, cssClass, iconClass, closeWhite, duration) {
    var container = document.getElementById('errorToastContainer');
    if (!container) {
        container = document.createElement('div');
        container.id = 'errorToastContainer';
        document.body.appendChild(container);
    }
    var toast = document.createElement('div');
    toast.className = cssClass + ' fade show d-flex align-items-center';
    toast.innerHTML =
        '<i class="bi ' + iconClass + ' me-2"></i>' +
        '<div class="flex-grow-1">' + message + '</div>' +
        '<button type="button" class="btn-close' + (closeWhite ? ' btn-close-white' : '') + ' ms-2" aria-label="Close"></button>';
    toast.querySelector('.btn-close').addEventListener('click', function() {
        toast.classList.remove('show');
        setTimeout(function() { toast.remove(); }, 150);
    });
    container.appendChild(toast);
    setTimeout(function() {
        toast.classList.remove('show');
        setTimeout(function() { toast.remove(); }, 150);
    }, duration);
}

// Close notification dropdown when clicking outside
document.addEventListener('click', function(e) {
    const dropdown = document.getElementById('notifCenterDropdown');
    const toggle = document.getElementById('notifCenterToggle');
    if (dropdown && dropdown.classList.contains('show') &&
        !dropdown.contains(e.target) && toggle && !toggle.contains(e.target)) {
        dropdown.classList.remove('show');
    }
});


// ========== CSRF UTILITY ==========

function _getCSRFToken() {
    const m = document.cookie.match(/(?:^|;\s*)csrf_token=([^;]+)/);
    return m ? m[1] : '';
}

// ========== API UTILITIES ==========

/**
 * Make an API request with error handling
 * @param {string} url - API endpoint URL
 * @param {Object} options - Fetch options
 * @returns {Promise<Object>} Response data
 */
async function apiRequest(url, options = {}) {
    try {
        const csrfToken = _getCSRFToken();
        const defaultOptions = {
            headers: {
                'Content-Type': 'application/json'
            },
            credentials: 'same-origin'
        };
        if (csrfToken) {
            defaultOptions.headers['X-CSRF-Token'] = csrfToken;
        }
        
        const mergedOptions = {
            ...defaultOptions,
            ...options,
            headers: {
                ...defaultOptions.headers,
                ...options.headers
            },
            credentials: options.credentials || 'same-origin'
        };
        
        const response = await fetch(url, mergedOptions);
        const data = await response.json();
        
        if (!response.ok) {
            throw new Error(data.message || data.error || `HTTP ${response.status}`);
        }
        
        return data;
    } catch (error) {
        console.error(`API Error (${url}):`, error);
        throw error;
    }
}


// ========== FORM UTILITIES ==========

/**
 * Populate a select element with options
 * @param {HTMLSelectElement} select - The select element
 * @param {Array} options - Array of options (strings or {value, label} objects)
 * @param {string} placeholder - Placeholder text
 */
function populateSelect(select, options, placeholder = 'Select...') {
    select.innerHTML = `<option value="">${placeholder}</option>`;
    
    options.forEach(option => {
        const opt = document.createElement('option');
        if (typeof option === 'string') {
            opt.value = option;
            opt.textContent = option;
        } else {
            opt.value = option.value;
            opt.textContent = option.label || option.value;
        }
        select.appendChild(opt);
    });
    
    select.disabled = false;
}


// ========== DATABRICKS UTILITIES ==========

/**
 * Load catalogs from Databricks
 * @returns {Promise<Array>} List of catalog names
 */
async function loadCatalogs() {
    try {
        const data = await apiRequest('/settings/catalogs');
        return data.catalogs || [];
    } catch (error) {
        console.error('Error loading catalogs:', error);
        return [];
    }
}


/**
 * Load schemas for a catalog
 * @param {string} catalog - Catalog name
 * @returns {Promise<Array>} List of schema names
 */
async function loadSchemas(catalog) {
    try {
        const data = await apiRequest(`/settings/schemas/${catalog}`);
        return data.schemas || [];
    } catch (error) {
        console.error('Error loading schemas:', error);
        return [];
    }
}


/**
 * Load volumes for a catalog and schema
 * @param {string} catalog - Catalog name
 * @param {string} schema - Schema name
 * @returns {Promise<Array>} List of volume names
 */
async function loadVolumes(catalog, schema) {
    try {
        const data = await apiRequest(`/settings/volumes/${catalog}/${schema}`);
        return data.volumes || [];
    } catch (error) {
        console.error('Error loading volumes:', error);
        return [];
    }
}


// ========== HTML UTILITIES ==========

/**
 * Escape HTML special characters to prevent XSS.
 * Canonical implementation — all modules should use this instead of local copies.
 * @param {*} text - Text to escape
 * @returns {string} Escaped HTML string
 */
function escapeHtml(text) {
    if (text === null || text === undefined) return '';
    const div = document.createElement('div');
    div.textContent = String(text);
    return div.innerHTML;
}


// ========== STRING UTILITIES ==========

/**
 * Truncate a string to a maximum length
 * @param {string} str - The string to truncate
 * @param {number} maxLength - Maximum length
 * @returns {string} Truncated string
 */
function truncate(str, maxLength = 50) {
    if (!str) return '';
    if (str.length <= maxLength) return str;
    return str.substring(0, maxLength - 3) + '...';
}


/**
 * Extract local name from a URI
 * @param {string} uri - The URI
 * @returns {string} Local name
 */
function extractLocalName(uri) {
    if (!uri) return '';
    if (uri.includes('#')) {
        return uri.split('#').pop();
    }
    return uri.split('/').pop();
}


// ========== CONFIRMATION DIALOG ==========

/**
 * Show a confirmation dialog (replaces native confirm())
 * @param {Object} options - Dialog options
 * @param {string} options.title - Dialog title (default: 'Confirm')
 * @param {string} options.message - Message to display
 * @param {string} options.confirmText - Confirm button text (default: 'Yes')
 * @param {string} options.cancelText - Cancel button text (default: 'Cancel')
 * @param {string} options.confirmClass - Bootstrap button class for confirm (default: 'btn-primary')
 * @param {string} options.icon - Bootstrap icon name (default: 'question-circle')
 * @returns {Promise<boolean>} Resolves to true if confirmed, false if cancelled
 */
function showConfirmDialog(options = {}) {
    return new Promise((resolve) => {
        const {
            title = 'Confirm',
            message = 'Are you sure?',
            confirmText = 'Yes',
            cancelText = 'Cancel',
            confirmClass = 'btn-primary',
            icon = 'question-circle'
        } = options;
        
        const modalId = 'confirmDialog_' + Date.now();
        
        const modalHtml = `
            <div class="modal fade" id="${modalId}" tabindex="-1" data-bs-backdrop="static">
                <div class="modal-dialog modal-dialog-centered">
                    <div class="modal-content">
                        <div class="modal-header">
                            <h5 class="modal-title">
                                <i class="bi bi-${icon} me-2"></i>${title}
                            </h5>
                            <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                        </div>
                        <div class="modal-body">
                            <p class="mb-0">${message}</p>
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal" id="${modalId}_cancel">
                                ${cancelText}
                            </button>
                            <button type="button" class="btn ${confirmClass}" id="${modalId}_confirm">
                                ${confirmText}
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        // Remove any existing dialog with same ID
        const existing = document.getElementById(modalId);
        if (existing) existing.remove();
        
        // Add modal to page
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        
        const modalEl = document.getElementById(modalId);
        const modal = new bootstrap.Modal(modalEl);
        
        let resolved = false;
        
        // Handle confirm
        document.getElementById(`${modalId}_confirm`).addEventListener('click', () => {
            resolved = true;
            modal.hide();
            resolve(true);
        });
        
        // Handle cancel/close
        modalEl.addEventListener('hidden.bs.modal', () => {
            if (!resolved) {
                resolve(false);
            }
            // Clean up modal from DOM
            setTimeout(() => modalEl.remove(), 100);
        });
        
        modal.show();
    });
}

/**
 * Show a delete confirmation dialog (red themed)
 * @param {string} itemName - Name of item being deleted
 * @param {string} itemType - Type of item (e.g., 'entity', 'relationship')
 * @returns {Promise<boolean>}
 */
function showDeleteConfirm(itemName, itemType = 'item') {
    return showConfirmDialog({
        title: 'Confirm Delete',
        message: `Are you sure you want to delete ${itemType} "<strong>${itemName}</strong>"? This action cannot be undone.`,
        confirmText: 'Delete',
        cancelText: 'Cancel',
        confirmClass: 'btn-danger',
        icon: 'trash'
    });
}

// ========== DOCUMENT PREVIEW ==========

const DocumentPreview = {
    _modalId: 'docPreviewModal',

    _ensureModal() {
        let el = document.getElementById(this._modalId);
        if (el) return el;
        el = document.createElement('div');
        el.id = this._modalId;
        el.className = 'modal fade';
        el.tabIndex = -1;
        el.innerHTML = `
            <div class="modal-dialog modal-xl modal-dialog-centered">
                <div class="modal-content">
                    <div class="modal-header py-2">
                        <h6 class="modal-title" id="docPreviewTitle">
                            <i class="bi bi-eye me-2"></i>Document Preview
                        </h6>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body p-0" id="docPreviewBody"
                         style="min-height:400px; max-height:80vh; overflow:auto;">
                    </div>
                </div>
            </div>`;
        document.body.appendChild(el);
        return el;
    },

    _extOf(filename) {
        return (filename.includes('.') ? filename.split('.').pop() : '').toLowerCase();
    },

    _isBinary(ext) {
        return ['pdf', 'png', 'jpg', 'jpeg', 'gif', 'svg'].includes(ext);
    },

    _isImage(ext) {
        return ['png', 'jpg', 'jpeg', 'gif', 'svg'].includes(ext);
    },

    open(filename) {
        const modalEl = this._ensureModal();
        const title = document.getElementById('docPreviewTitle');
        const body = document.getElementById('docPreviewBody');
        const ext = this._extOf(filename);

        title.innerHTML = `<i class="bi bi-eye me-2"></i>${escapeHtml(filename)}`;
        body.innerHTML = `<div class="d-flex justify-content-center align-items-center" style="min-height:300px;">
            <div class="spinner-border spinner-border-sm text-secondary" role="status"></div>
        </div>`;

        const modal = bootstrap.Modal.getOrCreateInstance(modalEl);
        modal.show();

        const url = `/domain/documents/preview/${encodeURIComponent(filename)}`;

        if (ext === 'pdf') {
            body.innerHTML = `<iframe class="doc-preview-iframe" src="${url}"></iframe>`;
            return;
        }

        if (this._isImage(ext)) {
            body.innerHTML = `<div class="text-center p-3">
                <img src="${url}" class="doc-preview-image" alt="${escapeHtml(filename)}">
            </div>`;
            return;
        }

        fetch(url, { credentials: 'same-origin' })
            .then(r => r.json())
            .then(data => {
                if (!data.success) {
                    body.innerHTML = `<div class="p-4 text-center text-muted">
                        <i class="bi bi-file-earmark-x fs-1 d-block mb-2"></i>
                        ${escapeHtml(data.message || 'Preview not available')}
                    </div>`;
                    return;
                }
                if (ext === 'md') {
                    body.innerHTML = `<div class="doc-preview-md p-4">${this._renderMarkdown(data.content)}</div>`;
                } else {
                    body.innerHTML = `<pre class="doc-preview-text p-3 m-0">${escapeHtml(data.content)}</pre>`;
                }
            })
            .catch(err => {
                body.innerHTML = `<div class="p-4 text-center text-danger">
                    <i class="bi bi-exclamation-triangle fs-1 d-block mb-2"></i>
                    Error loading preview: ${escapeHtml(err.message)}
                </div>`;
            });
    },

    _renderMarkdown(text) {
        let html = escapeHtml(text);
        html = html.replace(/^### (.+)$/gm, '<h5>$1</h5>');
        html = html.replace(/^## (.+)$/gm, '<h4>$1</h4>');
        html = html.replace(/^# (.+)$/gm, '<h3>$1</h3>');
        html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
        html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');
        html = html.replace(/`([^`]+)`/g, '<code>$1</code>');
        html = html.replace(/^- (.+)$/gm, '<li>$1</li>');
        html = html.replace(/(<li>.*<\/li>)/s, '<ul>$1</ul>');
        html = html.replace(/\n{2,}/g, '</p><p>');
        html = '<p>' + html + '</p>';
        html = html.replace(/<p>\s*<h([345])>/g, '<h$1>');
        html = html.replace(/<\/h([345])>\s*<\/p>/g, '</h$1>');
        html = html.replace(/<p>\s*<ul>/g, '<ul>');
        html = html.replace(/<\/ul>\s*<\/p>/g, '</ul>');
        return html;
    },
};

// ========== FETCH DEDUPLICATION ==========

const _fetchOnceCache = {};

/**
 * Fetch a URL at most once per page load.  Concurrent callers share the same
 * in-flight promise so the server only sees a single request.
 */
function fetchOnce(url, opts) {
    if (!_fetchOnceCache[url]) {
        _fetchOnceCache[url] = fetch(url, { credentials: 'same-origin', ...opts })
            .then(r => r.json());
    }
    return _fetchOnceCache[url];
}

/**
 * Invalidate the fetchOnce cache for a URL (e.g. after a mutation).
 */
function fetchOnceInvalidate(url) {
    delete _fetchOnceCache[url];
}

// ========== FETCH WITH TTL CACHE ==========

const _fetchCachedInflight = {};

/**
 * Fetch with a TTL-based cache backed by sessionStorage.
 * Survives page navigations within the same browser tab, so repeated
 * loads (e.g. navbar init on every page) are served instantly from
 * cache until the TTL expires.
 *
 * Also deduplicates concurrent in-flight requests to the same URL.
 *
 * @param {string}  url     The URL to fetch (GET, same-origin credentials).
 * @param {number}  ttlMs   Cache lifetime in milliseconds (default 15 000).
 * @returns {Promise<any>}  Parsed JSON response.
 */
function fetchCached(url, ttlMs = 15000) {
    const cacheKey = '__fc__' + url;
    try {
        const raw = sessionStorage.getItem(cacheKey);
        if (raw) {
            const entry = JSON.parse(raw);
            if (Date.now() - entry.ts < ttlMs) {
                return Promise.resolve(entry.data);
            }
            sessionStorage.removeItem(cacheKey);
        }
    } catch (_) { /* sessionStorage unavailable or quota exceeded */ }

    if (_fetchCachedInflight[url]) {
        return _fetchCachedInflight[url];
    }

    _fetchCachedInflight[url] = fetch(url, { credentials: 'same-origin' })
        .then(r => r.json())
        .then(data => {
            try {
                sessionStorage.setItem(cacheKey, JSON.stringify({ ts: Date.now(), data }));
            } catch (_) { /* quota exceeded – stale entry already removed */ }
            return data;
        })
        .finally(() => {
            delete _fetchCachedInflight[url];
        });

    return _fetchCachedInflight[url];
}

/**
 * Invalidate the TTL cache for a URL (e.g. after a mutation).
 * Also clears the per-page-load fetchOnce cache for the same URL.
 */
function fetchCachedInvalidate(url) {
    try { sessionStorage.removeItem('__fc__' + url); } catch (_) {}
    delete _fetchCachedInflight[url];
    fetchOnceInvalidate(url);
}


// Automatically attach CSRF token to state-changing fetch requests
const _origFetch = window.fetch;
window.fetch = function(input, init) {
    init = init || {};
    const method = (init.method || 'GET').toUpperCase();
    if (method !== 'GET' && method !== 'HEAD' && method !== 'OPTIONS') {
        const token = _getCSRFToken();
        if (token) {
            init.headers = init.headers instanceof Headers
                ? init.headers
                : new Headers(init.headers || {});
            if (!init.headers.has('X-CSRF-Token')) {
                init.headers.set('X-CSRF-Token', token);
            }
        }
    }
    return _origFetch.call(this, input, init);
};

// Make all utility functions globally available
window.showNotification = showNotification;
window.NotificationCenter = NotificationCenter;
window.DocumentPreview = DocumentPreview;
window.showConfirmDialog = showConfirmDialog;
window.showDeleteConfirm = showDeleteConfirm;
window.apiRequest = apiRequest;
window.escapeHtml = escapeHtml;
window.fetchOnce = fetchOnce;
window.fetchOnceInvalidate = fetchOnceInvalidate;
window.fetchCached = fetchCached;
window.fetchCachedInvalidate = fetchCachedInvalidate;
