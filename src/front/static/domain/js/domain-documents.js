// =====================================================
// KNOWLEDGE STORE — Upload, parse, view & purge documents
// =====================================================

const MAX_UPLOAD_BYTES = 10 * 1024 * 1024; // Keep in sync with DocumentParseService.MAX_UPLOAD_BYTES

const DocManager = {
    queuedFiles: [],
    parsePollTimer: null,
    selected: new Set(),

    init() {
        const dropZone = document.getElementById('docDropZone');
        const fileInput = document.getElementById('docFileInput');
        const uploadBtn = document.getElementById('docUploadBtn');
        const clearBtn = document.getElementById('docClearQueueBtn');
        const refreshBtn = document.getElementById('docRefreshBtn');
        const purgeBtn = document.getElementById('docPurgeSelectedBtn');

        if (!dropZone) return;

        dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('drag-over'); });
        dropZone.addEventListener('dragleave', () => dropZone.classList.remove('drag-over'));
        dropZone.addEventListener('drop', e => {
            e.preventDefault();
            dropZone.classList.remove('drag-over');
            this.addFiles(e.dataTransfer.files);
        });

        fileInput.addEventListener('change', () => {
            this.addFiles(fileInput.files);
            fileInput.value = '';
        });

        uploadBtn.addEventListener('click', () => this.uploadAll());
        clearBtn.addEventListener('click', () => this.clearQueue());
        refreshBtn.addEventListener('click', () => this.refreshList());
        if (purgeBtn) purgeBtn.addEventListener('click', () => this.purgeSelected());

        this.refreshList();
    },

    addFiles(fileList) {
        if (window.isActiveVersion === false) return;
        for (const f of fileList) {
            if (f.size > MAX_UPLOAD_BYTES) {
                showNotification(`${f.name} exceeds the 10 MB upload limit and was skipped`, 'error');
                continue;
            }
            if (!this.queuedFiles.some(q => q.name === f.name && q.size === f.size)) {
                this.queuedFiles.push(f);
            }
        }
        this.renderQueue();
    },

    removeFromQueue(index) {
        this.queuedFiles.splice(index, 1);
        this.renderQueue();
    },

    clearQueue() {
        this.queuedFiles = [];
        this.renderQueue();
    },

    renderQueue() {
        const container = document.getElementById('docUploadQueue');
        const list = document.getElementById('docQueueList');
        const btn = document.getElementById('docUploadBtn');

        if (this.queuedFiles.length === 0) {
            container.classList.add('d-none');
            return;
        }

        container.classList.remove('d-none');
        btn.disabled = false;

        list.innerHTML = this.queuedFiles.map((f, i) => `
            <div class="d-flex align-items-center justify-content-between py-1 px-2 mb-1 rounded" style="background:#ffffff;">
                <span class="small text-truncate me-2" title="${f.name}">
                    <i class="bi ${fileIcon(f.name)} me-1"></i>${f.name}
                    <span class="text-muted">(${formatSize(f.size)})</span>
                </span>
                <button class="btn btn-sm btn-link text-secondary p-0" onclick="DocManager.removeFromQueue(${i})" title="Remove">
                    <i class="bi bi-x-lg"></i>
                </button>
            </div>
        `).join('');
    },

    async uploadAll() {
        if (window.isActiveVersion === false) return;
        if (this.queuedFiles.length === 0) return;

        const btn = document.getElementById('docUploadBtn');
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Uploading...';

        const formData = new FormData();
        for (const f of this.queuedFiles) {
            formData.append('files', f);
        }

        try {
            const resp = await fetch('/domain/documents/upload', {
                method: 'POST',
                body: formData,
                credentials: 'same-origin',
            });
            const result = await resp.json();

            if (result.success) {
                const hasPending = (result.results || []).some(
                    file => file.parse_status === 'pending'
                );
                showNotification(
                    hasPending ? 'Upload complete; document parsing started' : result.message,
                    'success'
                );
                this.clearQueue();
                this.refreshList();
            } else {
                showNotification('Upload failed: ' + result.message, 'error');
            }
        } catch (err) {
            showNotification('Upload error: ' + err.message, 'error');
        } finally {
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-cloud-upload"></i> Upload All';
        }
    },

    async refreshList() {
        const container = document.getElementById('docFileList');
        container.innerHTML = '<div class="text-muted small"><span class="spinner-border spinner-border-sm me-1"></span> Loading...</div>';

        try {
            const resp = await fetch('/domain/documents/list', { credentials: 'same-origin' });
            const result = await resp.json();

            if (!result.success) {
                container.innerHTML = `<div class="text-muted small fst-italic"><i class="bi bi-info-circle"></i> ${result.message}</div>`;
                return;
            }

            const files = (result.files || []).filter(f => !f.is_directory);
            this.scheduleParseRefresh(files);

            // Drop selections for files that no longer exist.
            const present = new Set(files.map(f => f.name));
            this.selected.forEach(name => { if (!present.has(name)) this.selected.delete(name); });

            if (files.length === 0) {
                container.innerHTML = '<div class="text-muted small fst-italic"><i class="bi bi-folder2-open"></i> No documents uploaded yet.</div>';
                this.updateSelectionUI();
                return;
            }

            const allChecked = files.every(f => this.selected.has(f.name));
            container.innerHTML = `
                <div class="list-group list-group-flush">
                    <label class="list-group-item d-flex align-items-center px-2 py-1 text-muted small">
                        <input type="checkbox" class="form-check-input mt-0 me-2" id="docSelectAll"
                               ${allChecked ? 'checked' : ''}
                               onchange="DocManager.toggleSelectAll(this.checked)">
                        Select all
                    </label>
                    ${files.map(f => {
                        const encodedName = encodeURIComponent(f.name);
                        const safeName = escapeDocHtml(f.name);
                        const isChecked = this.selected.has(f.name) ? 'checked' : '';
                        const retryButton = f.parse_status === 'failed' && f.parser !== 'unsupported'
                            ? `<button class="btn btn-sm btn-outline-secondary py-0 px-2"
                                      onclick="DocManager.retryParse(decodeURIComponent('${encodedName}'))"
                                      title="Retry parsing">
                                   <i class="bi bi-arrow-repeat me-1"></i>Retry
                               </button>`
                            : '';
                        return `
                        <div class="list-group-item d-flex align-items-center justify-content-between px-2 py-2">
                            <input type="checkbox" class="form-check-input mt-0 me-2" ${isChecked}
                                   aria-label="Select ${safeName}"
                                   onchange="DocManager.toggleSelect(decodeURIComponent('${encodedName}'), this.checked)">
                            <span class="small text-truncate me-2 doc-preview-link flex-grow-1" role="button"
                                  tabindex="0"
                                  title="View parsed content of ${safeName}"
                                  onclick="DocumentPreview.open(decodeURIComponent('${encodedName}'))"
                                  onkeydown="if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); DocumentPreview.open(decodeURIComponent('${encodedName}')); }">
                                <i class="bi ${fileIcon(f.name)} me-1"></i>${safeName}
                                ${f.size != null ? `<span class="text-muted">(${formatSize(f.size)})</span>` : ''}
                            </span>
                            <div class="d-flex align-items-center gap-1">
                                ${parseStatusBadge(f)}
                                ${retryButton}
                                <button class="btn btn-sm btn-outline-primary py-0 px-1"
                                        onclick="DocumentPreview.open(decodeURIComponent('${encodedName}'))" title="View parsed content">
                                    <i class="bi bi-eye"></i>
                                </button>
                                <button class="btn btn-sm btn-outline-secondary py-0 px-1"
                                        onclick="DocManager.deleteFile(decodeURIComponent('${encodedName}'))" title="Purge">
                                    <i class="bi bi-trash"></i>
                                </button>
                            </div>
                        </div>
                    `;}).join('')}
                </div>
            `;
            this.updateSelectionUI();
        } catch (err) {
            this.scheduleParseRefresh([]);
            container.innerHTML = `<div class="text-muted small"><i class="bi bi-exclamation-triangle text-warning"></i> ${err.message}</div>`;
        }
    },

    toggleSelect(filename, checked) {
        if (checked) this.selected.add(filename); else this.selected.delete(filename);
        this.updateSelectionUI();
    },

    toggleSelectAll(checked) {
        document.querySelectorAll('#docFileList .list-group-item input[type="checkbox"]').forEach(cb => {
            if (cb.id === 'docSelectAll') return;
            const label = cb.getAttribute('aria-label') || '';
            const name = label.replace(/^Select /, '');
            if (!name) return;
            cb.checked = checked;
            if (checked) this.selected.add(name); else this.selected.delete(name);
        });
        this.updateSelectionUI();
    },

    updateSelectionUI() {
        const btn = document.getElementById('docPurgeSelectedBtn');
        const count = document.getElementById('docSelectedCount');
        if (count) count.textContent = String(this.selected.size);
        if (btn) btn.classList.toggle('d-none', this.selected.size === 0);
    },

    async purgeSelected() {
        if (window.isActiveVersion === false) return;
        const filenames = Array.from(this.selected);
        if (filenames.length === 0) return;

        const label = filenames.length === 1
            ? filenames[0]
            : `${filenames.length} documents`;
        const confirmed = await showDeleteConfirm(label, 'document');
        if (!confirmed) return;

        try {
            const resp = await fetch('/domain/documents/delete', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ filenames }),
                credentials: 'same-origin',
            });
            const result = await resp.json();
            if (result.success) {
                showNotification(result.message || `Purged ${filenames.length} document(s)`, 'success');
                this.selected.clear();
                this.refreshList();
            } else {
                showNotification('Purge failed: ' + result.message, 'error');
            }
        } catch (err) {
            showNotification('Purge error: ' + err.message, 'error');
        }
    },

    scheduleParseRefresh(files) {
        if (this.parsePollTimer) {
            clearTimeout(this.parsePollTimer);
            this.parsePollTimer = null;
        }
        if (files.some(file => file.parse_status === 'pending')) {
            this.parsePollTimer = setTimeout(() => this.refreshList(), 2000);
        }
    },

    async retryParse(filename) {
        if (window.isActiveVersion === false) return;
        try {
            const resp = await fetch('/domain/documents/retry-parse', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ filename }),
                credentials: 'same-origin',
            });
            const result = await resp.json();
            if (!resp.ok || !result.success) {
                throw new Error(result.message || result.detail || 'Retry failed');
            }
            showNotification(result.message || 'Document parsing restarted', 'success');
            this.refreshList();
        } catch (err) {
            showNotification('Retry failed: ' + err.message, 'error');
        }
    },

    async deleteFile(filename) {
        if (window.isActiveVersion === false) return;
        const confirmed = await showDeleteConfirm(filename, 'document');
        if (!confirmed) return;

        try {
            const resp = await fetch('/domain/documents/delete', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ filenames: [filename] }),
                credentials: 'same-origin',
            });
            const result = await resp.json();

            if (result.success) {
                showNotification(`Deleted ${filename}`, 'success');
                this.selected.delete(filename);
                this.refreshList();
            } else {
                showNotification('Delete failed: ' + result.message, 'error');
            }
        } catch (err) {
            showNotification('Delete error: ' + err.message, 'error');
        }
    },
};

function escapeDocHtml(value) {
    return String(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#039;');
}

function parseStatusBadge(file) {
    const states = {
        pending: ['text-bg-warning', 'bi-hourglass-split', 'Parsing'],
        ready: ['text-bg-success', 'bi-check-circle', 'Ready'],
        failed: ['text-bg-danger', 'bi-exclamation-triangle', 'Parse failed'],
    };
    const [badgeClass, icon, label] = states[file.parse_status] || states.failed;
    const detail = file.parse_error ? ` title="${escapeDocHtml(file.parse_error)}"` : '';
    return `<span class="badge ${badgeClass}"${detail}>
        <i class="bi ${icon} me-1"></i>${label}
    </span>`;
}

function fileIcon(name) {
    const ext = (name.split('.').pop() || '').toLowerCase();
    const icons = {
        pdf: 'bi-file-earmark-pdf', doc: 'bi-file-earmark-word', docx: 'bi-file-earmark-word',
        xls: 'bi-file-earmark-excel', xlsx: 'bi-file-earmark-excel', csv: 'bi-file-earmark-spreadsheet',
        png: 'bi-file-earmark-image', jpg: 'bi-file-earmark-image', jpeg: 'bi-file-earmark-image',
        txt: 'bi-file-earmark-text', json: 'bi-file-earmark-code', xml: 'bi-file-earmark-code',
        ttl: 'bi-file-earmark-code', owl: 'bi-file-earmark-code', rdf: 'bi-file-earmark-code',
        zip: 'bi-file-earmark-zip', gz: 'bi-file-earmark-zip',
    };
    return icons[ext] || 'bi-file-earmark';
}

function formatSize(bytes) {
    if (bytes == null) return '';
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

document.addEventListener('DOMContentLoaded', () => DocManager.init());
