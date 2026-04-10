// =====================================================
// PROJECT DOCUMENTS — Upload & list files in UC volume
// =====================================================

const DocManager = {
    queuedFiles: [],

    init() {
        const dropZone = document.getElementById('docDropZone');
        const fileInput = document.getElementById('docFileInput');
        const uploadBtn = document.getElementById('docUploadBtn');
        const clearBtn = document.getElementById('docClearQueueBtn');
        const refreshBtn = document.getElementById('docRefreshBtn');

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

        this.loadVolumeLocation();
        this.refreshList();
    },

    async loadVolumeLocation() {
        const el = document.getElementById('docVolumePath');
        if (!el) return;
        try {
            const data = await fetchOnce('/project/version-status');
            if (data.success && data.registry && data.registry.catalog && data.project_folder) {
                const r = data.registry;
                el.innerHTML = `<strong>${r.catalog}.${r.schema}.${r.volume}</strong>/projects/<strong>${data.project_folder}</strong>/documents`;
            } else {
                el.textContent = 'Project not saved to the registry yet';
            }
        } catch {
            el.textContent = 'Unable to load location';
        }
    },

    addFiles(fileList) {
        for (const f of fileList) {
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
            <div class="d-flex align-items-center justify-content-between py-1 px-2 mb-1 rounded" style="background:#f8f9fa;">
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
        if (this.queuedFiles.length === 0) return;

        const btn = document.getElementById('docUploadBtn');
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Uploading...';

        const formData = new FormData();
        for (const f of this.queuedFiles) {
            formData.append('files', f);
        }

        try {
            const resp = await fetch('/project/documents/upload', {
                method: 'POST',
                body: formData,
                credentials: 'same-origin',
            });
            const result = await resp.json();

            if (result.success) {
                showNotification(result.message, 'success');
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
            const resp = await fetch('/project/documents/list', { credentials: 'same-origin' });
            const result = await resp.json();

            if (!result.success) {
                container.innerHTML = `<div class="text-muted small fst-italic"><i class="bi bi-info-circle"></i> ${result.message}</div>`;
                return;
            }

            const files = (result.files || []).filter(f => !f.is_directory);

            if (files.length === 0) {
                container.innerHTML = '<div class="text-muted small fst-italic"><i class="bi bi-folder2-open"></i> No documents uploaded yet.</div>';
                return;
            }

            container.innerHTML = `
                <div class="list-group list-group-flush">
                    ${files.map(f => `
                        <div class="list-group-item d-flex align-items-center justify-content-between px-2 py-2">
                            <span class="small text-truncate me-2 doc-preview-link" role="button"
                                  title="Click to preview ${f.name}"
                                  onclick="DocumentPreview.open('${f.name.replace(/'/g, "\\'")}')">
                                <i class="bi ${fileIcon(f.name)} me-1"></i>${f.name}
                                ${f.size != null ? `<span class="text-muted">(${formatSize(f.size)})</span>` : ''}
                            </span>
                            <div class="d-flex gap-1">
                                <button class="btn btn-sm btn-outline-primary py-0 px-1"
                                        onclick="DocumentPreview.open('${f.name.replace(/'/g, "\\'")}')" title="Preview">
                                    <i class="bi bi-eye"></i>
                                </button>
                                <button class="btn btn-sm btn-outline-secondary py-0 px-1"
                                        onclick="DocManager.deleteFile('${f.name}')" title="Delete">
                                    <i class="bi bi-trash"></i>
                                </button>
                            </div>
                        </div>
                    `).join('')}
                </div>
            `;
        } catch (err) {
            container.innerHTML = `<div class="text-muted small"><i class="bi bi-exclamation-triangle text-warning"></i> ${err.message}</div>`;
        }
    },

    async deleteFile(filename) {
        if (!confirm(`Delete "${filename}"?`)) return;

        try {
            const resp = await fetch('/project/documents/delete', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ filename }),
                credentials: 'same-origin',
            });
            const result = await resp.json();

            if (result.success) {
                showNotification(`Deleted ${filename}`, 'success');
                this.refreshList();
            } else {
                showNotification('Delete failed: ' + result.message, 'error');
            }
        } catch (err) {
            showNotification('Delete error: ' + err.message, 'error');
        }
    },
};

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
