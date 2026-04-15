/**
 * OntoBricks - ontology-groups.js
 * Entity group management for the Ontology page.
 */
var OntologyGroups = (function () {
    var _editIndex = -1;
    var _cachedClasses = null;
    var _modalRelocated = false;
    var _picker = null;

    function _ensureModalAtBody() {
        if (_modalRelocated) return;
        var el = document.getElementById('groupModal');
        if (el && el.parentElement !== document.body) {
            document.body.appendChild(el);
            _modalRelocated = true;
        }
    }

    function _fetchClasses() {
        return fetch('/ontology/load')
            .then(function (r) { return r.json(); })
            .then(function (data) {
                if (data.success && data.config) {
                    _cachedClasses = data.config.classes || [];
                } else {
                    _cachedClasses = [];
                }
                return _cachedClasses;
            })
            .catch(function (err) {
                console.error('[Groups] fetch classes error:', err);
                _cachedClasses = [];
                return _cachedClasses;
            });
    }

    function _getClasses() {
        if (typeof OntologyState !== 'undefined' && OntologyState.config && OntologyState.config.classes && OntologyState.config.classes.length > 0) {
            return OntologyState.config.classes;
        }
        if (_cachedClasses && _cachedClasses.length > 0) {
            return _cachedClasses;
        }
        return [];
    }

    function _showLoading(show) {
        var spinner = document.getElementById('groupsLoading');
        var list = document.getElementById('groupsList');
        if (spinner) spinner.style.display = show ? '' : 'none';
        if (list) list.style.display = show ? 'none' : '';
    }

    function _load() {
        _showLoading(true);
        fetch('/ontology/groups/list')
            .then(function (r) { return r.json(); })
            .then(function (data) {
                _showLoading(false);
                if (data.success) _render(data.groups || []);
            })
            .catch(function (err) {
                _showLoading(false);
                console.error('[Groups] load error:', err);
            });
    }

    function _render(groups) {
        var container = document.getElementById('groupsList');
        var empty = document.getElementById('groupsEmptyState');
        if (!container) return;

        if (!groups || groups.length === 0) {
            container.innerHTML = '';
            if (empty) {
                container.appendChild(empty);
                empty.style.display = '';
            }
            return;
        }
        if (empty) empty.style.display = 'none';

        var html = '';
        groups.forEach(function (g, idx) {
            var color = g.color || '#4A90D9';
            var members = g.members || [];
            var memberBadges = members.map(function (m) {
                return '<span class="group-member-badge">' + _esc(m) + '</span>';
            }).join('');
            if (!memberBadges) memberBadges = '<span class="text-muted small">No members</span>';

            html += '<div class="col-md-6 col-lg-4">' +
                '<div class="card group-card h-100" style="--group-color:' + _esc(color) + '">' +
                '<div class="card-body">' +
                '<div class="d-flex justify-content-between align-items-start mb-2">' +
                '<h6 class="mb-0">' +
                '<span class="group-color-dot" style="background-color:' + _esc(color) + '"></span>' +
                (g.icon ? _esc(g.icon) + ' ' : '') + _esc(g.label || g.name) +
                '</h6>' +
                '<div class="btn-group btn-group-sm">' +
                '<button class="btn btn-outline-secondary btn-sm py-0" onclick="OntologyGroups.editGroup(' + idx + ')" title="Edit">' +
                '<i class="bi bi-pencil"></i></button>' +
                '<button class="btn btn-outline-danger btn-sm py-0" onclick="OntologyGroups.deleteGroup(' + idx + ')" title="Delete">' +
                '<i class="bi bi-trash"></i></button>' +
                '</div></div>' +
                (g.description ? '<p class="text-muted small mb-2">' + _esc(g.description) + '</p>' : '') +
                '<div class="small"><strong>' + members.length + '</strong> member' + (members.length !== 1 ? 's' : '') + '</div>' +
                '<div class="mt-1">' + memberBadges + '</div>' +
                '</div></div></div>';
        });
        container.innerHTML = html;
    }

    function _esc(s) {
        if (!s) return '';
        var el = document.createElement('span');
        el.textContent = s;
        return el.innerHTML;
    }

    function _renderCheckboxes(classes, selected, otherGroupMap) {
        var container = document.getElementById('groupMembersList');
        if (!container) return;
        var selSet = new Set(selected || []);

        if (!classes || !classes.length) {
            container.innerHTML = '<p class="text-muted small m-2">No classes defined in the ontology.</p>';
            return;
        }

        var html = '';
        classes.forEach(function (cls) {
            var name = cls.name || '';
            var label = cls.label || name;
            var emoji = cls.emoji || '';
            var checked = selSet.has(name) ? 'checked' : '';
            var otherGroup = otherGroupMap && otherGroupMap[name];
            var badge = otherGroup
                ? ' <span class="badge bg-warning text-dark ms-1" title="Currently in group ' + _esc(otherGroup) + '">' + _esc(otherGroup) + '</span>'
                : '';
            html += '<div class="form-check">' +
                '<input class="form-check-input group-member-cb" type="checkbox" value="' +
                _esc(name) + '" id="gm_' + _esc(name) + '" ' + checked + '>' +
                '<label class="form-check-label" for="gm_' + _esc(name) + '">' +
                (emoji ? emoji + ' ' : '') + _esc(label) + ' <code class="small text-muted">' + _esc(name) + '</code>' +
                badge +
                '</label></div>';
        });
        container.innerHTML = html;
    }

    function _populateMembersCheckboxes(selected, editingGroupName) {
        var classes = _getClasses();
        var container = document.getElementById('groupMembersList');

        function buildOtherGroupMap(callback) {
            fetch('/ontology/groups/list')
                .then(function (r) { return r.json(); })
                .then(function (data) {
                    var map = {};
                    if (data.success && data.groups) {
                        data.groups.forEach(function (g) {
                            if (g.name === editingGroupName) return;
                            (g.members || []).forEach(function (m) { map[m] = g.label || g.name; });
                        });
                    }
                    callback(map);
                })
                .catch(function () { callback({}); });
        }

        if (classes.length > 0) {
            buildOtherGroupMap(function (map) { _renderCheckboxes(classes, selected, map); });
        } else {
            if (container) container.innerHTML = '<p class="text-muted small m-2"><i class="bi bi-hourglass-split"></i> Loading classes...</p>';
            _fetchClasses().then(function (fetched) {
                buildOtherGroupMap(function (map) { _renderCheckboxes(fetched, selected, map); });
            });
        }
    }

    function _getSelectedMembers() {
        var cbs = document.querySelectorAll('.group-member-cb:checked');
        var members = [];
        cbs.forEach(function (cb) { members.push(cb.value); });
        return members;
    }

    function _ensureIconPicker() {
        if (_picker) return;
        var triggerEl = document.getElementById('groupIconPickerBtn');
        var previewEl = document.getElementById('groupIconPreview');
        var inputEl   = document.getElementById('groupIcon');
        var mountEl   = document.getElementById('groupIconPickerMount');
        if (!triggerEl) return;
        _picker = EmojiPicker.create({
            triggerEl:   triggerEl,
            previewEl:   previewEl,
            inputEl:     inputEl,
            containerEl: mountEl || undefined
        });
    }

    return {
        init: function () { _load(); },

        showCreateModal: function () {
            _editIndex = -1;
            _ensureModalAtBody();
            var el = document.getElementById('groupModal');
            if (!el) { console.error('[Groups] groupModal element not found'); return; }
            document.getElementById('groupModalTitle').innerHTML = '<i class="bi bi-collection me-2"></i>New Group';
            document.getElementById('groupName').value = '';
            document.getElementById('groupName').disabled = false;
            document.getElementById('groupLabel').value = '';
            document.getElementById('groupDescription').value = '';
            document.getElementById('groupColor').value = '#4A90D9';
            _ensureIconPicker();
            if (_picker) _picker.setEmoji('');
            _populateMembersCheckboxes([], '');
            var modal = bootstrap.Modal.getOrCreateInstance(el);
            modal.show();
        },

        editGroup: function (idx) {
            fetch('/ontology/groups/list')
                .then(function (r) { return r.json(); })
                .then(function (data) {
                    if (!data.success) return;
                    var groups = data.groups || [];
                    if (idx < 0 || idx >= groups.length) return;
                    var g = groups[idx];
                    _editIndex = idx;
                    _ensureModalAtBody();
                    var el = document.getElementById('groupModal');
                    if (!el) { console.error('[Groups] groupModal element not found'); return; }
                    document.getElementById('groupModalTitle').innerHTML = '<i class="bi bi-collection me-2"></i>Edit Group';
                    document.getElementById('groupName').value = g.name || '';
                    document.getElementById('groupName').disabled = true;
                    document.getElementById('groupLabel').value = g.label || '';
                    document.getElementById('groupDescription').value = g.description || '';
                    document.getElementById('groupColor').value = g.color || '#4A90D9';
                    _ensureIconPicker();
                    if (_picker) _picker.setEmoji(g.icon);
                    _populateMembersCheckboxes(g.members || [], g.name);
                    var modal = bootstrap.Modal.getOrCreateInstance(el);
                    modal.show();
                });
        },

        saveGroup: function () {
            var name = document.getElementById('groupName').value.trim();
            if (!name) {
                showNotification('Group name is required.', 'warning');
                return;
            }

            var group = {
                name: name,
                label: document.getElementById('groupLabel').value.trim() || name,
                description: document.getElementById('groupDescription').value.trim(),
                color: document.getElementById('groupColor').value,
                icon: document.getElementById('groupIcon').value.trim(),
                members: _getSelectedMembers()
            };

            fetch('/ontology/groups/save', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ group: group, index: _editIndex })
            })
                .then(function (r) { return r.json(); })
                .then(function (data) {
                    if (data.success) {
                        bootstrap.Modal.getInstance(document.getElementById('groupModal'))?.hide();
                        _render(data.groups || []);
                        showNotification('Group "' + name + '" saved successfully.', 'success', 3000);
                    } else {
                        showNotification(data.message || 'Save failed.', 'error');
                    }
                })
                .catch(function (err) {
                    console.error('[Groups] save error:', err);
                    showNotification('Error saving group: ' + err.message, 'error');
                });
        },

        deleteGroup: function (idx) {
            showConfirmDialog({
                title: 'Delete Group',
                message: 'Delete this group? Member classes will not be removed from the ontology.',
                confirmText: 'Delete',
                confirmClass: 'btn-danger'
            }).then(function (confirmed) {
                if (!confirmed) return;
                fetch('/ontology/groups/delete', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ index: idx })
                })
                    .then(function (r) { return r.json(); })
                    .then(function (data) {
                        if (data.success) {
                            _render(data.groups || []);
                            showNotification('Group deleted.', 'success', 3000);
                        }
                    })
                    .catch(function (err) {
                        console.error('[Groups] delete error:', err);
                        showNotification('Error deleting group: ' + err.message, 'error');
                    });
            });
        },

        refresh: function () { _load(); }
    };
})();
