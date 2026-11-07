/**
 * OntoBricks - ontology-groups.js
 * Entity group create/edit modal + Studio toolbar menu.
 * Per-entity membership is edited in the right pane General tab.
 */
var OntologyGroups = (function () {
    var _editIndex = -1;
    var _cachedClasses = null;
    var _cachedGroups = [];
    var _modalRelocated = false;
    var _picker = null;
    var _toolbarWired = false;

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

    function _esc(s) {
        if (!s) return '';
        var el = document.createElement('span');
        el.textContent = s;
        return el.innerHTML;
    }

    function _afterGroupsChanged(groups) {
        _cachedGroups = groups || [];
        _populateStudioMenu(_cachedGroups);
        if (typeof window.refreshStudioGroups === 'function') {
            window.refreshStudioGroups();
        }
        if (typeof window.refreshSharedEntityGroupOptions === 'function') {
            window.refreshSharedEntityGroupOptions(_cachedGroups);
        }
    }

    function _loadGroups() {
        return fetch('/ontology/groups/list')
            .then(function (r) { return r.json(); })
            .then(function (data) {
                if (data.success) {
                    _cachedGroups = data.groups || [];
                    _populateStudioMenu(_cachedGroups);
                }
                return _cachedGroups;
            })
            .catch(function (err) {
                console.error('[Groups] load error:', err);
                return [];
            });
    }

    function _populateStudioMenu(groups) {
        var menu = document.getElementById('studioGroupsMenu');
        if (!menu) return;
        var newBtn = document.getElementById('studioNewGroupBtn');
        var divider = document.getElementById('studioGroupsDivider');
        menu.querySelectorAll('[data-group-idx]').forEach(function (el) {
            el.closest('li')?.remove();
        });
        var empty = document.getElementById('studioGroupsEmpty');
        if (empty) empty.closest('li')?.remove();

        if (!groups || groups.length === 0) {
            var emptyLi = document.createElement('li');
            emptyLi.innerHTML = '<span class="dropdown-item-text text-muted small" id="studioGroupsEmpty">No groups yet</span>';
            menu.appendChild(emptyLi);
            if (divider) divider.classList.add('d-none');
            return;
        }
        if (divider) divider.classList.remove('d-none');
        groups.forEach(function (g, idx) {
            var li = document.createElement('li');
            var color = g.color || '#4A90D9';
            var icon = g.icon || '📁';
            var label = g.label || g.name;
            li.innerHTML =
                '<button type="button" class="dropdown-item studio-group-menu-item d-flex align-items-center flex-nowrap" data-group-idx="' + idx + '">' +
                '<span class="group-color-dot flex-shrink-0" style="background-color:' + _esc(color) + '"></span>' +
                '<span class="studio-group-icon flex-shrink-0 me-1">' + _esc(icon) + '</span>' +
                '<span class="studio-group-name flex-grow-1 text-truncate me-2">' + _esc(label) + '</span>' +
                '<i class="bi bi-pencil text-muted flex-shrink-0 ms-auto"></i>' +
                '</button>';
            li.querySelector('button').addEventListener('click', function (e) {
                e.preventDefault();
                OntologyGroups.editGroup(idx);
            });
            menu.appendChild(li);
        });
        if (newBtn && newBtn.parentElement !== menu.firstElementChild) {
            // keep New group as first item — already in markup
        }
    }

    function _wireStudioToolbar() {
        if (_toolbarWired) return;
        var newBtn = document.getElementById('studioNewGroupBtn');
        if (!newBtn) return;
        _toolbarWired = true;
        newBtn.addEventListener('click', function (e) {
            e.preventDefault();
            OntologyGroups.showCreateModal();
        });
        var groupsBtn = document.getElementById('studioGroupsBtn');
        if (groupsBtn) {
            groupsBtn.addEventListener('show.bs.dropdown', function () {
                _loadGroups();
            });
        }
        var delBtn = document.getElementById('groupModalDeleteBtn');
        if (delBtn) {
            delBtn.addEventListener('click', function () {
                if (_editIndex < 0) return;
                OntologyGroups.deleteGroup(_editIndex);
            });
        }
    }

    function _setDeleteVisible(show) {
        var btn = document.getElementById('groupModalDeleteBtn');
        if (!btn) return;
        btn.classList.toggle('d-none', !show);
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
                ? ' <span class="badge bg-warning text-dark ms-1" title="Currently in group ' + _esc(otherGroup) + ' — selecting moves it here">' + _esc(otherGroup) + '</span>'
                : '';
            html += '<div class="form-check">' +
                '<input class="form-check-input group-member-cb" type="checkbox" value="' +
                _esc(name) + '" id="gm_' + _esc(name) + '" ' + checked +
                (otherGroup ? ' data-other-group="' + _esc(otherGroup) + '"' : '') + '>' +
                '<label class="form-check-label" for="gm_' + _esc(name) + '">' +
                (emoji ? emoji + ' ' : '') + _esc(label) + ' <code class="small text-muted">' + _esc(name) + '</code>' +
                badge +
                '</label></div>';
        });
        container.innerHTML = html;
        _wireExclusiveMemberChecks();
    }

    /**
     * Confirm before claiming a member that already belongs to another group.
     * Backend also enforces exclusive membership on save.
     */
    function _wireExclusiveMemberChecks() {
        var cbs = document.querySelectorAll('.group-member-cb[data-other-group]');
        cbs.forEach(function (cb) {
            cb.addEventListener('change', function () {
                if (!cb.checked) return;
                var other = cb.getAttribute('data-other-group') || '';
                if (!other) return;
                var entity = cb.value;
                var msg = '"' + entity + '" is already in group "' + other +
                    '". An entity can belong to only one group. Move it here?';
                var proceed = window.confirm(msg);
                if (!proceed) {
                    cb.checked = false;
                }
            });
        });
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
        if (!triggerEl || typeof EmojiPicker === 'undefined') return;
        _picker = EmojiPicker.create({
            triggerEl:   triggerEl,
            previewEl:   previewEl,
            inputEl:     inputEl,
            containerEl: mountEl || undefined
        });
    }

    return {
        init: function () {
            _wireStudioToolbar();
            _loadGroups();
        },

        showCreateModal: function () {
            _editIndex = -1;
            _ensureModalAtBody();
            _wireStudioToolbar();
            var el = document.getElementById('groupModal');
            if (!el) { console.error('[Groups] groupModal element not found'); return; }
            document.getElementById('groupModalTitle').innerHTML = '<i class="bi bi-collection me-2"></i>New Group';
            document.getElementById('groupName').value = '';
            document.getElementById('groupName').disabled = false;
            document.getElementById('groupLabel').value = '';
            document.getElementById('groupDescription').value = '';
            document.getElementById('groupColor').value = '#4A90D9';
            _setDeleteVisible(false);
            _ensureIconPicker();
            if (_picker) _picker.setEmoji('');
            _populateMembersCheckboxes([], '');
            bootstrap.Modal.getOrCreateInstance(el).show();
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
                    _wireStudioToolbar();
                    var el = document.getElementById('groupModal');
                    if (!el) { console.error('[Groups] groupModal element not found'); return; }
                    document.getElementById('groupModalTitle').innerHTML = '<i class="bi bi-collection me-2"></i>Edit Group';
                    document.getElementById('groupName').value = g.name || '';
                    document.getElementById('groupName').disabled = true;
                    document.getElementById('groupLabel').value = g.label || '';
                    document.getElementById('groupDescription').value = g.description || '';
                    document.getElementById('groupColor').value = g.color || '#4A90D9';
                    _setDeleteVisible(true);
                    _ensureIconPicker();
                    if (_picker) _picker.setEmoji(g.icon);
                    _populateMembersCheckboxes(g.members || [], g.name);
                    bootstrap.Modal.getOrCreateInstance(el).show();
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
                        _afterGroupsChanged(data.groups || []);
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
                message: 'Delete this group? Member entities will not be removed from the ontology.',
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
                            bootstrap.Modal.getInstance(document.getElementById('groupModal'))?.hide();
                            _afterGroupsChanged(data.groups || []);
                            showNotification('Group deleted.', 'success', 3000);
                        }
                    })
                    .catch(function (err) {
                        console.error('[Groups] delete error:', err);
                        showNotification('Error deleting group: ' + err.message, 'error');
                    });
            });
        },

        refresh: function () { return _loadGroups(); }
    };
})();

document.addEventListener('DOMContentLoaded', function () {
    if (typeof OntologyGroups !== 'undefined') OntologyGroups.init();
});
