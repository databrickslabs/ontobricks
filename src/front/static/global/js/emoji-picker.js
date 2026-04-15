/**
 * OntoBricks — Shared Emoji Picker component.
 *
 * Usage:
 *   var picker = EmojiPicker.create({
 *       triggerEl:   document.getElementById('myBtn'),
 *       previewEl:   document.getElementById('myPreview'),   // optional
 *       inputEl:     document.getElementById('myInput'),     // optional
 *       containerEl: document.getElementById('myMount'),     // optional (defaults to after triggerEl)
 *       categories:  EmojiPicker.DEFAULT_CATEGORIES,         // optional override
 *       showSearch:  true,                                   // default true
 *       onSelect:    function (emoji) { ... }                // optional callback
 *   });
 *
 *   picker.setEmoji('🏢');   // programmatically update preview + input
 *   picker.close();          // hide the dropdown
 *   picker.destroy();        // remove DOM and unbind events
 */
var EmojiPicker = (function () {
    'use strict';

    var DEFAULT_CATEGORIES = {
        'People & Roles':     ['👤','👥','👨','👩','👶','👴','👵','🧑','👨‍💼','👩‍💼','👨‍🔬','👩‍🔬','👨‍💻','👩‍💻','👨‍🏫','👩‍🏫','👨‍⚕️','👩‍⚕️','🧑‍🤝‍🧑','👪'],
        'Business & Work':    ['🏢','🏭','🏬','🏛️','💼','📊','📈','📉','💰','💵','💳','🏦','📋','📁','📂','🗂️','📝','✏️','📌','📎'],
        'Technology':         ['💻','🖥️','⌨️','🖱️','📱','📲','☎️','🔌','💾','💿','📀','🔧','🔩','⚙️','🔬','🔭','📡','🤖','🔋','💡'],
        'Data & Documents':   ['📄','📃','📑','📰','📚','📖','📒','📓','📔','📕','📗','📘','📙','🗃️','🗄️','📦','📫','📬','📭','📮'],
        'Nature & Science':   ['🌍','🌎','🌏','🌐','🌳','🌲','🌴','🌵','🌾','🌻','🔥','💧','⚡','🌈','☀️','🌙','⭐','🌟','💎','🔮'],
        'Objects & Things':   ['🏠','🏡','🚗','🚕','🚌','✈️','🚀','🛸','⚓','🎯','🎨','🎭','🎪','🎬','🎮','🎲','🧩','🔑','🗝️','🔒'],
        'Symbols':            ['❤️','💙','💚','💛','💜','🖤','🤍','🤎','⭕','❌','✅','❎','➕','➖','➗','✖️','💯','🔴','🟠','🟢'],
        'Arrows & Shapes':    ['⬆️','⬇️','⬅️','➡️','↗️','↘️','↙️','↖️','↕️','↔️','🔄','🔃','🔀','🔁','🔂','▶️','⏸️','⏹️','🔷','🔶']
    };

    function _esc(s) {
        if (!s) return '';
        var el = document.createElement('span');
        el.textContent = s;
        return el.innerHTML;
    }

    function allEmojis(categories) {
        var cats = categories || DEFAULT_CATEGORIES;
        var result = [];
        for (var key in cats) {
            result = result.concat(cats[key]);
        }
        return result;
    }

    function create(opts) {
        opts = opts || {};
        var triggerEl   = opts.triggerEl;
        var previewEl   = opts.previewEl   || null;
        var inputEl     = opts.inputEl     || null;
        var containerEl = opts.containerEl || null;
        var categories  = opts.categories  || DEFAULT_CATEGORIES;
        var showSearch  = opts.showSearch !== false;
        var onSelect    = opts.onSelect    || null;

        if (!triggerEl) {
            console.warn('[EmojiPicker] triggerEl is required');
            return null;
        }

        // Build dropdown DOM
        var wrapper = document.createElement('div');
        wrapper.className = 'emoji-picker-container mt-2';
        wrapper.style.display = 'none';

        var card = document.createElement('div');
        card.className = 'card';

        var header = document.createElement('div');
        header.className = 'card-header py-1';
        header.innerHTML =
            '<div class="d-flex justify-content-between align-items-center">' +
            '<small class="fw-bold">Select Icon</small>' +
            '<button type="button" class="btn-close btn-sm emoji-picker-close-btn"></button>' +
            '</div>';
        card.appendChild(header);

        var body = document.createElement('div');
        body.className = 'card-body p-2';

        var searchInput = null;
        if (showSearch) {
            searchInput = document.createElement('input');
            searchInput.type = 'text';
            searchInput.className = 'form-control form-control-sm mb-2';
            searchInput.placeholder = 'Search...';
            body.appendChild(searchInput);
        }

        var grid = document.createElement('div');
        grid.className = 'emoji-picker-grid';
        body.appendChild(grid);
        card.appendChild(body);
        wrapper.appendChild(card);

        // Attach to DOM
        if (containerEl) {
            containerEl.innerHTML = '';
            containerEl.appendChild(wrapper);
        } else {
            triggerEl.parentNode.insertBefore(wrapper, triggerEl.nextSibling);
        }

        // Rendering
        function renderGrid(filter) {
            grid.innerHTML = '';
            var lf = (filter || '').toLowerCase();
            for (var category in categories) {
                if (lf && category.toLowerCase().indexOf(lf) === -1) continue;
                var catDiv = document.createElement('div');
                catDiv.className = 'mb-2';
                catDiv.innerHTML = '<small class="text-muted fw-bold">' + _esc(category) + '</small>';
                var row = document.createElement('div');
                row.className = 'd-flex flex-wrap gap-1 mt-1';
                categories[category].forEach(function (emoji) {
                    var btn = document.createElement('button');
                    btn.type = 'button';
                    btn.className = 'btn btn-light btn-sm emoji-btn';
                    btn.textContent = emoji;
                    btn.addEventListener('click', function () {
                        selectEmoji(emoji);
                    });
                    row.appendChild(btn);
                });
                catDiv.appendChild(row);
                grid.appendChild(catDiv);
            }
        }

        function selectEmoji(emoji) {
            if (previewEl) previewEl.textContent = emoji;
            if (inputEl) inputEl.value = emoji;
            wrapper.style.display = 'none';
            if (onSelect) onSelect(emoji);
        }

        function toggle() {
            var visible = wrapper.style.display !== 'none';
            wrapper.style.display = visible ? 'none' : 'block';
            if (!visible) {
                renderGrid('');
                if (searchInput) searchInput.value = '';
            }
        }

        function close() {
            wrapper.style.display = 'none';
        }

        // Event wiring
        function onTriggerClick(e) {
            e.preventDefault();
            toggle();
        }
        triggerEl.addEventListener('click', onTriggerClick);

        header.querySelector('.emoji-picker-close-btn').addEventListener('click', close);

        if (searchInput) {
            searchInput.addEventListener('input', function (e) {
                renderGrid(e.target.value);
            });
        }

        if (inputEl) {
            inputEl.addEventListener('input', function () {
                if (previewEl) previewEl.textContent = inputEl.value || '';
            });
        }

        // Public instance API
        return {
            close: close,
            toggle: toggle,
            setEmoji: function (emoji) {
                if (previewEl) previewEl.textContent = emoji || '';
                if (inputEl) inputEl.value = emoji || '';
                close();
            },
            destroy: function () {
                triggerEl.removeEventListener('click', onTriggerClick);
                if (wrapper.parentNode) wrapper.parentNode.removeChild(wrapper);
            }
        };
    }

    return {
        DEFAULT_CATEGORIES: DEFAULT_CATEGORIES,
        allEmojis: allEmojis,
        create: create
    };
})();
