/**
 * Minimal DOM for running global/js/i18n.js under plain Node (no jsdom).
 *
 * Implements only what the translator touches: element / text nodes,
 * attributes, matches / closest / querySelector for simple selectors
 * (tag, .class, [attr], [attr="value"] and compounds of those, comma lists),
 * and document.createTreeWalker with FILTER_ACCEPT / SKIP / REJECT.
 *
 * Usage: const { h, document } = require(<this file>); build a tree with
 * h('div', { 'data-i18n-skip': '' }, 'Save', h('span', {}, 'Edit')).
 */
'use strict';

const ELEMENT_NODE = 1;
const TEXT_NODE = 3;
const DOCUMENT_NODE = 9;
const DOCUMENT_FRAGMENT_NODE = 11;

const NodeFilter = {
    SHOW_ELEMENT: 0x1,
    SHOW_TEXT: 0x4,
    FILTER_ACCEPT: 1,
    FILTER_REJECT: 2,
    FILTER_SKIP: 3,
};

const SIMPLE = /^([a-zA-Z][a-zA-Z0-9-]*)?((?:\.[\w-]+|\[[\w-]+(?:="[^"]*")?\])*)$/;
const PART = /\.([\w-]+)|\[([\w-]+)(?:="([^"]*)")?\]/g;

function parseCompound(sel) {
    const m = sel.trim().match(SIMPLE);
    if (!m) throw new Error('unsupported selector: ' + sel);
    const parts = [];
    let p;
    PART.lastIndex = 0;
    while ((p = PART.exec(m[2] || ''))) {
        parts.push(p[1] ? { cls: p[1] } : { attr: p[2], value: p[3] });
    }
    return { tag: m[1] ? m[1].toUpperCase() : null, parts };
}

function matchesCompound(el, c) {
    if (c.tag && el.tagName !== c.tag) return false;
    return c.parts.every((p) => {
        if (p.cls) return el.classList.contains(p.cls);
        if (!el.hasAttribute(p.attr)) return false;
        return p.value === undefined || el.getAttribute(p.attr) === p.value;
    });
}

class Text {
    constructor(value) {
        this.nodeType = TEXT_NODE;
        this.nodeValue = value;
        this.parentNode = null;
    }
    get parentElement() {
        return this.parentNode;
    }
}

class Element {
    constructor(tag, attrs) {
        this.nodeType = ELEMENT_NODE;
        this.tagName = tag.toUpperCase();
        this.attrs = Object.assign({}, attrs || {});
        this.childNodes = [];
        this.parentNode = null;
        const self = this;
        this.classList = {
            contains: (c) => (self.attrs['class'] || '').split(/\s+/).indexOf(c) >= 0,
        };
    }
    get parentElement() {
        return this.parentNode;
    }
    get type() {
        return this.attrs.type;
    }
    get value() {
        return this.attrs.value;
    }
    set value(v) {
        this.attrs.value = v;
    }
    get textContent() {
        return this.childNodes.map((n) => (n.nodeType === TEXT_NODE ? n.nodeValue : n.textContent)).join('');
    }
    append(...children) {
        for (const c of children) {
            const node = typeof c === 'string' ? new Text(c) : c;
            node.parentNode = this;
            this.childNodes.push(node);
        }
        return this;
    }
    getAttribute(name) {
        return Object.prototype.hasOwnProperty.call(this.attrs, name) ? this.attrs[name] : null;
    }
    setAttribute(name, value) {
        this.attrs[name] = String(value);
    }
    hasAttribute(name) {
        return Object.prototype.hasOwnProperty.call(this.attrs, name);
    }
    matches(selectorList) {
        return selectorList.split(',').some((s) => matchesCompound(this, parseCompound(s)));
    }
    closest(selectorList) {
        for (let el = this; el; el = el.parentNode) {
            if (el.nodeType === ELEMENT_NODE && el.matches(selectorList)) return el;
        }
        return null;
    }
    querySelector(selectorList) {
        for (const c of this.childNodes) {
            if (c.nodeType !== ELEMENT_NODE) continue;
            if (c.matches(selectorList)) return c;
            const deep = c.querySelector(selectorList);
            if (deep) return deep;
        }
        return null;
    }
    getElementById(id) {
        if (this.attrs.id === id) return this;
        for (const c of this.childNodes) {
            if (c.nodeType !== ELEMENT_NODE) continue;
            const hit = c.getElementById(id);
            if (hit) return hit;
        }
        return null;
    }
}

function h(tag, attrs, ...children) {
    return new Element(tag, attrs).append(...children);
}

/** Pre-order walker honouring whatToShow and FILTER_SKIP / FILTER_REJECT. */
function createTreeWalker(root, whatToShow, filter) {
    const out = [];
    function shown(n) {
        return (n.nodeType === ELEMENT_NODE && whatToShow & NodeFilter.SHOW_ELEMENT) ||
            (n.nodeType === TEXT_NODE && whatToShow & NodeFilter.SHOW_TEXT);
    }
    function visit(node) {
        for (const c of node.childNodes) {
            if (!shown(c)) continue;
            const verdict = filter.acceptNode(c);
            if (verdict === NodeFilter.FILTER_REJECT) continue;
            if (verdict === NodeFilter.FILTER_ACCEPT) out.push(c);
            if (c.nodeType === ELEMENT_NODE) visit(c);
        }
    }
    visit(root);
    let i = 0;
    return { nextNode: () => (i < out.length ? out[i++] : null) };
}

const document = {
    documentElement: h('html', {}),
    addEventListener() {},
    createTreeWalker,
};

module.exports = {
    h,
    document,
    Node: { ELEMENT_NODE, TEXT_NODE, DOCUMENT_NODE, DOCUMENT_FRAGMENT_NODE },
    NodeFilter,
};
