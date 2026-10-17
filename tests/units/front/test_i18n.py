"""Contracts for the UI internationalisation layer (English / Korean).

The UI is authored in English. ``global/js/i18n.js`` translates server
markup and script-generated text on the client using the flat dictionary in
``global/js/i18n-ko.js``; the server decides ``<html lang>`` from the
``ob_lang`` cookie (falling back to ``ONTOBRICKS_UI_LANG``) and only ships the
dictionary to non-English pages.
"""

import json
import re
import shutil
import subprocess
from html.parser import HTMLParser
from pathlib import Path

import pytest

from front.fastapi.dependencies import (
    SUPPORTED_UI_LANGS,
    UI_LANG_COOKIE,
    default_ui_lang,
    resolve_ui_lang,
)

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
FRONT = REPO_ROOT / "src/front"
TEMPLATES = FRONT / "templates"
STATIC = FRONT / "static"
STATIC_JS = STATIC / "global/js"
I18N_JS = STATIC_JS / "i18n.js"
KO_JS = STATIC_JS / "i18n-ko.js"
I18N_CSS = STATIC / "global/css/i18n.css"
BASE_HTML = TEMPLATES / "base.html"
MENU_CONFIG = FRONT / "config/menu_config.json"
MINI_DOM = REPO_ROOT / "tests/fixtures/i18n_mini_dom.js"
UNTRANSLATED_BASELINE = Path(__file__).with_name("i18n_untranslated_baseline.json")

needs_node = pytest.mark.skipif(
    shutil.which("node") is None, reason="node is required to run the i18n module"
)


def _ko_dict() -> dict:
    src = KO_JS.read_text(encoding="utf-8")
    body = src[src.index("= {") + 2 : src.rindex("}") + 1]
    return json.loads(body)


def _run_node(script: str) -> object:
    completed = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=30)
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


_CORE_HARNESS = """
const fs = require('fs');
global.window = {};
global.document = { addEventListener() {} };
eval(fs.readFileSync(__SOURCE__, 'utf8'));
const core = window.OB_I18N_CORE;
const dict = __DICT__;
const out = __INPUTS__.map(s => {
    const hit = core.translateWith(dict, s);
    return hit === null ? null : core.keepPadding(s, hit);
});
process.stdout.write(JSON.stringify(out));
"""


def _translate(dict_: dict, inputs: list) -> list:
    return _run_node(
        _CORE_HARNESS.replace("__SOURCE__", json.dumps(str(I18N_JS)))
        .replace("__DICT__", json.dumps(dict_, ensure_ascii=False))
        .replace("__INPUTS__", json.dumps(inputs, ensure_ascii=False))
    )


# Runs the real translator over a tree built with the mini DOM. __TREE__ is a
# JS expression using h() and __TARGET__ the node handed to translateTree; the
# harness returns textContent / title per id.
_DOM_HARNESS = """
const fs = require('fs');
const dom = require(__MINI_DOM__);
global.window = {};
global.document = dom.document;
global.Node = dom.Node;
global.NodeFilter = dom.NodeFilter;
const h = dom.h;
eval(fs.readFileSync(__SOURCE__, 'utf8'));
const translator = window.OB_I18N_CORE.createTranslator(__DICT__);
const root = __TREE__;
translator.translateTree(__TARGET__);
const ids = __IDS__;
const out = {};
for (const id of ids) {
    const el = root.getElementById(id);
    out[id] = { text: el.textContent, title: el.getAttribute('title') };
}
process.stdout.write(JSON.stringify(out));
"""

DOM_DICT = {"Save": "저장", "Edit": "편집", "Loading": "불러오는 중", "Mapping": "매핑"}


def _translate_tree(tree_js: str, ids: list, target: str = "root") -> dict:
    script = (
        _DOM_HARNESS.replace("__MINI_DOM__", json.dumps(str(MINI_DOM)))
        .replace("__SOURCE__", json.dumps(str(I18N_JS)))
        .replace("__DICT__", json.dumps(DOM_DICT, ensure_ascii=False))
        .replace("__TREE__", tree_js)
        .replace("__IDS__", json.dumps(ids))
        .replace("__TARGET__", target)
    )
    return _run_node(script)


# Product / standard names shown as-is in every language.
KEEP_IN_ENGLISH = {
    "API",
    "Databricks",
    "Lakebase",
    "Lakehouse",
    "Neo4j",
    "OWL",
    "OntoBricks",
    "R2RML",
    "Spark SQL",
    "UI",
}

SAMPLE = {
    "Save": "저장",
    "{n} entities": "엔티티 {n}개",
    "Error deleting relationship:": "관계 삭제 오류:",
    "Loading": "불러오는 중",
}

# Containers that render user-authored content. Each must carry
# data-i18n-skip so ontology / data names are never matched against the
# dictionary.
USER_CONTENT_ROOTS = (
    # Ontology
    "classHierarchyTree",
    "propertiesList",
    "groupsList",
    "groupMembersList",
    "axiomsList",
    "expressionsList",
    "swrlRulesList",
    "sparqlList",
    "dtList",
    "cohortRulesList",
    "cohortLinksList",
    "cohortCompatList",
    "cohortSampleValuesList",
    "metaAttrTableList",
    "metaAttrColumnList",
    "wizardMetadataTableBody",
    "assistantMessages",
    "ontology-map-container",
    # Mapping
    "entityDetailList",
    "relationshipDetailList",
    "manualAssignmentTree",
    "diagEntitiesBody",
    "diagRelationshipsBody",
    "mapping-map-container",
    # Knowledge graph explorer and query results
    "sgContainer",
    "sgDetailsContent",
    "sgClusterList",
    "sgSeedTableBody",
    "chatMessages",
    "cohortPreviewBody",
    "inferredTriplesBody",
    "pagerankDetailBody",
    "dqExecViolationsBody",
    # Registry
    "registryDomainsList",
    "bridgesContent",
    "exportObxTableBody",
    "importObxTableBody",
    # Domain
    "versionsTableBody",
    "domainDiscussionsContainer",
    "docFileList",
    "docQueueList",
    "metadataTablesBody",
    "tableSelectionBody",
    "dsModalTableName",
    "unmappedEntitiesList",
    "unmappedRelationshipsList",
    "domainMyTasksContainer",
    "homeTasksContainer",
    # Settings and shared dialogs
    "neo4jConnectionList",
    "lockReleaseName",
    "ucBrowserFilesList",
    "ucFileDialogFilesList",
)


# ---------------------------------------------------------------- server side


def test_supported_languages_are_english_and_korean():
    assert SUPPORTED_UI_LANGS == ("en", "ko")


@pytest.mark.parametrize(
    "cookie,expected", [("ko", "ko"), ("en", "en"), (None, "en"), ("fr", "en"), ("", "en")]
)
def test_resolve_ui_lang_without_deploy_default(monkeypatch, cookie, expected):
    monkeypatch.delenv("ONTOBRICKS_UI_LANG", raising=False)
    assert resolve_ui_lang(cookie) == expected


@pytest.mark.parametrize("cookie,expected", [(None, "ko"), ("", "ko"), ("fr", "ko"), ("en", "en")])
def test_resolve_ui_lang_falls_back_to_deploy_default(monkeypatch, cookie, expected):
    monkeypatch.setenv("ONTOBRICKS_UI_LANG", "ko")
    assert resolve_ui_lang(cookie) == expected


def test_default_ui_lang_is_read_per_call(monkeypatch):
    monkeypatch.setenv("ONTOBRICKS_UI_LANG", "ko")
    assert default_ui_lang() == "ko"
    monkeypatch.setenv("ONTOBRICKS_UI_LANG", "en")
    assert default_ui_lang() == "en"


def test_unsupported_deploy_default_falls_back_to_english(monkeypatch):
    monkeypatch.setenv("ONTOBRICKS_UI_LANG", "fr")
    assert default_ui_lang() == "en"


# ---------------------------------------------------------------- rendered page


def _home_html(client, monkeypatch, cookie=None, env=None) -> str:
    if env is None:
        monkeypatch.delenv("ONTOBRICKS_UI_LANG", raising=False)
    else:
        monkeypatch.setenv("ONTOBRICKS_UI_LANG", env)
    client.cookies.clear()
    if cookie is not None:
        client.cookies.set(UI_LANG_COOKIE, cookie)
    return client.get("/", headers={"accept": "text/html"}).text


def test_english_page_does_not_load_the_korean_dictionary(client, monkeypatch):
    html = _home_html(client, monkeypatch, cookie="en")
    assert '<html lang="en"' in html
    assert "global/js/i18n-ko.js" not in html
    assert "global/js/i18n.js" in html


def test_korean_page_loads_the_dictionary_before_the_translator(client, monkeypatch):
    html = _home_html(client, monkeypatch, cookie="ko")
    assert '<html lang="ko"' in html
    assert html.index("global/js/i18n-ko.js") < html.index("global/js/i18n.js")


def test_deploy_default_applies_when_no_cookie_is_set(client, monkeypatch):
    html = _home_html(client, monkeypatch, env="ko")
    assert '<html lang="ko"' in html
    assert "global/js/i18n-ko.js" in html


# ---------------------------------------------------------------- translation core


@needs_node
def test_exact_match_keeps_surrounding_whitespace():
    assert _translate(SAMPLE, ["  Save "]) == ["  저장 "]


@needs_node
def test_internal_whitespace_is_normalised_before_lookup():
    assert _translate(SAMPLE, ["12\n   entities"]) == ["엔티티 12개"]


@needs_node
def test_number_template_substitutes_digits():
    assert _translate(SAMPLE, ["42 entities"]) == ["엔티티 42개"]


@needs_node
def test_known_prefix_keeps_dynamic_detail_verbatim():
    assert _translate(SAMPLE, ["Error deleting relationship: HTTP 500"]) == [
        "관계 삭제 오류: HTTP 500"
    ]


@needs_node
def test_trailing_punctuation_falls_back_to_core_text():
    assert _translate(SAMPLE, ["Loading..."]) == ["불러오는 중..."]


@needs_node
def test_unknown_text_and_non_latin_text_are_left_alone():
    assert _translate(SAMPLE, ["Customer_42_xyz", "한국어", "123"]) == [None, None, None]


# ---------------------------------------------------------------- DOM walker / skip regions


@needs_node
def test_plain_ui_text_is_translated():
    out = _translate_tree("h('div', {}, h('button', { id: 'b' }, 'Save'))", ["b"])
    assert out["b"]["text"] == "저장"


@needs_node
def test_user_label_inside_skipped_region_stays_english():
    tree = "h('div', {}, h('div', { 'data-i18n-skip': '' }, h('span', { id: 'name' }, 'Save')))"
    assert _translate_tree(tree, ["name"])["name"]["text"] == "Save"


@needs_node
def test_attributes_inside_skipped_region_stay_english():
    tree = (
        "h('div', {}, h('div', { 'data-i18n-skip': '' },"
        " h('span', { id: 'name', title: 'Mapping' }, 'x')),"
        " h('span', { id: 'chrome', title: 'Mapping' }, 'y'))"
    )
    out = _translate_tree(tree, ["name", "chrome"])
    assert out["name"]["title"] == "Mapping"
    assert out["chrome"]["title"] == "매핑"


@needs_node
def test_node_added_later_inside_skipped_region_stays_english():
    # The MutationObserver hands each added node to translateTree directly.
    tree = "h('div', {}, h('div', { 'data-i18n-skip': '' }, h('b', { id: 'late' }, 'Save')))"
    out = _translate_tree(tree, ["late"], target="root.getElementById('late')")
    assert out["late"]["text"] == "Save"


@needs_node
def test_text_node_added_later_inside_skipped_region_stays_english():
    tree = "h('div', {}, h('div', { 'data-i18n-skip': '' }, h('b', { id: 'late' }, 'Save')))"
    call = "root.getElementById('late').childNodes[0]"
    assert _translate_tree(tree, ["late"], target=call)["late"]["text"] == "Save"


# ---------------------------------------------------------------- dictionary


def test_korean_dictionary_is_valid_and_non_trivial():
    ko = _ko_dict()
    assert len(ko) > 1000
    assert all(isinstance(v, str) and v.strip() for v in ko.values())


def test_korean_dictionary_preserves_number_placeholders():
    bad = [k for k, v in _ko_dict().items() if k.count("{n}") != v.count("{n}")]
    assert bad == []


def test_korean_dictionary_keys_are_normalised():
    bad = [k for k in _ko_dict() if k != re.sub(r"\s+", " ", k).strip()]
    assert bad == []


def test_every_menu_label_has_a_korean_translation():
    labels = set()

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key in ("label", "title") and isinstance(value, str):
                    labels.add(re.sub(r"\s+", " ", value).strip())
                else:
                    walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(json.loads(MENU_CONFIG.read_text(encoding="utf-8")))
    ko = _ko_dict()
    missing = sorted(label for label in labels - KEEP_IN_ENGLISH if label and label not in ko)
    assert missing == []


@pytest.mark.parametrize(
    "english,korean",
    [
        ("Ontology", "온톨로지"),
        ("Mapping", "매핑"),
        ("Knowledge Graph", "지식 그래프"),
        ("Registry", "레지스트리"),
        ("Settings", "설정"),
        ("Save", "저장"),
        ("Cancel", "취소"),
    ],
)
def test_core_glossary_terms(english, korean):
    assert _ko_dict()[english] == korean


# ---------------------------------------------------------------- coverage ratchet
#
# Every English UI string in the templates and in the main script call sites
# must either have a Korean entry or be listed in i18n_untranslated_baseline.json
# (product names, code tokens, sentence fragments split by inline markup). New
# strings therefore fail here instead of silently shipping untranslated, the
# way the Insights KPI labels did.

_ATTRS = ("placeholder", "title", "aria-label", "data-bs-original-title", "data-bs-title")
_SKIP_TAGS = {"script", "style", "code", "pre", "textarea", "svg", "kbd", "samp"}
_VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param"}
_VOID |= {"source", "track", "wbr"}
_JINJA = re.compile(r"\{\{.*?\}\}|\{%.*?%\}|\{#.*?#\}", re.S)
_TRAILING_PUNCT = re.compile(r"^(.*?)(\s*(?:\.\.\.|…|[:.!?])+)$")
_SCRIPT_CALL = re.compile(
    r"\b(?:showNotification|showConfirmDialog)\(\s*(['\"])((?:\\.|(?!\1).)*)\1"
)
_SCRIPT_OPTION = re.compile(
    r"\b(?:title|message|label|confirmText|cancelText)\s*:\s*(['\"])((?:\\.|(?!\1).)*)\1"
)


def _normalise(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _is_translated(dict_: dict, key: str) -> bool:
    """Python mirror of translateWith() in i18n.js (exact, {n}, prefix, punctuation)."""

    def lookup(k):
        return k in dict_ or (bool(re.search(r"\d", k)) and re.sub(r"\d+", "{n}", k) in dict_)

    if lookup(key):
        return True
    colon = key.find(": ")
    if colon > 0 and (lookup(key[: colon + 1]) or lookup(key[:colon])):
        return True
    m = _TRAILING_PUNCT.match(key)
    return bool(m and m[1] and lookup(m[1]))


class _TemplateText(HTMLParser):
    """Collect translatable text / attribute values, honouring the skip rules of i18n.js."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack = []  # True per open element inside a skipped subtree
        self.found = []

    def _visible(self):
        return not (self.stack and self.stack[-1])

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        skip = not self._visible() or tag in _SKIP_TAGS or a.get("translate") == "no"
        skip = skip or "data-i18n-skip" in a
        skip = skip or "notranslate" in (a.get("class") or "").split()
        skip = skip or a.get("contenteditable") in ("", "true")
        if not skip:
            self.found.extend(a[n] for n in _ATTRS if a.get(n))
            if tag == "input" and a.get("type") in ("button", "submit") and a.get("value"):
                self.found.append(a["value"])
        if tag not in _VOID:
            self.stack.append(skip)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in _VOID and self.stack:
            self.stack.pop()

    def handle_endtag(self, tag):
        if tag not in _VOID and self.stack:
            self.stack.pop()

    def handle_data(self, data):
        if self._visible():
            self.found.append(data)


def _untranslated(pieces, dict_):
    out = set()
    for piece in pieces:
        key = _normalise(piece)
        if key and re.search(r"[A-Za-z]{2}", key) and not _is_translated(dict_, key):
            out.add(key)
    return out


def _untranslated_template_strings(dict_: dict) -> set:
    pieces = []
    for path in sorted(TEMPLATES.rglob("*.html")):
        raw = path.read_text(encoding="utf-8")
        raw = re.sub(r"<script\b.*?</script>", "", raw, flags=re.S | re.I)
        parser = _TemplateText()
        parser.feed(_JINJA.sub("\x00", raw))
        for found in parser.found:
            pieces.extend(found.split("\x00"))
    return _untranslated(pieces, dict_)


def _untranslated_script_strings(dict_: dict) -> set:
    pieces = []
    for path in sorted(STATIC.rglob("*.js")):
        if path.name.startswith("i18n"):
            continue
        src = path.read_text(encoding="utf-8")
        for rx in (_SCRIPT_CALL, _SCRIPT_OPTION):
            for m in rx.finditer(src):
                text = m.group(2).replace("\\'", "'").replace('\\"', '"')
                if "<" not in text:
                    pieces.append(text)
    return _untranslated(pieces, dict_)


def _baseline(section: str) -> set:
    return set(json.loads(UNTRANSLATED_BASELINE.read_text(encoding="utf-8"))[section])


def test_new_template_strings_have_a_korean_translation():
    new = sorted(_untranslated_template_strings(_ko_dict()) - _baseline("templates"))
    assert new == [], "Add these to i18n-ko.js (or, if not UI copy, to the baseline)"


def test_new_script_strings_have_a_korean_translation():
    new = sorted(_untranslated_script_strings(_ko_dict()) - _baseline("scripts"))
    assert new == [], "Add these to i18n-ko.js (or, if not UI copy, to the baseline)"


@pytest.mark.parametrize("section", ["templates", "scripts"])
def test_untranslated_baseline_has_no_stale_entries(section):
    current = (
        _untranslated_template_strings(_ko_dict())
        if section == "templates"
        else _untranslated_script_strings(_ko_dict())
    )
    assert sorted(_baseline(section) - current) == []


# ---------------------------------------------------------------- wiring


def _opening_tag(html: str, element_id: str) -> str:
    m = re.search(r"<[a-zA-Z][^>]*\bid=\"%s\"[^>]*>" % re.escape(element_id), html)
    return m.group(0) if m else ""


@pytest.mark.parametrize("element_id", USER_CONTENT_ROOTS)
def test_user_content_roots_are_skip_regions(element_id):
    tags = [
        _opening_tag(p.read_text(encoding="utf-8"), element_id) for p in TEMPLATES.rglob("*.html")
    ]
    tags = [t for t in tags if t]
    assert tags, "template element #%s not found" % element_id
    assert all("data-i18n-skip" in t for t in tags)


def test_script_rendered_user_names_are_skip_regions():
    panels = (STATIC / "ontology/js/ontology-shared-panels.js").read_text(encoding="utf-8")
    # Entity / relationship names are skipped; static titles such as
    # "Select Item" or "Add Entity" in the same slot stay translatable.
    assert '<span id="sharedPanelItemName">${' not in panels
    assert '<span id="sharedPanelItemName" data-i18n-skip>${cls.name}</span>' in panels
    assert '<span id="sharedPanelItemName" data-i18n-skip>${prop.name}</span>' in panels
    assert '<span id="sharedPanelItemName">Select Item</span>' in panels
    comments = (STATIC_JS / "comments-panel.js").read_text(encoding="utf-8")
    assert '"oc-text oc-md" data-i18n-skip' in comments
    assert '"oc-author" data-i18n-skip' in comments


def test_name_slots_with_static_placeholder_are_skipped_only_while_showing_a_name():
    manual = (STATIC / "mapping/js/mapping-manual.js").read_text(encoding="utf-8")
    assert "titleSpan.setAttribute('data-i18n-skip', '');" in manual
    assert "titleSpan.removeAttribute('data-i18n-skip');" in manual
    metadata = (STATIC / "domain/js/domain-metadata.js").read_text(encoding="utf-8")
    assert "tableNameEl.setAttribute('data-i18n-skip', '');" in metadata


def test_native_dialogs_are_not_wrapped():
    js = I18N_JS.read_text(encoding="utf-8")
    assert "window[name] =" not in js
    assert "'alert', 'confirm', 'prompt'" not in js


def test_language_cookie_is_secure_on_https():
    js = I18N_JS.read_text(encoding="utf-8")
    assert "location.protocol === 'https:'" in js
    assert "'; Secure'" in js


def test_i18n_scripts_load_after_permissions_and_before_navbar():
    html = BASE_HTML.read_text(encoding="utf-8")
    perms = html.index("global/js/permissions.js")
    dictionary = html.index("filename='global/js/i18n-' ~ _ui_lang ~ '.js'")
    core = html.index("global/js/i18n.js")
    navbar = html.index("global/js/navbar.js")
    assert perms < dictionary < core < navbar


def test_i18n_css_loads_before_permissions_css():
    html = BASE_HTML.read_text(encoding="utf-8")
    assert html.index("global/css/i18n.css") < html.index("global/css/permissions.css")


def test_user_menu_offers_both_languages():
    html = BASE_HTML.read_text(encoding="utf-8")
    assert 'data-ob-lang="en"' in html
    assert 'data-ob-lang="ko"' in html


def test_hidden_until_translated_has_a_reveal_failsafe():
    css = I18N_CSS.read_text(encoding="utf-8")
    assert ":not(.ob-i18n-ready) body" in css
    assert "animation: ob-i18n-reveal" in css


def test_skip_selectors_cover_code_editors_and_user_content():
    js = I18N_JS.read_text(encoding="utf-8")
    for selector in ("'code'", "'pre'", "'textarea'", "'svg'", "'[translate=\"no\"]'"):
        assert selector in js
    assert "'[data-i18n-skip]'" in js


def test_admin_only_navbar_dropdown_items_keep_flex_layout():
    # permissions.css reveals admin-only items with display:block, which broke
    # the icon + label flex row of navbar dropdowns (uneven icon spacing).
    css = (STATIC / "global/css/permissions.css").read_text(encoding="utf-8")
    rule = (
        'body[data-app-role="admin"] .navbar .dropdown-menu '
        '.dropdown-item[data-requires-app="admin"]'
    )
    start = css.index(rule)
    block = css[start : css.index("}", start)]
    assert "display: flex !important;" in block


def test_settings_ui_section_has_language_selector():
    html = (TEMPLATES / "settings.html").read_text(encoding="utf-8")
    ui = html[html.index('id="ui-section"') :]
    # Language sits right below Branding and above Theme.
    assert ui.index("Branding</h5>") < ui.index('id="uiLanguageCard"') < ui.index("Theme</h5>")
    card = ui[ui.index('id="uiLanguageCard"') : ui.index("Theme</h5>")]
    assert "data-ob-lang-select" in card
    assert '<option value="en" translate="no">English</option>' in card
    assert '<option value="ko" translate="no">한국어</option>' in card


def test_language_select_switches_on_change():
    js = I18N_JS.read_text(encoding="utf-8")
    assert "[data-ob-lang-select]" in js
    assert "addEventListener('change'" in js


def test_settings_ui_cards_do_not_scroll_internally():
    css = (STATIC / "global/css/config.css").read_text(encoding="utf-8")
    start = css.index("#ui-section > .content-section > .card {")
    block = css[start : css.index("}", start)]
    assert "flex: 0 0 auto;" in block
    assert "overflow: visible;" in block
