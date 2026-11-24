"""Unmapping in the Mapping designer must keep hidden (excluded) items hidden.

Excluded entities/relationships render grey; unmapped ones render red. Every
unmap path used to splice the whole entry out of ``MappingState.config``, which
also deleted the ``excluded`` flag. Unmapping an excluded entity therefore
un-hid it, and every unmapped relationship attached to it (grey only because an
endpoint was excluded) turned red. "Unmap all" wiped every exclusion.

The shared helper ``unmapMappingEntry`` lives in ``mapping-core.js``; the
behaviour tests run it (and ``resetAllMappings``) under node.
"""

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
CORE_JS = REPO_ROOT / "src/front/static/global/js/mapping-core.js"
INFO_JS = REPO_ROOT / "src/front/static/global/js/mapping-information.js"
MAPPING_JS_DIR = REPO_ROOT / "src/front/static/mapping/js"

needs_node = pytest.mark.skipif(
    shutil.which("node") is None, reason="node is required to run the frontend module"
)

_PRELUDE = """
const fs = require('fs');
const vm = require('vm');
global.window = global;
global.addEventListener = () => {};
global.document = {
    readyState: 'complete',
    getElementById: () => null,
    querySelector: () => null,
    querySelectorAll: () => [],
    addEventListener: () => {},
};
const load = (p) => vm.runInThisContext(fs.readFileSync(p, 'utf8'), { filename: p });
console.log = () => {};
console.warn = () => {};
"""


def _run(body: str, *sources: Path) -> dict:
    loads = "\n".join(f"load({json.dumps(str(p))});" for p in sources)
    script = f"{_PRELUDE}\n{loads}\n(async () => {{\n{body}\n}})().catch(e => {{ process.stderr.write(String(e && e.stack || e)); process.exit(1); }});"
    completed = subprocess.run(
        ["node", "-e", script], capture_output=True, text=True, timeout=30
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


@needs_node
class TestUnmapMappingEntry:
    def _unmap(self, entries, key, uri, label):
        body = f"""
        const entries = {json.dumps(entries)};
        const found = unmapMappingEntry(entries, {json.dumps(key)}, {json.dumps(uri)}, {json.dumps(label)});
        process.stdout.write(JSON.stringify({{ found, entries }}));
        """
        return _run(body, CORE_JS)

    def test_plain_mapping_is_removed(self):
        out = self._unmap(
            [{"ontology_class": "A", "sql_query": "SELECT 1"}],
            "ontology_class", "A", "ontology_class_label",
        )
        assert out == {"found": True, "entries": []}

    def test_excluded_entry_keeps_its_exclusion(self):
        out = self._unmap(
            [{
                "ontology_class": "A",
                "ontology_class_label": "Alpha",
                "sql_query": "SELECT 1",
                "id_column": "id",
                "excluded": True,
            }],
            "ontology_class", "A", "ontology_class_label",
        )
        assert out["found"] is True
        assert out["entries"] == [{
            "ontology_class": "A",
            "ontology_class_label": "Alpha",
            "sql_query": "",
            "excluded": True,
        }]

    def test_excluded_attributes_survive_without_excluding_the_item(self):
        out = self._unmap(
            [{
                "property": "p",
                "property_label": "p",
                "sql_query": "SELECT 1",
                "excluded_attributes": ["since"],
            }],
            "property", "p", "property_label",
        )
        assert out["entries"] == [{
            "property": "p",
            "property_label": "p",
            "sql_query": "",
            "excluded_attributes": ["since"],
        }]
        assert "excluded" not in out["entries"][0]

    def test_both_kinds_of_exclusion_are_kept(self):
        out = self._unmap(
            [{"property": "p", "sql_query": "x", "excluded": True,
              "excluded_attributes": ["a", "b"]}],
            "property", "p", "property_label",
        )
        stub = out["entries"][0]
        assert stub["excluded"] is True
        assert stub["excluded_attributes"] == ["a", "b"]
        assert stub["sql_query"] == ""

    def test_unknown_uri_changes_nothing(self):
        entries = [{"ontology_class": "A", "excluded": True}]
        out = self._unmap(entries, "ontology_class", "ZZZ", "ontology_class_label")
        assert out == {"found": False, "entries": entries}

    def test_only_the_targeted_entry_is_touched(self):
        out = self._unmap(
            [
                {"ontology_class": "A", "sql_query": "x"},
                {"ontology_class": "B", "sql_query": "y", "excluded": True},
                {"ontology_class": "C", "sql_query": "z"},
            ],
            "ontology_class", "B", "ontology_class_label",
        )
        assert [e["ontology_class"] for e in out["entries"]] == ["A", "B", "C"]
        assert out["entries"][1]["excluded"] is True
        assert out["entries"][1]["sql_query"] == ""
        assert out["entries"][0]["sql_query"] == "x"

    def test_attribute_list_is_copied_not_aliased(self):
        body = """
        const entries = [{ property: 'p', sql_query: 'x', excluded_attributes: ['a'] }];
        const original = entries[0].excluded_attributes;
        unmapMappingEntry(entries, 'property', 'p', 'property_label');
        entries[0].excluded_attributes.push('b');
        process.stdout.write(JSON.stringify({ original }));
        """
        assert _run(body, CORE_JS) == {"original": ["a"]}


@needs_node
class TestUnmapAllKeepsHiddenItems:
    def _unmap_all(self, server_after_clear):
        """Run resetAllMappings against a fake server; return the recorded calls."""
        body = f"""
        const calls = [];
        global.fetch = async (url, opts) => {{
            calls.push({{ url, method: (opts && opts.method) || 'GET', body: opts && opts.body }});
            if (url.startsWith('/mapping/load')) {{
                return {{ json: async () => ({{ success: true, config: {json.dumps(server_after_clear)} }}) }};
            }}
            return {{ json: async () => ({{ success: true }}) }};
        }};
        global.showNotification = () => {{}};
        global.updateMappingCompletionStatus = () => {{}};
        global.refreshMappingLiveStatus = () => {{}};

        MappingState.loadedOntology = {{
            classes: [{{ uri: 'E1' }}, {{ uri: 'E2' }}],
            properties: [{{ uri: 'R1' }}, {{ uri: 'R2' }}],
        }};
        MappingState.config = {{
            entities: [
                {{ ontology_class: 'E1', sql_query: 'x', excluded: true }},
                {{ ontology_class: 'E2', sql_query: 'y' }},
            ],
            relationships: [{{ property: 'R1', sql_query: 'x', excluded: true }}, {{ property: 'R2', sql_query: 'y' }}],
        }};

        await resetAllMappings();

        process.stdout.write(JSON.stringify({{
            calls,
            config: MappingState.config,
            excludedClasses: MappingState.loadedOntology.classes.filter(c => c.excluded).map(c => c.uri),
            excludedProps: MappingState.loadedOntology.properties.filter(p => p.excluded).map(p => p.uri),
        }}));
        """
        return _run(body, CORE_JS, INFO_JS)

    SERVER_AFTER_CLEAR = {
        "entities": [{"ontology_class": "E1", "excluded": True}],
        "relationships": [{"property": "R1", "excluded": True}],
    }

    def test_never_posts_an_empty_config_over_the_server_state(self):
        out = self._unmap_all(self.SERVER_AFTER_CLEAR)
        saves = [c for c in out["calls"] if c["url"] == "/mapping/save"]
        assert saves == [], "an empty /mapping/save would wipe the exclusions"

    def test_asks_the_server_to_keep_exclusions(self):
        out = self._unmap_all(self.SERVER_AFTER_CLEAR)
        clears = [c for c in out["calls"] if c["url"].startswith("/mapping/clear")]
        assert len(clears) == 1
        assert "keep_exclusions=true" in clears[0]["url"]
        assert clears[0]["method"] == "POST"

    def test_reloads_the_kept_exclusions_and_restamps_the_ontology(self):
        out = self._unmap_all(self.SERVER_AFTER_CLEAR)
        urls = [c["url"] for c in out["calls"]]
        assert urls.index("/mapping/load") > next(
            i for i, u in enumerate(urls) if u.startswith("/mapping/clear")
        )
        assert out["config"]["entities"] == self.SERVER_AFTER_CLEAR["entities"]
        assert out["config"]["relationships"] == self.SERVER_AFTER_CLEAR["relationships"]
        assert out["excludedClasses"] == ["E1"]
        assert out["excludedProps"] == ["R1"]


class TestNoUnmapPathSplicesAwayExclusions:
    def test_no_mapping_script_splices_entries_out_of_the_config(self):
        offenders = []
        pattern = re.compile(r"config\.(entities|relationships)\.splice\(")
        for js in sorted(MAPPING_JS_DIR.glob("*.js")):
            for no, line in enumerate(js.read_text(encoding="utf-8").splitlines(), 1):
                if pattern.search(line):
                    offenders.append(f"{js.name}:{no}: {line.strip()}")
        assert not offenders, "use unmapMappingEntry() instead:\n" + "\n".join(offenders)

    @pytest.mark.parametrize(
        "script, minimum",
        [
            ("mapping-design.js", 4),
            ("mapping-manual.js", 2),
            ("mapping-entity-modal.js", 1),
            ("mapping-relationship-modal.js", 1),
        ],
    )
    def test_every_unmap_site_uses_the_shared_helper(self, script, minimum):
        text = (MAPPING_JS_DIR / script).read_text(encoding="utf-8")
        assert text.count("unmapMappingEntry(") >= minimum
