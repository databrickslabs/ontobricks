"""Schema-drift re-render must not flash a second mapping-designer spinner.

After a mapping change, refreshMappingDesign() rebuilds the canvas (spinner)
and re-fetches schema drift. When that fetch resolves it re-inits the designer
so drift markers appear. That second pass must be silent — otherwise the
overlay blinks twice whenever the warehouse call is slower than the first paint.
"""

from pathlib import Path
import re


REPO_ROOT = Path(__file__).resolve().parents[3]
DESIGN_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-design.js"


def _js() -> str:
    return DESIGN_JS.read_text(encoding="utf-8")


def test_refresh_resets_drift_and_inits_designer():
    js = _js()
    refresh = re.search(
        r"function refreshMappingDesign\(\)\s*\{(.*?)\n\}",
        js,
        re.DOTALL,
    )
    assert refresh, "refreshMappingDesign is missing"
    body = refresh.group(1)
    assert "mappingDriftLoaded = false" in body
    assert "initMappingDesigner()" in body


def test_drift_callback_reinits_without_loading_overlay():
    js = _js()
    assert "initMappingDesigner({ showLoading: false })" in js
    assert re.search(
        r"loadSchemaDrift\(\)\.then\(\(\)\s*=>\s*\{[^}]*showLoading:\s*false",
        js,
        re.DOTALL,
    )


def test_init_skips_overlay_when_show_loading_is_false():
    js = _js()
    init = re.search(
        r"async function initMappingDesigner\(([^)]*)\)\s*\{",
        js,
    )
    assert init, "initMappingDesigner is missing"
    assert init.group(1).strip() in ("opts", "options", "opts = {}")
    assert "showMappingDesignerLoading(true)" in js
    assert re.search(
        r"if\s*\(\s*showLoading\s*\)\s*\{?\s*showMappingDesignerLoading\(true\)",
        js,
        re.DOTALL,
    )
