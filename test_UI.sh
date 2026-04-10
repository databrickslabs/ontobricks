#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

REPORTS_DIR="tests/reports"
TIMESTAMP="$(date '+%Y-%m-%d_%H-%M-%S')"
REPORT_FILE="${REPORTS_DIR}/${TIMESTAMP}_ui_tests_report.md"
VENV_PYTHON=".venv/bin/python"

mkdir -p "$REPORTS_DIR"

if [[ ! -x "$VENV_PYTHON" ]]; then
    echo "ERROR: Virtual environment not found at $VENV_PYTHON"
    echo "Run 'uv sync --dev' first."
    exit 1
fi

echo "============================================"
echo " OntoBricks UI Test Campaign"
echo " $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"
echo ""

RAW_L1=$(mktemp)
RAW_L2=$(mktemp)
trap 'rm -f "$RAW_L1" "$RAW_L2"' EXIT

# -----------------------------------------------
# Layer 1: HTML Rendering Tests (stdlib html.parser)
# -----------------------------------------------
echo "[1/4] Layer 1 -- HTML Rendering Tests (stdlib html.parser)..."
set +e
$VENV_PYTHON -m pytest tests/test_ui_rendering.py -v --tb=short 2>&1 | tee "$RAW_L1"
L1_EXIT=${PIPESTATUS[0]}
set -e
echo ""

L1_SUMMARY=$(grep -E '(passed|failed|error)' "$RAW_L1" | tail -1)
L1_PASSED=$(echo "$L1_SUMMARY" | awk '{for(i=1;i<=NF;i++) if($i=="passed" || $i=="passed,") print $(i-1)}')
L1_FAILED=$(echo "$L1_SUMMARY" | awk '{for(i=1;i<=NF;i++) if($i=="failed" || $i=="failed,") print $(i-1)}')
L1_ERRORS=$(echo "$L1_SUMMARY" | awk '{for(i=1;i<=NF;i++) if(index($i,"error")) print $(i-1)}')
L1_DURATION=$(echo "$L1_SUMMARY" | grep -oE '[0-9]+\.[0-9]+s' | tail -1)
L1_PASSED=${L1_PASSED:-0}; L1_FAILED=${L1_FAILED:-0}; L1_ERRORS=${L1_ERRORS:-0}; L1_DURATION=${L1_DURATION:-?}

# -----------------------------------------------
# Layer 2: End-to-End Browser Tests (Playwright)
# -----------------------------------------------
echo "[2/4] Checking Playwright browser install..."
if ! $VENV_PYTHON -c "from playwright.sync_api import sync_playwright; p=sync_playwright().start(); b=p.chromium.launch(headless=True); b.close(); p.stop()" 2>/dev/null; then
    echo "  -> Installing Chromium for Playwright..."
    $VENV_PYTHON -m playwright install chromium 2>&1 | tail -2
fi

echo "[3/4] Layer 2 -- End-to-End Browser Tests (Playwright)..."
set +e
$VENV_PYTHON -m pytest tests/e2e/ -v --tb=short 2>&1 | tee "$RAW_L2"
L2_EXIT=${PIPESTATUS[0]}
set -e
echo ""

L2_SUMMARY=$(grep -E '(passed|failed|error)' "$RAW_L2" | tail -1)
L2_PASSED=$(echo "$L2_SUMMARY" | awk '{for(i=1;i<=NF;i++) if($i=="passed" || $i=="passed,") print $(i-1)}')
L2_FAILED=$(echo "$L2_SUMMARY" | awk '{for(i=1;i<=NF;i++) if($i=="failed" || $i=="failed,") print $(i-1)}')
L2_ERRORS=$(echo "$L2_SUMMARY" | awk '{for(i=1;i<=NF;i++) if(index($i,"error")) print $(i-1)}')
L2_DURATION=$(echo "$L2_SUMMARY" | grep -oE '[0-9]+\.[0-9]+s' | tail -1)
L2_PASSED=${L2_PASSED:-0}; L2_FAILED=${L2_FAILED:-0}; L2_ERRORS=${L2_ERRORS:-0}; L2_DURATION=${L2_DURATION:-?}

# -----------------------------------------------
# Totals
# -----------------------------------------------
TOTAL_PASSED=$((L1_PASSED + L2_PASSED))
TOTAL_FAILED=$((L1_FAILED + L2_FAILED))
TOTAL_ERRORS=$((L1_ERRORS + L2_ERRORS))

if [[ $TOTAL_FAILED -gt 0 ]] || [[ $TOTAL_ERRORS -gt 0 ]]; then
    OVERALL="FAIL"
    FINAL_EXIT=1
else
    OVERALL="PASS"
    FINAL_EXIT=0
fi

# -----------------------------------------------
# Per-test results (both layers combined)
# -----------------------------------------------
L1_DETAILS=$(mktemp)
L2_DETAILS=$(mktemp)
trap 'rm -f "$RAW_L1" "$RAW_L2" "$L1_DETAILS" "$L2_DETAILS"' EXIT

awk '/::.*(PASSED|FAILED|ERROR)/ {
    name=$0; gsub(/^tests\//, "", name); sub(/ +PASSED.*/, "", name); sub(/ +FAILED.*/, "", name); sub(/ +ERROR.*/, "", name)
    if (match($0, / PASSED/))      printf "| `%s` | PASS |\n", name
    else if (match($0, / FAILED/)) printf "| `%s` | **FAIL** |\n", name
    else if (match($0, / ERROR/))  printf "| `%s` | **ERROR** |\n", name
}' "$RAW_L1" > "$L1_DETAILS"

awk '/::.*(PASSED|FAILED|ERROR)/ {
    name=$0; gsub(/^tests\//, "", name); sub(/ +PASSED.*/, "", name); sub(/ +FAILED.*/, "", name); sub(/ +ERROR.*/, "", name)
    if (match($0, / PASSED/))      printf "| `%s` | PASS |\n", name
    else if (match($0, / FAILED/)) printf "| `%s` | **FAIL** |\n", name
    else if (match($0, / ERROR/))  printf "| `%s` | **ERROR** |\n", name
}' "$RAW_L2" > "$L2_DETAILS"

# -----------------------------------------------
# Failure details
# -----------------------------------------------
L1_FAIL_DETAILS=""
if [[ "$L1_FAILED" -gt 0 ]] || [[ "$L1_ERRORS" -gt 0 ]]; then
    L1_FAIL_DETAILS=$(awk '/^=+ short test summary/,0' "$RAW_L1" | head -30 || true)
fi
L2_FAIL_DETAILS=""
if [[ "$L2_FAILED" -gt 0 ]] || [[ "$L2_ERRORS" -gt 0 ]]; then
    L2_FAIL_DETAILS=$(awk '/^=+ short test summary/,0' "$RAW_L2" | head -30 || true)
fi

# -----------------------------------------------
# Versions
# -----------------------------------------------
PY_VERSION=$($VENV_PYTHON --version 2>&1 | awk '{print $2}')
PT_VERSION=$($VENV_PYTHON -m pytest --version 2>&1 | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
PW_VERSION=$($VENV_PYTHON -c "from importlib.metadata import version; print(version('playwright'))" 2>/dev/null || echo "?")

# -----------------------------------------------
# Generate report
# -----------------------------------------------
echo "[4/4] Generating report -> $REPORT_FILE"

cat > "$REPORT_FILE" <<REPORT_EOF
# OntoBricks UI Test Campaign Report

**Date:** $(date '+%Y-%m-%d %H:%M:%S')
**Platform:** $(uname -s) -- Python ${PY_VERSION}, pytest ${PT_VERSION}
**Tools:** stdlib html.parser, Playwright ${PW_VERSION}
**Overall:** ${OVERALL}

---

## Summary

| Layer | Description | Passed | Failed | Errors | Duration | Status |
|-------|-------------|--------|--------|--------|----------|--------|
| Layer 1 | HTML Rendering (stdlib html.parser) | ${L1_PASSED} | ${L1_FAILED} | ${L1_ERRORS} | ${L1_DURATION} | $([ "$L1_EXIT" -eq 0 ] && echo "PASS" || echo "FAIL") |
| Layer 2 | E2E Browser (Playwright) | ${L2_PASSED} | ${L2_FAILED} | ${L2_ERRORS} | ${L2_DURATION} | $([ "$L2_EXIT" -eq 0 ] && echo "PASS" || echo "FAIL") |
| **Total** | | **${TOTAL_PASSED}** | **${TOTAL_FAILED}** | **${TOTAL_ERRORS}** | | **${OVERALL}** |

---

## Layer 1 -- HTML Rendering Tests

Tests DOM structure of all pages using \`TestClient\` + stdlib \`html.parser\`.
No browser required.

**File:** \`tests/test_ui_rendering.py\`

| Test | Status |
|------|--------|
$(cat "$L1_DETAILS")

REPORT_EOF

if [[ -n "$L1_FAIL_DETAILS" ]]; then
cat >> "$REPORT_FILE" <<L1FAIL_EOF

### Layer 1 Failures

\`\`\`
${L1_FAIL_DETAILS}
\`\`\`

L1FAIL_EOF
fi

cat >> "$REPORT_FILE" <<L2HEADER_EOF
---

## Layer 2 -- End-to-End Browser Tests

Tests navigation, sidebar switching, and interactive elements using Playwright + Chromium.

**File:** \`tests/e2e/test_e2e_flows.py\`

| Test | Status |
|------|--------|
$(cat "$L2_DETAILS")

L2HEADER_EOF

if [[ -n "$L2_FAIL_DETAILS" ]]; then
cat >> "$REPORT_FILE" <<L2FAIL_EOF

### Layer 2 Failures

\`\`\`
${L2_FAIL_DETAILS}
\`\`\`

L2FAIL_EOF
fi

cat >> "$REPORT_FILE" <<FOOTER_EOF
---

## How to Run

\`\`\`bash
# Layer 1 only (fast, ~16s, no browser needed)
.venv/bin/python -m pytest tests/test_ui_rendering.py -v

# Layer 2 only (~42s, requires Chromium)
.venv/bin/python -m playwright install chromium
.venv/bin/python -m pytest tests/e2e/ -v

# Both layers
./test_UI.sh
\`\`\`

---

*Report generated by test_UI.sh on $(date '+%Y-%m-%d %H:%M:%S')*
FOOTER_EOF

echo ""
echo "============================================"
echo " UI Test Campaign Complete"
echo "============================================"
echo " Layer 1: ${L1_PASSED} passed, ${L1_FAILED} failed (${L1_DURATION})"
echo " Layer 2: ${L2_PASSED} passed, ${L2_FAILED} failed (${L2_DURATION})"
echo " Total:   ${TOTAL_PASSED} passed, ${TOTAL_FAILED} failed"
echo " Overall: ${OVERALL}"
echo " Report:  ${REPORT_FILE}"
echo "============================================"

exit $FINAL_EXIT
