"""The live-mode harness must not send the workspace bearer to third-party hosts.

Live e2e contexts carry ``Authorization: Bearer <token>`` in
``extra_http_headers``, which Playwright adds to every request, including
CDN sub-resources (the app loads D3 from jsdelivr). ``_install_redirect_fix``
strips it for any host other than the app. Two local HTTP servers stand in for
the app and a CDN; the CDN records what it receives.
"""

from __future__ import annotations

import http.server
import threading

import pytest

pytestmark = pytest.mark.e2e

TOKEN = "Bearer test-token-not-real"


def _serve(body: str, ctype: str, seen: list) -> http.server.ThreadingHTTPServer:
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 -- http.server API
            seen.append(self.headers.get("Authorization"))
            data = body.encode()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def test_third_party_host_never_receives_the_bearer():
    sync_api = pytest.importorskip("playwright.sync_api")
    from tests.e2e.conftest import _install_redirect_fix

    app_seen, cdn_seen = [], []
    cdn = _serve("window.lib = 1;", "application/javascript", cdn_seen)
    page_html = '<html><body><script src="http://127.0.0.1:%d/lib.js"></script></body></html>' % cdn.server_port
    app = _serve(page_html, "text/html", app_seen)
    base = "http://127.0.0.1:%d" % app.server_port
    try:
        with sync_api.sync_playwright() as pw:
            try:
                browser = pw.chromium.launch(headless=True)
            except Exception as exc:  # noqa: BLE001 -- no browser installed here
                pytest.skip("Playwright Chromium not installed: %s" % str(exc).splitlines()[0])
            ctx = browser.new_context(extra_http_headers={"Authorization": TOKEN})
            _install_redirect_fix(ctx, base, TOKEN.split(" ", 1)[1])
            ctx.new_page().goto(base + "/settings", wait_until="networkidle")
            ctx.close()
            browser.close()
    finally:
        app.shutdown()
        cdn.shutdown()
    assert cdn_seen and all(h is None for h in cdn_seen), cdn_seen
