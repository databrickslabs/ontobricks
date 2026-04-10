#!/usr/bin/env python3
"""Main entry point for OntoBricks application (FastAPI)."""
import os
import sys
import traceback

_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_ROOT, "src"))

startup_error = None
app = None

try:
    import uvicorn
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Configure structured logging (must happen before any app import)
    from back.core.logging import setup_logging
    setup_logging()
    
    # Import and create the FastAPI app
    from shared.fastapi.main import create_app
    app = create_app()
    
except Exception as e:
    startup_error = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
    print(f"STARTUP ERROR: {startup_error}", flush=True)

# Fallback app if main app fails to load
if app is None:
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse
    import uvicorn
    
    app = FastAPI(title="OntoBricks - Error")
    
    @app.get("/", response_class=HTMLResponse)
    def error_page():
        error_html = startup_error.replace('\n', '<br>') if startup_error else "Unknown error"
        return f"""
        <html>
        <head><title>OntoBricks - Startup Error</title></head>
        <body style="font-family: monospace; padding: 20px;">
            <h1 style="color: red;">OntoBricks Failed to Start</h1>
            <h3>Error:</h3>
            <pre style="background: #f0f0f0; padding: 15px; overflow: auto;">{error_html}</pre>
            <p>Please check the logs for more details.</p>
        </body>
        </html>
        """
    
    @app.get("/health")
    def health():
        return {"status": "error", "message": "App failed to start", "error": startup_error}

if __name__ == '__main__':
    import logging
    from shared.config.constants import APP_LOGGER_NAME
    _log = logging.getLogger(APP_LOGGER_NAME)

    port = int(os.getenv('DATABRICKS_APP_PORT', 8000))
    is_databricks_app = os.getenv('DATABRICKS_APP_PORT') is not None
    
    _log.info("Starting uvicorn — port=%d, databricks_mode=%s", port, is_databricks_app)
    
    if is_databricks_app:
        uvicorn.run(
            app,
            host='0.0.0.0',
            port=port,
            log_level="info",
            log_config=None,
        )
    else:
        uvicorn.run(
            "shared.fastapi.main:app",
            host='127.0.0.1',
            port=port,
            reload=True,
            reload_dirs=["src/back", "src/front", "src/api", "src/shared", "src/agents"],
            log_level="info",
            log_config=None,
        )
