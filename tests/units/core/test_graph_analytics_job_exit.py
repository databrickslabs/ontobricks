"""A successful analytics run must not leave the CLI via ``SystemExit``.

Databricks serverless executes ``spark_python_task`` files inside an IPython
shell. A raised ``SystemExit`` — even with code 0 — is reported there as an
uncaught exception, so the run terminates with ``RUN_EXECUTION_ERROR`` while the
metrics table has in fact been written. That is exactly what happened to run
314895410340960 on 2026-07-30: the driver log ended with
``wrote … (1349202 nodes)`` and the summary line, then ``SystemExit: 0`` failed
the workload and the UI reported the analysis as failed.

Genuine failures must still surface, so a non-zero return has to keep raising.
"""

import pytest

from jobs import graph_analytics_job as job

pytestmark = pytest.mark.unit


class TestCliExitCode:
    def test_success_does_not_raise(self, monkeypatch):
        """The IPython harness treats any raised SystemExit as a failed run."""
        monkeypatch.setattr(job, "main", lambda argv=None: 0)
        assert job.run_cli([]) is None

    def test_none_return_is_a_success(self, monkeypatch):
        monkeypatch.setattr(job, "main", lambda argv=None: None)
        assert job.run_cli([]) is None

    def test_failure_still_exits_non_zero(self, monkeypatch):
        monkeypatch.setattr(job, "main", lambda argv=None: 2)
        with pytest.raises(SystemExit) as excinfo:
            job.run_cli([])
        assert excinfo.value.code == 2

    def test_argv_is_forwarded(self, monkeypatch):
        seen = {}

        def fake_main(argv=None):
            seen["argv"] = argv
            return 0

        monkeypatch.setattr(job, "main", fake_main)
        job.run_cli(["--source-table", "a.b.c"])
        assert seen["argv"] == ["--source-table", "a.b.c"]

    def test_the_module_guard_uses_run_cli(self):
        """A bare ``raise SystemExit(main())`` is what broke the run."""
        source = job.__file__.replace(".pyc", ".py")
        with open(source, encoding="utf-8") as fh:
            text = fh.read()
        assert "raise SystemExit(main())" not in text
        assert "run_cli()" in text.rsplit('if __name__ == "__main__":', 1)[1]
