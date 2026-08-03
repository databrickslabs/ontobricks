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


class TestExcludedPredicateMerge:
    """A caller's exclusions add to the defaults; they never replace them.

    The app started forwarding ``MetricsRequest.predicate_filter`` when analytics
    became job-only. Treating that list as the complete set would turn
    ``rdf:type`` and ``rdfs:label`` into entity-entity edges, wiring every
    instance to its class node and inflating every degree and PageRank — a wrong
    number rather than a visible failure.
    """

    def test_no_filter_yields_exactly_the_defaults(self):
        assert job.merge_excluded_predicates(None) == list(
            job.DEFAULT_EXCLUDED_PREDICATES
        )
        assert job.merge_excluded_predicates("") == list(
            job.DEFAULT_EXCLUDED_PREDICATES
        )

    def test_a_caller_predicate_is_added_to_the_defaults(self):
        merged = job.merge_excluded_predicates("http://ex.org/noisy")
        assert job.RDF_TYPE in merged
        assert job.RDFS_LABEL in merged
        assert "http://ex.org/noisy" in merged

    def test_a_default_is_not_repeated_when_the_caller_names_it(self):
        merged = job.merge_excluded_predicates(f"{job.RDF_TYPE}, http://ex.org/noisy")
        assert merged.count(job.RDF_TYPE) == 1

    def test_whitespace_and_empty_entries_are_ignored(self):
        merged = job.merge_excluded_predicates(" , http://ex.org/noisy , ")
        assert merged == list(job.DEFAULT_EXCLUDED_PREDICATES) + [
            "http://ex.org/noisy"
        ]
