"""``TwinRealtimeStream``: start, stop and status of a domain version's stream job (fake SDK)."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from back.core.errors import InfrastructureError, NotFoundError, ValidationError
from back.objects.digitaltwin.build import TwinIncrementalBuild, TwinRealtimeStream

pytestmark = pytest.mark.unit

_SOURCE = {
    "kind": "kafka",
    "bootstrap_servers": "b:9092",
    "topic": "sales.customers",
    "secret_scope": "ontobricks",
    "secret_key": "kafka-jaas",
    "key_format": "json",
}


def _settings(**over):
    base = dict(
        realtime_enabled=True,
        realtime_job_name="",
        ontobricks_app_name="ontobricks-030",
        realtime_lakebase_user="ontobricks_rtm",
        realtime_lakebase_secret="ontobricks/pg-password",
        realtime_checkpoint_root="/Volumes/main/ob/registry/realtime/",
        databricks_triplestore_table="",
    )
    base.update(over)
    return SimpleNamespace(**base)


def _domain(status="PUBLISHED", source=None):
    return SimpleNamespace(
        info={"name": "sales", "status": status, "realtime_source": dict(source or _SOURCE)},
        current_version="3",
        delta={"catalog": "main", "schema": "kg"},
    )


class _Live:
    def __init__(self):
        self.ensured: list = []

    @staticmethod
    def table_name(graph):
        return f"{graph.lower()}_live"

    def ensure(self, graph):
        self.ensured.append(graph)
        return self.table_name(graph)

    def connection(self):
        return {"host": "pg.lakebase.io", "database": "graphs", "schema": "ontobricks_graph"}


class _Store:
    is_synced = True

    def __init__(self):
        self.live = _Live()
        self.views: list = []

    def live_table(self):
        return self.live

    def ensure_synced_union_view(self, graph):
        self.views.append(graph)


class _Jobs:
    def __init__(self, names=("ontobricks-030-triple-rtm",), runs=()):
        self.names, self.runs = list(names), list(runs)
        self.started: list = []
        self.cancelled: list = []

    def list(self):
        return [SimpleNamespace(job_id=10 + i, settings=SimpleNamespace(name=n)) for i, n in enumerate(self.names)]

    def list_runs(self, job_id, active_only):
        assert active_only is True
        return self.runs

    def run_now(self, job_id, job_parameters):
        self.started.append((job_id, job_parameters))
        return SimpleNamespace(run_id=777)

    def cancel_run(self, run_id):
        self.cancelled.append(run_id)


class _Workspace:
    def __init__(self):
        self.uploads: dict = {}
        self.folders: list = []

    def mkdirs(self, folder):
        self.folders.append(folder)

    def upload(self, path, content, format=None, overwrite=False):
        self.uploads[path] = json.loads(content)


def _client(jobs=None):
    return SimpleNamespace(
        jobs=jobs or _Jobs(),
        workspace=_Workspace(),
        current_user=SimpleNamespace(me=lambda: SimpleNamespace(user_name="app-sp")),
    )


def _run(domain="sales", version="3", state="RUNNING", run_id=55):
    params = [SimpleNamespace(name="domain", value=domain), SimpleNamespace(name="version", value=version)]
    return SimpleNamespace(
        run_id=run_id, job_parameters=params, state=SimpleNamespace(life_cycle_state=state)
    )


@pytest.fixture
def mappings(monkeypatch):
    monkeypatch.setattr(
        TwinIncrementalBuild,
        "mappings_for",
        staticmethod(lambda d: ({"C": {"table": "main.sales.customers"}}, [], "http://ex.org/")),
    )


def _stream(client, **settings):
    return TwinRealtimeStream(_settings(**settings), client_factory=lambda: client)


class TestStart:
    def test_the_job_is_started_with_every_parameter_and_no_secret_value(self, mappings):
        client, store = _client(), _Store()

        started = _stream(client).start(_domain(), store=store)

        assert started == {"run_id": 777, "state": "PENDING", "reused": False}
        [(job_id, params)] = client.jobs.started
        assert job_id == 10
        assert params == {
            "domain": "sales",
            "version": "3",
            "source_kind": "kafka",
            "bootstrap_servers": "b:9092",
            "topic": "sales.customers",
            "secret_scope": "ontobricks",
            "secret_key": "kafka-jaas",
            "slots_table": "main.kg.triplestore_sales_V3_slots",
            "mappings_json_path": "/Workspace/Users/app-sp/.ontobricks/realtime/ontobricks-rtm-sales-v3.json",
            "lakebase_host": "pg.lakebase.io",
            "lakebase_db": "graphs",
            "lakebase_user": "ontobricks_rtm",
            "lakebase_secret": "ontobricks/pg-password",
            "live_table": "ontobricks_graph.sales_v3_live",
            "checkpoint": "/Volumes/main/ob/registry/realtime/ontobricks-rtm-sales-v3",
        }

    def test_the_live_table_and_the_mappings_are_ready_before_the_run(self, mappings):
        client, store = _client(), _Store()

        _stream(client).start(_domain(), store=store)

        assert store.live.ensured == ["sales_V3"] and store.views == ["sales_V3"]
        uploaded = client.workspace.uploads[
            "/Workspace/Users/app-sp/.ontobricks/realtime/ontobricks-rtm-sales-v3.json"
        ]
        assert uploaded["base_uri"] == "http://ex.org/" and "C" in uploaded["entity_mappings"]

    def test_a_running_stream_of_the_version_is_reused(self, mappings):
        client = _client(_Jobs(runs=[_run(version="2"), _run(run_id=56)]))

        started = _stream(client).start(_domain(), store=_Store())

        assert started == {"run_id": 56, "state": "RUNNING", "reused": True}
        assert client.jobs.started == []

    def test_a_dev_mode_job_name_is_found(self, mappings):
        client = _client(_Jobs(names=("other", "[dev someone] ontobricks-030-triple-rtm")))

        _stream(client).start(_domain(), store=_Store())

        assert client.jobs.started[0][0] == 11

    @pytest.mark.parametrize(
        ("domain", "settings", "match"),
        [
            (_domain(), {"realtime_enabled": False}, "disabled"),
            (_domain(status="DRAFT"), {}, "published"),
            (_domain(source={**_SOURCE, "topic": ""}), {}, "incomplete"),
            (_domain(), {"realtime_lakebase_secret": ""}, "ONTOBRICKS_REALTIME_LAKEBASE_SECRET"),
            (_domain(), {"realtime_checkpoint_root": " "}, "ONTOBRICKS_REALTIME_CHECKPOINT_ROOT"),
        ],
        ids=["disabled", "draft", "no-topic", "no-sink-secret", "no-checkpoint"],
    )
    def test_a_missing_prerequisite_is_refused_before_any_api_call(self, domain, settings, match):
        def no_client():
            raise AssertionError("no API call expected")

        with pytest.raises(ValidationError, match=match):
            TwinRealtimeStream(_settings(**settings), client_factory=no_client).start(domain, store=_Store())

    def test_a_store_without_live_serving_is_refused(self, mappings):
        with pytest.raises(ValidationError, match="Lakebase"):
            _stream(_client()).start(_domain(), store=SimpleNamespace())

    def test_an_undeployed_job_is_not_found(self, mappings):
        with pytest.raises(NotFoundError, match="triple-rtm"):
            _stream(_client(_Jobs(names=()))).start(_domain(), store=_Store())

    def test_an_sdk_failure_is_an_infrastructure_error(self, mappings):
        jobs = _Jobs()
        jobs.run_now = lambda **kw: (_ for _ in ()).throw(RuntimeError("PERMISSION_DENIED"))

        with pytest.raises(InfrastructureError):
            _stream(_client(jobs)).start(_domain(), store=_Store())


class TestStopAndStatus:
    def test_stop_cancels_the_version_s_run(self):
        client = _client(_Jobs(runs=[_run(run_id=56)]))

        assert _stream(client).stop(_domain()) == {"run_id": 56, "state": "CANCELLING"}
        assert client.jobs.cancelled == [56]

    def test_stop_without_a_run(self):
        assert _stream(_client()).stop(_domain()) == {"run_id": None, "state": "not_running"}

    def test_status_reads_the_active_run(self):
        client = _client(_Jobs(runs=[_run(state="PENDING", run_id=9)]))

        assert _stream(client).status(_domain()) == {"run_id": 9, "state": "PENDING"}

    def test_status_without_a_job_name(self):
        stream = TwinRealtimeStream(_settings(ontobricks_app_name=""), client_factory=_client)

        assert stream.status(_domain())["state"] == "not_configured"

    def test_status_with_an_unreachable_api_is_unknown(self):
        def broken():
            raise RuntimeError("no host")

        status = TwinRealtimeStream(_settings(), client_factory=broken).status(_domain())

        assert status["state"] == "unknown" and "error" in status


class _Fake:
    """A ``TwinRealtimeStream`` double for the build hook."""

    log: list = []

    def __init__(self, settings):
        self.settings = settings

    def start(self, domain, store=None):
        _Fake.log.append("start")
        return {"run_id": 777, "state": "PENDING", "reused": False}

    def stop(self, domain):
        _Fake.log.append("stop")
        return {"run_id": 55, "state": "CANCELLING"}


def _pipe(effective, *, enabled=True, runs=()):
    return SimpleNamespace(
        settings=SimpleNamespace(realtime_enabled=enabled),
        mode_decision=SimpleNamespace(effective=effective),
        domain=_domain(),
        store=_Store(),
        task_id="t-1",
        _recent_build_runs=list(runs),
        realtime_run_id="",
        realtime_state="",
    )


class TestAfterBuild:
    def setup_method(self):
        _Fake.log = []

    def test_a_realtime_build_starts_the_stream_and_records_it(self):
        pipe = _pipe("realtime")

        TwinRealtimeStream.after_build(pipe, factory=_Fake)

        assert _Fake.log == ["start"]
        assert (pipe.realtime_run_id, pipe.realtime_state) == ("777", "PENDING")

    def test_a_build_that_left_realtime_stops_the_previous_stream(self):
        runs = [{"stats": {"materialization": {"realtime_run_id": "55"}}}]
        pipe = _pipe("microbatch", runs=runs)

        TwinRealtimeStream.after_build(pipe, factory=_Fake)

        assert _Fake.log == ["stop"] and pipe.realtime_state == "CANCELLING"

    def test_a_build_that_never_streamed_calls_nothing(self):
        TwinRealtimeStream.after_build(_pipe("microbatch"), factory=_Fake)

        assert _Fake.log == []

    def test_a_deployment_without_realtime_calls_nothing(self):
        TwinRealtimeStream.after_build(_pipe("realtime", enabled=False), factory=_Fake)

        assert _Fake.log == []

    def test_a_stream_that_cannot_start_is_recorded_not_raised(self):
        class _Refusing(_Fake):
            def start(self, domain, store=None):
                raise ValidationError("Only a published domain version streams in realtime")

        pipe = _pipe("realtime")

        TwinRealtimeStream.after_build(pipe, factory=_Refusing)

        assert pipe.realtime_state == "failed:ValidationError"


def test_the_job_name_defaults_to_the_bundle_name():
    assert TwinRealtimeStream.job_name(_settings()) == "ontobricks-030-triple-rtm"
    assert TwinRealtimeStream.job_name(_settings(realtime_job_name="custom")) == "custom"
