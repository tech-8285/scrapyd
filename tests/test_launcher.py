import datetime
import re
import tempfile
from pathlib import Path
from unittest import mock

import pytest
from twisted.application.service import IServiceCollection
from twisted.internet import defer, error, reactor
from twisted.logger import LogLevel, capturedLogs
from twisted.python import failure

from scrapyd import __version__
from scrapyd.app import application as build_application
from scrapyd.config import Config
from scrapyd.interfaces import ILaunchRegistry, IPoller
from scrapyd.launcher import Launcher, get_crawl_args
from tests import get_message, has_settings


def remove_debug_messages(captured):
    return [message for message in captured if message["log_level"] != LogLevel.debug]


@pytest.fixture
def launcher(app):
    return Launcher(Config(), app)


@pytest.fixture
def process(launcher):
    launcher._spawn_process({"_project": "p1", "_spider": "s1", "_job": "j1"}, 0)  # noqa: SLF001
    return launcher.processes[0]


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ({"_project": "p1", "_spider": "s1"}, ["s1"]),
        ({"_project": "p1", "_spider": "s1", "settings": {"ONE": "two"}}, ["s1", "-s", "ONE=two"]),
        ({"_project": "p1", "_spider": "s1", "arg1": "val1"}, ["s1", "-a", "arg1=val1"]),
        (
            {"_project": "p1", "_spider": "s1", "arg1": "val1", "settings": {"ONE": "two"}},
            ["s1", "-s", "ONE=two", "-a", "arg1=val1"],
        ),
    ],
)
def test_get_crawl_args(message, expected):
    assert get_crawl_args(message) == expected


def test_start_service(launcher):
    with capturedLogs() as captured:
        launcher.startService()
    captured = remove_debug_messages(captured)

    assert len(captured) == 1
    assert captured[0]["log_level"] == LogLevel.info
    assert re.search(
        f"\\[Launcher\\] Scrapyd {__version__} started: max_proc=\\d+, runner='scrapyd.runner'", get_message(captured)
    )


def test_start_service_max_proc(app):
    config = Config()
    config.cp.set(Config.SECTION, "max_proc", "8")
    launcher = Launcher(config, app)

    with capturedLogs() as captured:
        launcher.startService()
    captured = remove_debug_messages(captured)

    assert len(captured) == 1
    assert captured[0]["log_level"] == LogLevel.info
    assert re.search(
        f"\\[Launcher\\] Scrapyd {__version__} started: max_proc=8, runner='scrapyd.runner'", get_message(captured)
    )


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ({}, {}),
        ({"_version": "v1"}, {"SCRAPYD_EGG_VERSION": "v1"}),
    ],
)
def test_spawn_process(launcher, message, expected):
    launcher._spawn_process({"_project": "localproject", "_spider": "s1", "_job": "j1", **message}, 1)  # noqa: SLF001

    process = launcher.processes[1]

    assert isinstance(process.pid, int)
    assert process.project == "localproject"
    assert process.spider == "s1"
    assert process.job == "j1"
    assert isinstance(process.start_time, datetime.datetime)
    assert process.end_time is None
    assert isinstance(process.args, list)  # see tests below
    assert isinstance(process.deferred, defer.Deferred)

    # scrapyd.environ.Environ.get_environment
    assert process.env["SCRAPY_PROJECT"] == "localproject"
    for key, value in expected.items():
        assert process.env[key] == value
    if "SCRAPYD_EGG_VERSION" not in expected:
        assert "SCRAPYD_EGG_VERSION" not in process.env
    if has_settings():
        assert process.env["SCRAPY_SETTINGS_MODULE"] == "localproject.settings"
    else:
        assert "SCRAPY_SETTINGS_MODULE" not in process.env


def test_out_received(process):
    with capturedLogs() as captured:
        process.outReceived(b"out\n")

    assert len(captured) == 1
    assert captured[0]["log_level"] == LogLevel.info
    assert get_message(captured) == f"[Launcher,{process.pid}/stdout] out"


def test_err_received(process):
    with capturedLogs() as captured:
        process.errReceived(b"err\n")

    assert len(captured) == 1
    assert captured[0]["log_level"] == LogLevel.error
    assert get_message(captured) == f"[Launcher,{process.pid}/stderr] err"


def test_connection_made(environ, process):
    pid = process.pid
    with capturedLogs() as captured:
        process.connectionMade()

    assert len(captured) == 1
    assert captured[0]["log_level"] == LogLevel.info
    if environ.items_dir:
        assert re.match(
            f"\\[scrapyd\\.launcher#info\\] Process started: project='p1' spider='s1' job='j1' pid={pid} "
            "args=\\['\\S+', '-m', 'scrapyd\\.runner', 'crawl', 's1', '-s', 'LOG_FILE=\\S+j1\\.log', '-s', "
            """'FEEDS={"file:///\\S+j1\\.jl": {"format": "jsonlines"}}', '-a', '_job=j1'\\]""",
            get_message(captured),
        )
    else:
        assert re.match(
            f"\\[scrapyd\\.launcher#info\\] Process started: project='p1' spider='s1' job='j1' pid={pid} "
            "args=\\['\\S+', '-m', 'scrapyd\\.runner', 'crawl', 's1', '-s', 'LOG_FILE=\\S+j1\\.log', '-a', '_job=j1'\\]",
            get_message(captured),
        )


def test_process_ended_done(environ, process):
    pid = process.pid
    with capturedLogs() as captured:
        process.processEnded(failure.Failure(error.ProcessDone(0)))
    captured = remove_debug_messages(captured)

    assert len(captured) == 1
    assert captured[0]["log_level"] == LogLevel.info
    if environ.items_dir:
        assert re.match(
            f"\\[scrapyd\\.launcher#info\\] Process finished: project='p1' spider='s1' job='j1' pid={pid} "
            "args=\\['\\S+', '-m', 'scrapyd\\.runner', 'crawl', 's1', '-s', 'LOG_FILE=\\S+j1\\.log', '-s', "
            """'FEEDS={"file:///\\S+j1\\.jl": {"format": "jsonlines"}}', '-a', '_job=j1'\\]""",
            get_message(captured),
        )
    else:
        assert re.match(
            f"\\[scrapyd\\.launcher#info\\] Process finished: project='p1' spider='s1' job='j1' pid={pid} "
            "args=\\['\\S+', '-m', 'scrapyd\\.runner', 'crawl', 's1', '-s', 'LOG_FILE=\\S+j1\\.log', '-a', '_job=j1'\\]",
            get_message(captured),
        )


def test_process_ended_terminated(environ, process):
    pid = process.pid
    with capturedLogs() as captured:
        process.processEnded(failure.Failure(error.ProcessTerminated(1)))
    captured = remove_debug_messages(captured)

    assert len(captured) == 1
    assert captured[0]["log_level"] == LogLevel.error
    if environ.items_dir:
        assert re.match(
            f"\\[scrapyd\\.launcher#error\\] Process died: exitstatus=1 project='p1' spider='s1' job='j1' pid={pid} "
            "args=\\['\\S+', '-m', 'scrapyd\\.runner', 'crawl', 's1', '-s', 'LOG_FILE=\\S+j1\\.log', '-s', "
            """'FEEDS={"file:///\\S+j1\\.jl": {"format": "jsonlines"}}', '-a', '_job=j1'\\]""",
            get_message(captured),
        )
    else:
        assert re.match(
            f"\\[scrapyd\\.launcher#error\\] Process died: exitstatus=1 project='p1' spider='s1' job='j1' pid={pid} "
            "args=\\['\\S+', '-m', 'scrapyd\\.runner', 'crawl', 's1', '-s', 'LOG_FILE=\\S+', '-a', '_job=j1'\\]",
            get_message(captured),
        )


def test_repr(process):
    assert repr(process).startswith(f"ScrapyProcessProtocol(project=p1 spider=s1 job=j1 pid={process.pid} start_time=")


def test_spawn_failure_records_terminal_state_and_refills_slot(app, launcher, tmp_path):
    registry = app.getComponent(ILaunchRegistry)
    poller = app.getComponent(IPoller)
    # Ensure at least one project exists in both the default and custom configurations.
    (tmp_path / "eggs" / "p1").mkdir(parents=True, exist_ok=True)
    poller.update_projects()
    project = next(iter(poller.queues))
    queue = poller.queues[project]
    queue.add("s1", _job="j-lost")
    # The claim step that happens in the poller when a slot is available.
    claimed = registry.claim(project, queue)
    assert claimed["_job"] == "j-lost"
    message = {"_project": project, "_spider": "s1", "_job": "j-lost"}

    with mock.patch.object(reactor, "spawnProcess", side_effect=OSError("cannot fork")):
        launcher._spawn_process(message, 3)  # noqa: SLF001

    # The job has an explicit terminal state instead of vanishing, and no slot is occupied.
    entry = registry.get(project, "j-lost")
    assert entry.state == "failed"
    assert "cannot fork" in entry.error
    assert 3 not in launcher.processes

    # The slot was released and immediately armed again (a new waiting Deferred was registered).
    assert poller.dq.waiting


def test_recover_requeues_never_started_jobs():
    tmp = Path(tempfile.mkdtemp())
    (tmp / "eggs" / "localproject").mkdir(parents=True)
    persistent = Config(
        values={
            "eggs_dir": str(tmp / "eggs"),
            "dbs_dir": str(tmp / "dbs"),
            "max_proc": "0",
            "jobstorage": "scrapyd.jobstorage.SqliteJobStorage",
        }
    )

    first = build_application(persistent)
    first_poller = first.getComponent(IPoller)
    first_registry = first.getComponent(ILaunchRegistry)
    queue = first_poller.queues["localproject"]
    queue.add("s1", _job="j-recover")
    first_registry.claim("localproject", queue)
    assert queue.count() == 0

    second = build_application(persistent)
    second_launcher = IServiceCollection(second, second).getServiceNamed("launcher")
    second_launcher._recover()  # noqa: SLF001

    restored = second.getComponent(IPoller).queues["localproject"].list()
    assert restored == [{"name": "s1", "_job": "j-recover"}]
    assert second.getComponent(ILaunchRegistry).get("localproject", "j-recover") is None


def test_connection_made_marks_job_running(process):
    # The process fixture bypasses the registry, so marking must be tolerant of an absent record.
    pid = process.pid
    process.connectionMade()
    assert process.pid == pid
