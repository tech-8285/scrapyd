import datetime
import re

import pytest
from twisted.internet import defer, error, reactor
from twisted.logger import LogLevel, capturedLogs
from twisted.python import failure

from scrapyd import __version__
from scrapyd.config import Config
from scrapyd.interfaces import IPoller
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
        # The "_settle" recovery handle is not a spider argument.
        ({"_project": "p1", "_spider": "s1", "_settle": lambda: None}, ["s1"]),
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


def test_spawn_process_settles_message(launcher):
    settled = []
    launcher._spawn_process(  # noqa: SLF001
        {"_project": "p1", "_spider": "s1", "_job": "j1", "_settle": lambda: settled.append(True)}, 0
    )

    assert launcher.processes[0].job == "j1"
    assert settled == [True]


def test_spawn_process_failure(launcher, monkeypatch):
    def spawn_process(*args, **kwargs):
        raise OSError("No such file or directory")

    monkeypatch.setattr(reactor, "spawnProcess", spawn_process)

    settled = []
    message = {"_project": "p1", "_spider": "s1", "_job": "j1", "_settle": lambda: settled.append(True)}

    with capturedLogs() as captured:
        launcher._spawn_process(message, 0)  # noqa: SLF001
    captured = remove_debug_messages(captured)

    # The failure is logged.
    assert len(captured) == 1
    assert captured[0]["log_level"] == LogLevel.error
    assert re.search(
        r"\[Launcher\] Process failed to start: No such file or directory "
        r"project='p1' spider='s1' job='j1'",
        get_message(captured),
    )

    # The job is not lost: it's in an explicit terminal state, and the queue's recovery record is settled.
    assert 0 not in launcher.processes
    assert settled == [True]
    assert len(launcher.finished) == 1
    finished = next(iter(launcher.finished))
    assert (finished.project, finished.spider, finished.job) == ("p1", "s1", "j1")
    assert isinstance(finished.start_time, datetime.datetime)
    assert isinstance(finished.end_time, datetime.datetime)

    # The slot is released: the launcher is waiting for another message for it.
    poller = launcher.app.getComponent(IPoller)
    assert len(poller.dq.waiting) == 1


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
