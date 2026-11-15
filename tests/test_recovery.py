"""
Recovery contract for persistent spider queues.

A job that is dequeued from a persistent queue must, after a service restart or a launch failure,
correspond to a pending, running or explicitly terminal state: it must neither silently disappear nor
be scraped twice.

These tests use the local "example" spider (tests/fixtures/filesystem/localproject) and a temporary,
persistent (SQLite-backed) spider queue, and check that the queried state matches whether the crawl
process actually started.
"""

import shutil
import time
from pathlib import Path

import pytest
from twisted.internet import reactor

from scrapyd.app import application
from scrapyd.config import Config
from scrapyd.interfaces import IPoller, ISpiderScheduler
from scrapyd.launcher import Launcher

BASEDIR = Path(__file__).parent.resolve()


@pytest.fixture
def config(chdir):
    shutil.copytree(BASEDIR / "fixtures" / "filesystem", chdir, dirs_exist_ok=True)
    config = Config()
    config.cp.set(Config.SECTION, "items_dir", "items")
    config.cp.set(Config.SECTION, "max_proc", "1")
    return config


@pytest.fixture
def app(config):
    return application(config)


def wait_for_file(path, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.1)
    raise TimeoutError(f"{path} was not created")


def test_unstarted_job_is_pending_after_restart(config, app, chdir):
    scheduler = app.getComponent(ISpiderScheduler)
    poller = app.getComponent(IPoller)

    scheduler.schedule("localproject", "example", _job="j1")
    queue = poller.queues["localproject"]
    assert queue.count() == 1

    # The job is dequeued, but the service stops before the crawl process starts: the message is
    # never settled.
    message = queue.pop()
    assert message["_job"] == "j1"
    assert queue.count() == 0

    # After a restart, the unstarted job is pending again: it was not silently lost.
    restarted_app = application(config)
    restarted_poller = restarted_app.getComponent(IPoller)
    restarted_queue = restarted_poller.queues["localproject"]
    assert restarted_queue.count() == 1
    assert restarted_queue.list()[0]["_job"] == "j1"

    # The recovered job actually starts.
    launcher = Launcher(config, restarted_app)
    launcher._get_message(0)  # noqa: SLF001
    restarted_poller.poll()

    process = launcher.processes[0]
    assert process.job == "j1"
    assert isinstance(process.pid, int)

    # The job was settled when the process started: it is no longer pending, and a further restart
    # does not re-scrape it.
    assert restarted_queue.count() == 0
    assert application(config).getComponent(IPoller).queues["localproject"].count() == 0

    # The crawl records are attributed to the job that actually started.
    wait_for_file(chdir / "logs" / "localproject" / "example" / "j1.log")
    wait_for_file(chdir / "items" / "localproject" / "example" / "j1.jl")


def test_failed_launch_is_terminal_and_slot_is_released(config, app, chdir, monkeypatch):
    scheduler = app.getComponent(ISpiderScheduler)
    poller = app.getComponent(IPoller)
    launcher = Launcher(config, app)

    scheduler.schedule("localproject", "example", _job="j1")

    real_spawn_process = reactor.spawnProcess

    def spawn_process(*args, **kwargs):
        raise OSError("No such file or directory")

    monkeypatch.setattr(reactor, "spawnProcess", spawn_process)

    launcher._get_message(0)  # noqa: SLF001
    poller.poll()

    queue = poller.queues["localproject"]

    # The process never started: the job is in an explicit terminal state, not pending, not running.
    assert 0 not in launcher.processes
    assert queue.count() == 0
    finished = list(launcher.finished)
    assert len(finished) == 1
    assert (finished[0].project, finished[0].spider, finished[0].job) == ("localproject", "example", "j1")
    assert finished[0].end_time is not None

    # No crawl records exist for a job that never started.
    assert not (chdir / "logs" / "localproject" / "example" / "j1.log").exists()

    # The slot was released.
    assert len(poller.dq.waiting) == 1

    # A same-named job submitted afterwards is picked up by the released slot and really starts.
    # (Restore spawnProcess directly: monkeypatch.undo() would also revert the chdir fixture.)
    monkeypatch.setattr(reactor, "spawnProcess", real_spawn_process)
    scheduler.schedule("localproject", "example", _job="j1")
    poller.poll()

    process = launcher.processes[0]
    assert process.job == "j1"
    assert isinstance(process.pid, int)
    assert queue.count() == 0

    # The crawl records are attributed to the run that actually started.
    wait_for_file(chdir / "logs" / "localproject" / "example" / "j1.log")

    # The failed launch remains a single terminal record: the resubmitted job is still running.
    assert len(launcher.finished) == 1
