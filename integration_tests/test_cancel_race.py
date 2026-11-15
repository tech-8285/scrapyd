"""Integration tests for cancellation racing with dequeue, process start and completion.

The real Scrapyd application runs inside the test reactor, is exercised over real HTTP on an
isolated UNIX domain socket and spawns real ``scrapy crawl`` subprocesses (the ``cancel_spider``
project in tests/projects). A file-based barrier in the spider lets the tests pause a crawl at
its start, so each boundary of the cancel/dequeue/spawn/finish lifecycle is reached
deterministically instead of by sleeping.

External state (cancel.json, status.json, listjobs.json, daemonstatus.json) is always crosschecked
against the real crawl behavior, namely the item records written under ``items_dir``.
"""

import json
import os
import shutil
import tempfile
import time
import uuid
import warnings
from contextlib import suppress
from pathlib import Path
from urllib.parse import urlencode

import pytest
from pytest_twisted import blockon
from setuptools import setup
from setuptools.warnings import SetuptoolsDeprecationWarning
from twisted.application.app import startApplication
from twisted.application.service import IServiceCollection
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, maybeDeferred, succeed
from twisted.internet.endpoints import UNIXClientEndpoint
from twisted.internet.task import deferLater
from twisted.web.client import Agent, readBody
from twisted.web.http_headers import Headers

from scrapyd.app import application
from scrapyd.config import Config
from scrapyd.interfaces import IPoller


def silence_upstream_warnings():
    # These tests build the Scrapyd application in-process, which surfaces deprecations from
    # twisted.web itself (static.py instantiates _UnsafeNoResource at import time; readBody over
    # a UNIX socket has no abortConnection). Registered at test time because pytest re-applies its
    # own filters (including the CI "-W error" policy) after module collection, which would
    # overwrite filters set at import time.
    warnings.filterwarnings("ignore", message=r".*_Unsafe(No|Forbidden)Resource.*", category=DeprecationWarning)
    warnings.filterwarnings(
        "ignore",
        message="Using readBody with a transport that does not have an abortConnection method",
    )


PROJECT = "cancel_spider"
SPIDER = "cancel_spider"
VERSION = "0.1"

# Config(values=...) starts from an empty parser, so the default [services] section shipped in
# scrapyd/default_scrapyd.conf has to be supplied explicitly for the HTTP API to be registered.
SERVICES = {
    "schedule.json": "scrapyd.webservice.Schedule",
    "cancel.json": "scrapyd.webservice.Cancel",
    "status.json": "scrapyd.webservice.Status",
    "addversion.json": "scrapyd.webservice.AddVersion",
    "listprojects.json": "scrapyd.webservice.ListProjects",
    "listversions.json": "scrapyd.webservice.ListVersions",
    "listspiders.json": "scrapyd.webservice.ListSpiders",
    "delproject.json": "scrapyd.webservice.DeleteProject",
    "delversion.json": "scrapyd.webservice.DeleteVersion",
    "listjobs.json": "scrapyd.webservice.ListJobs",
    "daemonstatus.json": "scrapyd.webservice.DaemonStatus",
}

BASEDIR = Path(__file__).absolute().parent.parent
EGG_SOURCE = BASEDIR / "tests" / "fixtures" / f"{PROJECT}.egg"
PROJECT_DIR = BASEDIR / "tests" / "projects" / PROJECT


def build_egg():
    """Build the cancel_spider egg, mirroring tests/conftest.py's pytest_configure loop."""
    if EGG_SOURCE.exists():
        return

    previous_cwd = Path.cwd()
    try:
        # setup() resolves build/ and dist/ relative to the current working directory.
        os.chdir(PROJECT_DIR.parent)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=SetuptoolsDeprecationWarning)
            setup(
                script_args=["bdist_egg"],
                name="project",
                packages=["mybot", "mybot.spiders"],
                package_dir={"mybot": PROJECT_DIR.name},
                entry_points={"scrapy": ["settings = mybot.settings"]},
                zip_safe=True,
            )
        egg = next((PROJECT_DIR.parent / "dist").glob("*.egg"))
        EGG_SOURCE.parent.mkdir(parents=True, exist_ok=True)
        egg.rename(EGG_SOURCE)
        setup(
            script_args=["clean", "--all", "--build-scripts=build"],
            packages=["mybot", "mybot.spiders"],
        )
    finally:
        os.chdir(previous_cwd)


class _UNIXEndpointFactory:
    """Route every HTTP request of the agent at one UNIX socket, ignoring the URL host."""

    def __init__(self, socket_path):
        self._socket_path = str(socket_path)

    def endpointForURI(self, uri):
        return UNIXClientEndpoint(reactor, self._socket_path)


class ScrapydClient:
    """JSON HTTP client bound to a Scrapyd application living in the test reactor."""

    def __init__(self, app, service, socket_path):
        self.app = app
        self.service = service
        self.agent = Agent.usingEndpointFactory(reactor, _UNIXEndpointFactory(socket_path))
        # The host is never dialed (transport is a UNIX socket); it only builds request URLs.
        self.base_url = "http://scrapyd.test"
        self._stopped = False

    @property
    def launcher(self):
        return self.service.getServiceNamed("launcher")

    @inlineCallbacks
    def request(self, method, path, params=None):
        url = self.base_url + path
        body_producer = None
        headers = None
        if method == "POST":
            body_producer = _BytesProducer(urlencode(params or {}).encode())
            headers = Headers({"Content-Type": ["application/x-www-form-urlencoded"]})
        elif params:
            url = f"{url}?{urlencode(params)}"

        response = yield self.agent.request(method.encode(), url.encode(), headers, body_producer)
        raw = yield readBody(response)
        return json.loads(raw)

    def is_running(self, jobid):
        return any(process.job == jobid for process in self.launcher.processes.values())

    def is_finished(self, jobid):
        return any(job.job == jobid for job in self.launcher.finished)

    @inlineCallbacks
    def stop(self):
        if self._stopped:
            return None
        self._stopped = True

        # stopService does not kill child processes, so terminate any crawl still running.
        for process in list(self.launcher.processes.values()):
            # The process may have exited concurrently.
            with suppress(Exception):
                process.transport.signalProcess("KILL")

        yield self.service.stopService()
        # Give the reactor a moment to reap the terminated subprocesses.
        yield deferLater(reactor, 0.2, lambda: None)


class _BytesProducer:
    def __init__(self, body):
        self._body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self._body)
        return succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


def build_config(workdir, *, max_proc, poll_interval, socket_path):
    config = Config(values={})
    config.cp["scrapyd"].update(
        {
            "eggs_dir": str(workdir / "eggs"),
            "dbs_dir": str(workdir / "dbs"),
            "logs_dir": str(workdir / "logs"),
            "items_dir": str(workdir / "items"),
            "jobs_dir": str(workdir / "jobs"),
            "jobstorage": "scrapyd.jobstorage.SqliteJobStorage",
            "poll_interval": str(poll_interval),
            "max_proc": str(max_proc),
            # http_port 0 makes app.py skip the TCP service; a UNIX socket instead gives a
            # listener on an isolated path. The path stays short for macOS' 104-char limit.
            "unix_socket_path": str(socket_path),
        }
    )
    # Root.__init__ reads service registrations from a dedicated [services] section.
    config.cp.add_section("services")
    config.cp["services"].update(SERVICES)
    return config


@pytest.fixture(autouse=True)
def _silence_upstream_warnings():
    silence_upstream_warnings()


@pytest.fixture
def workdir(tmp_path, monkeypatch):
    # The scrapyd.runner subprocess resolves eggs relative to its working directory.
    monkeypatch.chdir(tmp_path)
    build_egg()
    # FilesystemEggStorage sanitizes the version when building the filename (dots -> underscores).
    egg_dir = tmp_path / "eggs" / PROJECT
    egg_dir.mkdir(parents=True)
    shutil.copy(EGG_SOURCE, egg_dir / f"{VERSION.replace('.', '_')}.egg")
    # scrapyd.runner imports pkg_resources, whose deprecation warning would otherwise be raised
    # as an error under pytest's "-W error" filter and make every "scrapy list"/"scrapy crawl"
    # subprocess fail before doing any work.
    monkeypatch.setenv("PYTHONWARNINGS", "ignore::UserWarning")
    return tmp_path


@pytest.fixture
def make_server(workdir):
    clients = []

    def _make(max_proc=2, poll_interval=3600):
        socket_path = Path(tempfile.gettempdir()) / f"scrapyd-{uuid.uuid4().hex[:8]}.sock"
        config = build_config(
            workdir,
            max_proc=max_proc,
            poll_interval=poll_interval,
            socket_path=socket_path,
        )
        app = application(config)
        startApplication(app, save=False)
        client = ScrapydClient(app, IServiceCollection(app), socket_path)
        clients.append(client)
        return client

    yield _make

    for client in clients:
        blockon(client.stop())


# --------------------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------------------


@inlineCallbacks
def schedule(client, jobid, **spider_args):
    params = {"project": PROJECT, "spider": SPIDER, "jobid": jobid}
    params.update({key: str(value) for key, value in spider_args.items()})
    result = yield client.request("POST", "/schedule.json", params)
    assert result["status"] == "ok", result
    return result["jobid"]


@inlineCallbacks
def cancel(client, jobid):
    result = yield client.request("POST", "/cancel.json", {"project": PROJECT, "job": jobid})
    assert result["status"] == "ok", result
    return result["prevstate"]


@inlineCallbacks
def get_status(client, jobid):
    result = yield client.request("GET", "/status.json", {"project": PROJECT, "job": jobid})
    return result["currstate"]


@inlineCallbacks
def list_jobs(client):
    result = yield client.request("GET", "/listjobs.json", {"project": PROJECT})
    return result


def find_job(entries, jobid):
    return next((entry for entry in entries if entry["id"] == jobid), None)


def item_file(workdir, jobid):
    return workdir / "items" / PROJECT / SPIDER / f"{jobid}.jl"


def read_items(workdir, jobid):
    path = item_file(workdir, jobid)
    if not path.exists():
        return None
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def assert_no_items(workdir, jobid):
    # An interrupted crawl may leave an empty feed file; either way it must not contain records.
    items = read_items(workdir, jobid)
    assert not items


def marker_dir(workdir, jobid):
    path = workdir / "markers" / jobid
    path.mkdir(parents=True, exist_ok=True)
    return path


@inlineCallbacks
def wait_for_path(path, timeout=30):
    deadline = time.monotonic() + timeout
    while not Path(path).exists():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {path}")
        yield deferLater(reactor, 0.05, lambda: None)


@inlineCallbacks
def wait_until(client, check, timeout=30):
    poller = client.app.getComponent(IPoller)
    deadline = time.monotonic() + timeout
    while not check():
        if time.monotonic() > deadline:
            raise AssertionError("condition was not met before the timeout")
        yield poller.poll()
        yield deferLater(reactor, 0.05, lambda: None)


@inlineCallbacks
def wait_running(client, jobid):
    yield wait_until(client, lambda: client.is_running(jobid))


@inlineCallbacks
def wait_finished(client, jobid):
    yield wait_until(client, lambda: client.is_finished(jobid), timeout=60)


@inlineCallbacks
def assert_job_in_single_state(client, jobid, expected_state):
    """The job must appear in exactly one (or no) list, matching status.json."""
    status = yield get_status(client, jobid)
    jobs = yield list_jobs(client)

    presence = {
        "pending": find_job(jobs["pending"], jobid) is not None,
        "running": find_job(jobs["running"], jobid) is not None,
        "finished": find_job(jobs["finished"], jobid) is not None,
    }

    assert status == expected_state
    assert presence == {state: state == expected_state for state in presence}


@inlineCallbacks
def assert_daemon_totals(client, *, pending, running, finished):
    result = yield client.request("GET", "/daemonstatus.json")
    assert (result["pending"], result["running"], result["finished"]) == (pending, running, finished)


def barrier_args(workdir, jobid, *, wait=True):
    markers = marker_dir(workdir, jobid)
    args = {
        "ready_file": str(markers / "ready"),
        "item_id": jobid,
    }
    if wait:
        args["go_file"] = str(markers / "go")
    return args, markers


# --------------------------------------------------------------------------------------------------
# Part 1: boundary timing replay
# --------------------------------------------------------------------------------------------------


@inlineCallbacks
def test_cancel_before_dequeue(workdir, make_server):
    client = make_server(max_proc=1)
    jobid = "cancel-before-dequeue"

    yield schedule(client, jobid, **barrier_args(workdir, jobid)[0])

    # The poller never ran: the job is still only in the SQLite pending queue.
    prevstate = yield cancel(client, jobid)
    assert prevstate == "pending"

    # A later poll must not resurrect the canceled job.
    yield client.app.getComponent(IPoller).poll()
    yield deferLater(reactor, 0.5, lambda: None)

    yield assert_job_in_single_state(client, jobid, None)
    assert read_items(workdir, jobid) is None
    yield assert_daemon_totals(client, pending=0, running=0, finished=0)


@inlineCallbacks
def test_cancel_after_dequeue_before_spawn(workdir, make_server):
    # One slot: the blocker occupies it, leaving no consumer waiting on the poller's DeferredQueue.
    client = make_server(max_proc=1)

    blocker = "dequeue-blocker"
    blocker_args, blocker_markers = barrier_args(workdir, blocker)
    yield schedule(client, blocker, **blocker_args)
    yield wait_running(client, blocker)
    yield wait_for_path(blocker_markers / "ready")

    victim = "dequeue-victim"
    queue = client.app.getComponent(IPoller).queues[PROJECT]
    # wait=False: once the message spawns, the crawl runs to completion without another release.
    yield schedule(client, victim, **barrier_args(workdir, victim, wait=False)[0])

    # Reconstruct the exact race window that QueuePoller.poll() passes through: the message has
    # been popped from the SQLite queue and put on the in-memory DeferredQueue, but no launcher
    # slot is available, so no process has been spawned and the process table is still empty.
    raw_message = yield maybeDeferred(queue.pop)
    assert raw_message["_job"] == victim
    message = raw_message.copy()
    message["_project"] = PROJECT
    message["_spider"] = message.pop("name")
    client.app.getComponent(IPoller).dq.put(message)

    # Current behavior, pinned deliberately: in this window the job is in neither the SQLite
    # queue nor the process table, so cancel reports no previous state.
    prevstate = yield cancel(client, victim)
    assert prevstate is None
    yield assert_job_in_single_state(client, victim, None)

    # Release the blocker. Its completion refills the slot, and the waiting message spawns.
    (blocker_markers / "go").touch()
    yield wait_finished(client, blocker)
    yield wait_finished(client, victim)

    # External state must agree with the real crawl: despite the "successful" cancel call, the
    # victim did run and its items are on disk.
    yield assert_job_in_single_state(client, victim, "finished")
    victim_items = read_items(workdir, victim)
    assert victim_items is not None
    assert {item["item_id"] for item in victim_items} == {victim}

    blocker_items = read_items(workdir, blocker)
    assert blocker_items is not None
    assert {item["item_id"] for item in blocker_items} == {blocker}
    yield assert_daemon_totals(client, pending=0, running=0, finished=2)


@inlineCallbacks
def test_cancel_while_running(workdir, make_server):
    client = make_server(max_proc=2)

    victim = "running-victim"
    victim_args, victim_markers = barrier_args(workdir, victim)
    yield schedule(client, victim, **victim_args)
    yield wait_running(client, victim)
    yield wait_for_path(victim_markers / "ready")

    # Another job must keep being dispatched while the victim is canceled.
    other = "running-other"
    other_args, other_markers = barrier_args(workdir, other)
    yield schedule(client, other, **other_args)
    yield wait_running(client, other)

    prevstate = yield cancel(client, victim)
    assert prevstate == "running"

    yield wait_finished(client, victim)
    yield assert_job_in_single_state(client, victim, "finished")
    # The crawl was interrupted before its barrier was released: no item records may exist.
    assert_no_items(workdir, victim)

    # A duplicate cancel after termination changes nothing and reports no previous state.
    assert (yield cancel(client, victim)) is None
    yield assert_job_in_single_state(client, victim, "finished")

    # The other job completes normally with its own items.
    (other_markers / "go").touch()
    yield wait_finished(client, other)
    yield assert_job_in_single_state(client, other, "finished")
    other_items = read_items(workdir, other)
    assert {item["item_id"] for item in other_items} == {other}

    yield assert_daemon_totals(client, pending=0, running=0, finished=2)


@inlineCallbacks
def test_cancel_at_exact_completion(workdir, make_server):
    client = make_server(max_proc=1)
    jobid = "finish-race"
    args, markers = barrier_args(workdir, jobid)
    yield schedule(client, jobid, **args)
    yield wait_running(client, jobid)
    yield wait_for_path(markers / "ready")

    # Release the crawl and wait for the completion callback to register the finished job.
    (markers / "go").touch()
    yield wait_finished(client, jobid)

    prevstate = yield cancel(client, jobid)
    assert prevstate is None

    yield assert_job_in_single_state(client, jobid, "finished")
    items = read_items(workdir, jobid)
    assert items is not None
    assert len(items) == 2
    assert {item["item_id"] for item in items} == {jobid}
    yield assert_daemon_totals(client, pending=0, running=0, finished=1)


# --------------------------------------------------------------------------------------------------
# Part 2: duplicate cancellation, service restart and non-interference
# --------------------------------------------------------------------------------------------------


@inlineCallbacks
def test_duplicate_pending_cancel_survives_restart(workdir, make_server):
    client = make_server(max_proc=1)
    jobid = "restart-pending"
    yield schedule(client, jobid, **barrier_args(workdir, jobid)[0])

    assert (yield cancel(client, jobid)) == "pending"
    assert (yield cancel(client, jobid)) is None
    yield assert_job_in_single_state(client, jobid, None)

    yield client.stop()

    # Restart against the same SQLite databases, eggs and items directory.
    restarted = make_server(max_proc=1)
    yield restarted.app.getComponent(IPoller).poll()
    yield deferLater(reactor, 1.0, lambda: None)

    # The canceled job must not come back, re-crawl or gain a contradictory terminal state.
    yield assert_job_in_single_state(restarted, jobid, None)
    assert read_items(workdir, jobid) is None
    yield assert_daemon_totals(restarted, pending=0, running=0, finished=0)

    # Reusing the jobid starts from a clean slate: it is pending again and can be canceled once.
    yield schedule(restarted, jobid, **barrier_args(workdir, f"{jobid}-second")[0])
    assert (yield cancel(restarted, jobid)) == "pending"
    assert (yield cancel(restarted, jobid)) is None

    # Unrelated jobs still dispatch normally after the restart.
    other = "restart-pending-other"
    yield schedule(restarted, other, **barrier_args(workdir, other, wait=False)[0])
    yield wait_finished(restarted, other)
    yield assert_job_in_single_state(restarted, other, "finished")
    assert read_items(workdir, other) is not None


@inlineCallbacks
def test_running_cancel_survives_restart(workdir, make_server):
    client = make_server(max_proc=1)
    jobid = "restart-running"
    args, markers = barrier_args(workdir, jobid)
    yield schedule(client, jobid, **args)
    yield wait_running(client, jobid)
    yield wait_for_path(markers / "ready")

    assert (yield cancel(client, jobid)) == "running"
    yield wait_finished(client, jobid)
    assert (yield cancel(client, jobid)) is None
    assert_no_items(workdir, jobid)
    yield client.stop()

    restarted = make_server(max_proc=1)
    # Polling must not re-enqueue or re-run anything.
    yield restarted.app.getComponent(IPoller).poll()
    yield deferLater(reactor, 1.0, lambda: None)

    yield assert_job_in_single_state(restarted, jobid, "finished")
    assert_no_items(workdir, jobid)
    yield assert_daemon_totals(restarted, pending=0, running=0, finished=1)

    jobs = yield list_jobs(restarted)
    assert len([entry for entry in jobs["finished"] if entry["id"] == jobid]) == 1

    # A fresh, unrelated job dispatches and writes items, proving the launcher still works.
    other = "restart-running-other"
    yield schedule(restarted, other, **barrier_args(workdir, other, wait=False)[0])
    yield wait_finished(restarted, other)
    other_items = read_items(workdir, other)
    assert other_items is not None
    assert {item["item_id"] for item in other_items} == {other}
    yield assert_daemon_totals(restarted, pending=0, running=0, finished=2)
