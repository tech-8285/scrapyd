"""
Replay a local crawl schedule, and compare the crawl records with the query states and with the dispatch order.

Every scenario starts an isolated Scrapyd instance in a temporary directory with its own port, database, log and
items directory, so that these tests don't need the shared instance that the other integration tests use.
No container, network target or cloud credential is used: the fixture spider fetches a ``data:`` URI.

Dispatch order is observed in two ways:

- ``listjobs.json``/``status.json`` describe the pending and running jobs before they start.
- The fixture spider writes one marker file when it starts and one record when it finishes, so the order of the
  crawl records is the order in which the crawls actually ran.

To accept a batch of jobs before any of them runs, the instance is started with a very long ``poll_interval``: jobs
are then dispatched by the first poll of a restarted instance, which is the restart path that this change is about.
The crawl itself is then held at a file barrier until the test releases it.
"""

import json
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
import requests

FIXTURES = Path(__file__).absolute().parent.parent / "tests" / "fixtures"
EGG = (FIXTURES / "ordered.egg").read_bytes()

AUTH = ("hello12345", "67890world")
SPIDER = "trace"
PROJECT_A = "ordereda"
PROJECT_B = "orderedb"

# Long enough that a job is never dispatched by the timer, only by the first poll after a restart.
NO_TIMER = 3600


def ephemeral_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class Server:
    """A Scrapyd instance that can be stopped and started again in the same working directory."""

    def __init__(self, working_dir, *, poll_interval=0.2, max_proc=1):
        self.working_dir = working_dir
        self.poll_interval = poll_interval
        self.max_proc = max_proc
        self.barrier = working_dir / "barrier"
        self.port = None
        self.process = None

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}"

    def start(self):
        self.port = ephemeral_port()
        (self.working_dir / "scrapyd.conf").write_text(
            "[scrapyd]\n"
            f"username = {AUTH[0]}\n"
            f"password = {AUTH[1]}\n"
            "bind_address = 127.0.0.1\n"
            f"http_port = {self.port}\n"
            f"max_proc = {self.max_proc}\n"
            f"poll_interval = {self.poll_interval}\n"
            "items_dir = items\n"
            "logs_dir = logs\n"
            "dbs_dir = dbs\n"
            "finished_to_keep = 1000\n"
            "jobstorage = scrapyd.jobstorage.SqliteJobStorage\n"
        )
        for directory in ("logs", "barrier"):
            (self.working_dir / directory).mkdir(exist_ok=True)

        self.log_name = f"server-{len(list(self.working_dir.glob('server-*.log')))}.log"
        self.output_file = (self.working_dir / self.log_name).open("wb")
        self.process = subprocess.Popen(
            [sys.executable, "-m", "scrapyd"],
            cwd=self.working_dir,
            stdout=self.output_file,
            stderr=subprocess.STDOUT,
        )

        deadline = time.time() + 30
        while time.time() < deadline:
            if self.process.poll() is not None:
                message = self.output()
                self.stop()
                raise RuntimeError(f"Scrapyd exited:\n{message}")
            try:
                if requests.get(f"{self.url}/daemonstatus.json", auth=AUTH, timeout=1).status_code == 200:
                    return self
            except requests.ConnectionError:
                time.sleep(0.1)
        message = self.output()
        self.stop()
        raise RuntimeError(f"Scrapyd did not start:\n{message}")

    def stop(self):
        if self.process is not None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process = None
        if self.output_file is not None:
            self.output_file.close()
            self.output_file = None

    def output(self):
        return b"".join(path.read_bytes() for path in sorted(self.working_dir.glob("server-*.log"))).decode(
            errors="replace"
        )

    def release(self):
        (self.barrier / "release").touch()

    def started(self):
        """Return the job IDs that started to crawl, in the order in which they started."""
        return [path.stem.removeprefix("started-") for path in sorted(self.barrier.glob("started-*"), key=_mtime)]

    def wait_for_started(self, count, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if len(self.started()) >= count:
                return
            time.sleep(0.05)
        raise TimeoutError(f"only {len(self.started())} of {count} crawls started within {timeout}s")

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc_info):
        self.stop()


def _mtime(path):
    return path.stat().st_mtime_ns


@pytest.fixture
def server(tmp_path):
    with Server(tmp_path) as running:
        yield running


@pytest.fixture
def accepting_server(tmp_path):
    """An instance that accepts jobs without dispatching them: its first poll finds an empty queue."""
    with Server(tmp_path, poll_interval=NO_TIMER) as running:
        yield running


def req(server, method, path, **kwargs):
    response = getattr(requests, method)(f"{server.url}{path}", auth=AUTH, timeout=10, **kwargs)
    assert response.status_code == 200, f"200 != {response.status_code}"
    return response.json()


def add_version(server, project):
    data = req(
        server,
        "post",
        "/addversion.json",
        data={"project": project, "version": "1.0"},
        files={"egg": ("ordered.egg", EGG, "application/octet-stream")},
    )
    assert data["status"] == "ok"
    assert data["spiders"] == 1


def schedule(server, project, jobid, *, priority=0, payload="payload", barrier=False, wait=10):
    data = {
        "project": project,
        "spider": SPIDER,
        "jobid": jobid,
        "priority": priority,
        "payload": payload,
    }
    if barrier:
        data["barrier"] = str(server.barrier)
        data["wait"] = wait
    return req(server, "post", "/schedule.json", data=data)


def pending(server):
    """Return the (project, job ID) pairs of the pending jobs, in the order in which they will be dispatched."""
    return [(job["project"], job["id"]) for job in req(server, "get", "/listjobs.json")["pending"]]


def state(server, project, jobid):
    return req(server, "get", "/status.json", params={"project": project, "job": jobid})["currstate"]


def wait_for_state(server, project, jobid, expected, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        current = state(server, project, jobid)
        if current == expected:
            return
        assert current is not None, f"job {jobid!r} of project {project!r} is unknown to Scrapyd"
        time.sleep(0.05)
    raise TimeoutError(f"job {jobid!r} of project {project!r} did not reach {expected!r} within {timeout}s")


def finished_jobs(server):
    """Return the (job ID, end time) pairs of the finished jobs, oldest first."""
    jobs = req(server, "get", "/listjobs.json")["finished"]
    return sorted(((job["id"], job["end_time"]) for job in jobs), key=lambda pair: pair[1])


def wait_for_finished_jobs(server, count, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if len(finished_jobs(server)) >= count:
            return finished_jobs(server)
        time.sleep(0.05)
    raise TimeoutError(f"only {len(finished_jobs(server))} of {count} jobs finished within {timeout}s")


def records(server, project):
    """
    Return the ``(job ID, payload)`` pairs of the crawl records of a project.

    The spider writes one record per crawl, so the records describe which crawls actually ran.
    """
    directory = server.working_dir / "items" / project / SPIDER
    if not directory.exists():
        return []
    pairs = []
    # The file names are job IDs, not the order in which the crawls ran, so order the files by the time at which the
    # crawl finished. Fall back to the modification time if the file has no record (for example, a crawl with no items).
    for job_file in sorted(directory.glob("*.jl"), key=lambda path: (_end_time(server, path.stem), _mtime(path))):
        for line in job_file.read_text().splitlines():
            item = json.loads(line)
            pairs.append((job_file.stem, item["payload"]))
    return pairs


def _end_time(server, jobid):
    for job in req(server, "get", "/listjobs.json")["finished"]:
        if job["id"] == jobid:
            return job["end_time"]
    return ""


def test_equal_priority_order_across_projects(tmp_path):
    scheduled = [(PROJECT_A, "a1"), (PROJECT_B, "b1"), (PROJECT_A, "a2"), (PROJECT_B, "b2")]

    with Server(tmp_path, poll_interval=NO_TIMER) as accepting:
        for project in (PROJECT_A, PROJECT_B):
            add_version(accepting, project)
        for project, jobid in scheduled:
            assert schedule(accepting, project, jobid, barrier=True)["status"] == "ok"

        # The query state before anything runs describes the order in which the jobs will be dispatched.
        assert pending(accepting) == scheduled

    # A restarted instance dispatches the accepted jobs in that order.
    with Server(tmp_path) as running:
        running.wait_for_started(1)
        running.release()
        running.wait_for_started(len(scheduled))

        assert running.started() == ["a1", "b1", "a2", "b2"]

        for project, jobid in scheduled:
            wait_for_state(running, project, jobid, "finished")

        # Each job ran once. PROJECT_A has two of the four jobs, which would both have run before PROJECT_B's jobs
        # if the projects were drained one after another.
        assert records(running, PROJECT_A) == [("a1", "payload"), ("a2", "payload")]
        assert records(running, PROJECT_B) == [("b1", "payload"), ("b2", "payload")]
        assert pending(running) == []


def test_higher_priority_runs_first(tmp_path):
    with Server(tmp_path, poll_interval=NO_TIMER) as accepting:
        add_version(accepting, PROJECT_A)
        add_version(accepting, PROJECT_B)
        assert schedule(accepting, PROJECT_A, "low", priority=0, barrier=True)["status"] == "ok"
        assert schedule(accepting, PROJECT_B, "high", priority=10, barrier=True)["status"] == "ok"
        assert schedule(accepting, PROJECT_A, "medium", priority=5, barrier=True)["status"] == "ok"

        # The higher-priority job is first, even though it was accepted later.
        assert pending(accepting) == [(PROJECT_B, "high"), (PROJECT_A, "medium"), (PROJECT_A, "low")]

    with Server(tmp_path) as running:
        running.wait_for_started(1)
        running.release()
        running.wait_for_started(3)

        assert running.started() == ["high", "medium", "low"]

        for project, jobid in [(PROJECT_B, "high"), (PROJECT_A, "medium"), (PROJECT_A, "low")]:
            wait_for_state(running, project, jobid, "finished")

        assert records(running, PROJECT_A) == [("medium", "payload"), ("low", "payload")]
        assert records(running, PROJECT_B) == [("high", "payload")]


def test_update_priority_of_pending_job(tmp_path):
    with Server(tmp_path, poll_interval=NO_TIMER) as accepting:
        add_version(accepting, PROJECT_A)
        assert schedule(accepting, PROJECT_A, "first", priority=0, payload="original", barrier=True)["status"] == "ok"
        assert schedule(accepting, PROJECT_A, "second", priority=0, barrier=True)["status"] == "ok"
        assert pending(accepting) == [(PROJECT_A, "first"), (PROJECT_A, "second")]

        # Raise the first job's priority while it is pending. It keeps the position that it was first given.
        assert schedule(accepting, PROJECT_A, "first", priority=1, payload="updated", barrier=True)["status"] == "ok"

        jobs = req(accepting, "get", "/listjobs.json")["pending"]
        assert [(job["id"], job["args"]["payload"]) for job in jobs] == [
            ("first", "updated"),
            ("second", "payload"),
        ]

    # The updated priority, position and payload are rebuilt from the persisted jobs.
    with Server(tmp_path) as running:
        running.wait_for_started(1)
        running.release()
        running.wait_for_started(2)

        assert running.started() == ["first", "second"]

        for jobid in ("first", "second"):
            wait_for_state(running, PROJECT_A, jobid, "finished")

        # The updated payload ran, and the job ran once: repeating the request did not add a second pending job.
        assert records(running, PROJECT_A) == [("first", "updated"), ("second", "payload")]


def test_reschedule_finished_job_runs_again(server):
    add_version(server, PROJECT_A)

    assert schedule(server, PROJECT_A, "job", payload="first run")["status"] == "ok"
    wait_for_finished_jobs(server, 1)
    assert records(server, PROJECT_A) == [("job", "first run")]

    # Re-scheduling a finished job starts a new crawl of the same job ID, which overwrites the previous records.
    assert schedule(server, PROJECT_A, "job", payload="second run")["status"] == "ok"
    wait_for_finished_jobs(server, 2)

    # The second crawl ran, and it wrote its own record. (Scrapy appends to an existing feed file.)
    assert records(server, PROJECT_A) == [("job", "first run"), ("job", "second run")]
    assert pending(server) == []


def test_reschedule_running_job_is_rejected(server):
    add_version(server, PROJECT_A)

    assert schedule(server, PROJECT_A, "job", payload="original", barrier=True, wait=30)["status"] == "ok"
    server.wait_for_started(1)
    assert state(server, PROJECT_A, "job") == "running"

    # A running job must not be dispatched twice, because the second process would write the same log and items files.
    response = schedule(server, PROJECT_A, "job", payload="duplicate")
    assert response["status"] == "error"
    assert response["message"] == "job 'job' is already running"

    server.release()
    wait_for_state(server, PROJECT_A, "job", "finished")

    # Exactly one crawl ran, and its records are the original crawl's.
    assert records(server, PROJECT_A) == [("job", "original")]
    assert pending(server) == []
