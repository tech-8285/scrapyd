import datetime
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import requests

from scrapyd.config import Config
from scrapyd.jobstorage import MemoryJobStorage, SqliteJobStorage
from scrapyd.launcher import ScrapyProcessProtocol

BASEDIR = Path(__file__).parent.parent.resolve()
FIXTURES = BASEDIR / "tests" / "fixtures"

# The same job identifier reused by two different projects. Status/ListJobs scope the finished state by project.
SHARED_JOBID = "shared"

# Ordinary jobs finish normally; the slow job is later canceled while running (abnormal end).
SLOW = ("gamma", "slow", "j07-slow")
SEQUENCE = [
    ("alpha", "spider1", "j01"),
    ("beta", "spider2", "j02"),
    ("alpha", "spider1", SHARED_JOBID),
    ("beta", "spider2", SHARED_JOBID),
    ("alpha", "spider1", "j05"),
    ("alpha", "spider2", "j06"),
]

# finished_to_keep = 5. Six ordinary jobs complete while the slow job stays running, so the storage holds the five
# most recent ordinary completions; the oldest one is evicted. Both projects share one identifier, and each keeps its
# own copy, proving project ownership scopes the finished state even when identifiers collide.
EVICTED = {("alpha", "j01")}
RETAINED_ORDINARY = {
    ("beta", "j02"),
    ("alpha", SHARED_JOBID),
    ("beta", SHARED_JOBID),
    ("alpha", "j05"),
    ("alpha", "j06"),
}
SLOW_JOB = ("gamma", "j07-slow")
# After the slow job ends abnormally it is added and the oldest retained ordinary job (beta/j02) is evicted.
EVICTED_AFTER_RESTART = EVICTED | {("beta", "j02")}
RETAINED_KEYS = (RETAINED_ORDINARY - {("beta", "j02")}) | {SLOW_JOB}

STORAGES = [
    ("memory", "scrapyd.jobstorage.MemoryJobStorage"),
    ("sqlite", "scrapyd.jobstorage.SqliteJobStorage"),
]


class ScrapydProcess:
    """A Scrapyd server running out of an isolated directory, restartable in place."""

    def __init__(self, base, jobstorage):
        self.base = Path(base)
        self.jobstorage = jobstorage
        self.process = None
        self.http_port = self._free_port()
        self.base.mkdir(parents=True, exist_ok=True)
        self._write_config()

    @staticmethod
    def _free_port():
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return str(sock.getsockname()[1])

    @property
    def url(self):
        return f"http://127.0.0.1:{self.http_port}/"

    def _write_config(self):
        # jobs_to_keep is high so the launcher never rotates the log/items files the assertions rely on.
        (self.base / "scrapyd.conf").write_text(
            "\n".join(
                [
                    "[scrapyd]",
                    f"http_port = {self.http_port}",
                    "bind_address = 127.0.0.1",
                    f"jobstorage = {self.jobstorage}",
                    "finished_to_keep = 5",
                    "jobs_to_keep = 20",
                    "logs_dir = logs",
                    "items_dir = items",
                    "dbs_dir = dbs",
                    "eggs_dir = eggs",
                ]
            )
        )

    def start(self):
        self.process = subprocess.Popen(
            [sys.executable, "-m", "scrapyd", "--nodaemon"],
            cwd=self.base,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        # The URL is logged before the port is listening, so poll until a request succeeds (as in mockserver).
        for _ in range(150):
            try:
                requests.get(self.url + "daemonstatus.json", timeout=1).raise_for_status()
                break
            except requests.RequestException:
                time.sleep(0.2)
        else:
            raise RuntimeError("Scrapyd did not start")

    def stop(self):
        if self.process is None:
            return
        self.process.terminate()
        try:
            self.process.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.communicate()
        self.process = None

    def restart(self):
        self.stop()
        self.start()


@contextmanager
def scrapyd_server(base, jobstorage):
    server = ScrapydProcess(base, jobstorage)
    try:
        server.start()
        yield server
    finally:
        server.stop()


def request(server, method, path, **kwargs):
    response = getattr(requests, method)(server.url + path, timeout=30, **kwargs)
    assert response.status_code == 200, f"{response.status_code} != 200: {response.text}"
    return response.json()


def add_eggs(server):
    for project, egg in [
        ("alpha", "mybot.egg"),
        ("beta", "mybot.egg"),
        ("gamma", "spiders_slow.egg"),
    ]:
        data = request(
            server,
            "post",
            "addversion.json",
            data={"project": project, "version": "1"},
            files={"egg": (egg, (FIXTURES / egg).read_bytes())},
        )
        assert data["status"] == "ok"


def schedule(server, project, spider, jobid):
    data = request(
        server,
        "post",
        "schedule.json",
        data={"project": project, "spider": spider, "jobid": jobid},
    )
    assert data["status"] == "ok", data
    assert data["jobid"] == jobid


def wait_for_state(server, project, jobid, expected):
    deadline = time.time() + 60
    state = None
    while time.time() < deadline:
        state = request(server, "get", f"status.json?job={jobid}&project={project}")["currstate"]
        if state == expected:
            return
        time.sleep(0.5)
    raise AssertionError(f"{project}/{jobid} never reached {expected!r}; last state {state!r}")


def wait_for_finished_count(server, count):
    deadline = time.time() + 60
    while time.time() < deadline:
        if request(server, "get", "daemonstatus.json")["finished"] == count:
            return
        time.sleep(0.5)
    raise AssertionError(f"finished count never reached {count}")


def wait_until_finished(server, project, jobid):
    """Wait for a job to leave the running state without requiring a fixed finished count (the storage is capped)."""
    deadline = time.time() + 60
    while time.time() < deadline:
        state = request(server, "get", f"status.json?job={jobid}&project={project}")["currstate"]
        if state in (None, "finished"):
            return state
        time.sleep(0.5)
    raise AssertionError(f"{project}/{jobid} never finished; last state {state!r}")


def list_jobs(server, project=None):
    query = f"?project={project}" if project else ""
    return request(server, "get", f"listjobs.json{query}")


def finished_keys(server):
    return {(job["project"], job["id"]) for job in list_jobs(server)["finished"]}


def snapshot(server):
    """Order-independent view of the query interfaces, suitable for cross-storage comparison."""
    jobs = list_jobs(server)
    return {
        "daemon": request(server, "get", "daemonstatus.json"),
        "pending": {(j["project"], j["id"]) for j in jobs["pending"]},
        "running": {(j["project"], j["id"]) for j in jobs["running"]},
        "finished": {(j["project"], j["id"], j["spider"], bool(j["end_time"])) for j in jobs["finished"]},
    }


def run_backend(tmp_path, label, jobstorage, persistent):
    """Replay the same completion/abnormal-termination sequence against one storage and restart the service.

    ``persistent`` selects the storage's restart contract: SQLite keeps its finished jobs across a restart, whereas
    the in-memory storage is process-local and starts empty.
    """
    with scrapyd_server(tmp_path / label, jobstorage) as server:
        add_eggs(server)

        # Start the slow job first and confirm it actually runs, so the later fast jobs complete while it is active.
        schedule(server, *SLOW)
        wait_for_state(server, SLOW[0], SLOW[2], "running")

        for project, spider, jobid in SEQUENCE:
            schedule(server, project, spider, jobid)
            wait_for_state(server, project, jobid, "finished")

        # The slow job keeps running while the six ordinary jobs complete and cross the retention cap.
        wait_until_finished(server, "alpha", "j06")
        wait_for_finished_count(server, 5)

        # Every retained finished state corresponds to a real crawl with a log and items feed on disk.
        items_dir = server.base / "items"
        logs_dir = server.base / "logs"
        for project, jobid in RETAINED_ORDINARY:
            assert list((items_dir / project).glob(f"*/{jobid}.jl")), f"missing items for {project}/{jobid}"
            assert list((logs_dir / project).glob(f"*/{jobid}.log")), f"missing log for {project}/{jobid}"

        # Evicted records are gone from the query interface, while the still-running job is unaffected.
        keys = finished_keys(server)
        assert keys == RETAINED_ORDINARY
        for project, jobid in EVICTED:
            assert (project, jobid) not in keys
        assert request(server, "get", "status.json?job=j07-slow&project=gamma")["currstate"] == "running"
        assert SLOW_JOB in {(j["project"], j["id"]) for j in list_jobs(server)["running"]}

        # The shared identifier is scoped per project: both projects retain their own copy of the same job id.
        assert request(server, "get", f"status.json?job={SHARED_JOBID}&project=alpha")["currstate"] == "finished"
        assert request(server, "get", f"status.json?job={SHARED_JOBID}&project=beta")["currstate"] == "finished"
        # Without a project filter, the identifier still resolves to a finished job.
        assert request(server, "get", f"status.json?job={SHARED_JOBID}")["currstate"] == "finished"
        # The evicted oldest job is not resolvable.
        assert request(server, "get", "status.json?job=j01&project=alpha")["currstate"] is None

        # Snapshot before the abnormal end, so a restart can also be shown not to resurrect anything already evicted.
        before_cancel = snapshot(server)

        # Abnormal termination: SIGKILL the running slow job ("Process died"), then restart the service.
        cancel = request(
            server,
            "post",
            "cancel.json",
            data={"project": "gamma", "job": "j07-slow", "signal": "KILL"},
        )
        assert cancel["prevstate"] == "running"
        wait_until_finished(server, "gamma", "j07-slow")
        wait_for_finished_count(server, 5)
        assert finished_keys(server) == RETAINED_KEYS

        # The abnormal (killed) job is a real crawl with a log file, and it reached a finished end state.
        assert list((logs_dir / "gamma").glob("*/j07-slow.log"))
        assert request(server, "get", "status.json?job=j07-slow&project=gamma")["currstate"] == "finished"

        before_restart = snapshot(server)
        server.restart()
        after_restart = snapshot(server)

        jobs = list_jobs(server)
        assert jobs["pending"] == []
        assert jobs["running"] == []

        # A restart never resurrects an evicted record and never restarts the killed job, under either storage.
        for project, jobid in EVICTED_AFTER_RESTART:
            assert request(server, "get", f"status.json?job={jobid}&project={project}")["currstate"] is None
        assert SLOW_JOB not in {(j["project"], j["id"]) for j in jobs["running"]}

        if persistent:
            # SQLite persists the retained history, identically under every project filter.
            assert finished_keys(server) == RETAINED_KEYS
            for project, jobid in RETAINED_KEYS:
                per_project = {(j["project"], j["id"]) for j in list_jobs(server, project)["finished"]}
                assert (project, jobid) in per_project
            assert request(server, "get", "status.json?job=j07-slow&project=gamma")["currstate"] == "finished"
        else:
            # The in-memory storage is process-local: a restart starts with no retained history.
            assert finished_keys(server) == set()
            assert request(server, "get", "status.json?job=j07-slow&project=gamma")["currstate"] is None

        return {
            "persistent": persistent,
            "before_cancel": before_cancel,
            "before_restart": before_restart,
            "after_restart": after_restart,
        }


def test_finished_job_retention_parity(tmp_path):
    results = {
        label: run_backend(tmp_path, label, jobstorage, persistent=(label == "sqlite"))
        for label, jobstorage in STORAGES
    }
    memory, sqlite = results["memory"], results["sqlite"]

    # Before any restart the two storages must expose identical project ownership, end states and retained sets.
    assert memory["before_cancel"]["finished"] == sqlite["before_cancel"]["finished"]
    assert memory["before_cancel"]["running"] == sqlite["before_cancel"]["running"]
    assert memory["before_restart"] == sqlite["before_restart"]

    # Neither backend resurrects evicted jobs or the killed running job after a restart.
    for result in results.values():
        assert result["after_restart"]["pending"] == set()
        assert result["after_restart"]["running"] == set()
        restarted_keys = {(project, jobid) for project, jobid, _spider, _ended in result["after_restart"]["finished"]}
        assert restarted_keys.isdisjoint(EVICTED_AFTER_RESTART)

    # Each backend honors its own restart contract: SQLite retains, memory resets to empty.
    assert sqlite["after_restart"]["finished"] == sqlite["before_restart"]["finished"]
    assert sqlite["after_restart"]["daemon"]["finished"] == 5
    assert memory["after_restart"]["finished"] == set()
    assert memory["after_restart"]["daemon"]["finished"] == 0


def _finished_job(project, spider, jobid, end_time):
    """Build a finished process protocol, the object the launcher hands to the job storage and ListJobs renders."""
    process = ScrapyProcessProtocol(project, spider, jobid, env={}, args=[])
    process.start_time = end_time - datetime.timedelta(seconds=1)
    process.end_time = end_time
    return process


def _record(job):
    """The fields the ListJobs query interface exposes for a finished job."""
    return job.project, job.spider, job.job, job.start_time, job.end_time


def test_tied_end_time_and_shared_identifier_retention(tmp_path):
    # Exact end-time ties cannot be produced by real crawls (now() always differs by microseconds), so the storages
    # are driven directly with the same monotonic completion sequence the launcher would create. The final three jobs
    # share one end_time, and two projects share one identifier within that tie group.
    tied = datetime.datetime(2001, 2, 3, 4, 5, 7)
    sequence = [
        ("alpha", "spider1", "old", datetime.datetime(2001, 2, 3, 4, 5, 6)),
        ("alpha", "spider2", "shared", tied),
        ("beta", "spider1", "shared", tied),
        ("beta", "spider2", "tied-b", tied),
        ("alpha", "spider1", "tied-a", tied),
    ]

    def build(jobstorage_cls, dbs_dir):
        config = Config(values={"dbs_dir": str(dbs_dir), "finished_to_keep": "3"})
        storage = jobstorage_cls(config)
        for project, spider, jobid, end_time in sequence:
            storage.add(_finished_job(project, spider, jobid, end_time))
        return storage

    memory = build(MemoryJobStorage, ":memory:")
    sqlite = build(SqliteJobStorage, tmp_path)

    # Both storages keep the same number of jobs under the cap.
    assert len(memory) == len(sqlite) == 3

    memory_records = {_record(job) for job in memory}
    sqlite_records = {_record(job) for job in sqlite}

    # Retention decision under an end-time tie is identical, including every query-interface field.
    assert memory_records == sqlite_records

    retained_keys = {(project, jobid) for project, _spider, jobid, _start, _end in memory_records}

    # With keep=3 and four jobs sharing one end_time, both storages make the identical tie-break: they retain the
    # same three jobs and evict the same one (the earliest insert in the tie group), plus the older pre-tie job.
    assert retained_keys == {
        ("beta", "shared"),
        ("beta", "tied-b"),
        ("alpha", "tied-a"),
    }
    assert ("alpha", "old") not in retained_keys

    # The shared identifier is scoped by project: beta's copy is retained while alpha's colliding copy is evicted.
    assert ("beta", "shared") in retained_keys
    assert ("alpha", "shared") not in retained_keys
