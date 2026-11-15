"""Helpers for driving a real Scrapyd instance from integration tests.

Unlike the other integration tests, these tests start their own Scrapyd
instance in a temporary directory, so that they control the configuration, the
deployed eggs, and the process lifetime, and can restart it.

Evidence is collected from three places:

- the HTTP API (``listjobs.json``, ``status.json``), for the job lifecycle;
- the spider queue's SQLite database, for what was persisted while a job waited;
- the item feed written by the crawl process, for what the process received;
- the Scrapyd log, for the exact command line passed to the crawl process.
"""

from __future__ import annotations

import ast
import json
import os
import re
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import zipfile
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
PROJECT_SRC = Path(__file__).resolve().parent / "projects" / "paramsbot"

PROJECT = "params"
ECHO_SPIDER = "echo"
GATE_SPIDER = "gate"

TIMEOUT = 60

PROCESS_STARTED_RE = re.compile(r"Process started: .* args=(\[.*\])$")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def build_egg(dest, marker, *, source=PROJECT_SRC, package="paramsbot"):
    """Build an egg from the fixture project, tagged with a version marker.

    The marker is the only difference between versions, so a crawl process
    reveals which egg version was activated.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as egg:
        for path in sorted(source.rglob("*.py")):
            egg.writestr(f"{package}/{path.relative_to(source).as_posix()}", path.read_bytes())
        egg.writestr(f"{package}/version_marker.py", f"MARKER = {marker!r}\n")
        egg.writestr("EGG-INFO/PKG-INFO", "Metadata-Version: 1.0\nName: paramsbot\nVersion: 0.0.0\n")
        egg.writestr("EGG-INFO/entry_points.txt", f"[scrapy]\nsettings = {package}.settings\n")
        egg.writestr("EGG-INFO/top_level.txt", f"{package}\n")


def parse_crawl_argv(argv):
    """Split the spawned ``scrapy crawl`` command line into its parts.

    ``argv`` is the full command line, as passed by the Launcher to the
    operating system: ``[python, -m, scrapyd.runner, crawl, spider, ...]``.
    """
    assert argv[1:4] == ["-m", "scrapyd.runner", "crawl"], argv
    spider, rest = argv[4], argv[5:]
    assert len(rest) % 2 == 0, rest

    settings, args = [], []
    for flag, pair in zip(rest[::2], rest[1::2], strict=True):
        key, value = pair.split("=", 1)
        if flag == "-s":
            settings.append((key, value))
        else:
            assert flag == "-a", flag
            args.append((key, value))
    return spider, settings, args


class Scrapyd:
    """A Scrapyd instance running in ``workdir``, with two egg versions deployed."""

    def __init__(self, workdir, versions=("v1", "v2")):
        self.workdir = Path(workdir)
        self.versions = versions
        self.port = free_port()
        self.proc = None
        self.log_file = None
        self.release_files = []

    # Setup and lifecycle

    def prepare(self):
        self.workdir.mkdir(parents=True, exist_ok=True)
        for version in self.versions:
            build_egg(self.workdir / "eggs" / PROJECT / f"{version}.egg", version)
        (self.workdir / "scrapyd.conf").write_text(
            "[scrapyd]\n"
            "bind_address = 127.0.0.1\n"
            "dbs_dir = dbs\n"
            "logs_dir = logs\n"
            "items_dir = items\n"
            "max_proc = 1\n"
            "poll_interval = 0.1\n"
            "finished_to_keep = 100\n",
            encoding="utf-8",
        )

    def environment(self):
        env = os.environ.copy()
        # Import this checkout, even when another copy of Scrapyd is installed.
        env["PYTHONPATH"] = os.pathsep.join([str(REPO_ROOT), env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
        env["PYTHONIOENCODING"] = "UTF-8"
        # The instance's port is fully controlled by the tests.
        env["SCRAPYD_HTTP_PORT"] = str(self.port)
        env["HOME"] = env["USERPROFILE"] = str(self.workdir)
        for name in (
            "SCRAPY_PROJECT",
            "SCRAPY_SETTINGS_MODULE",
            "SCRAPYD_EGG_VERSION",
            "SCRAPYD_USERNAME",
            "SCRAPYD_PASSWORD",
            "SCRAPYD_BIND_ADDRESS",
        ):
            env.pop(name, None)
        return env

    def start(self):
        assert self.proc is None, "already started"
        self.log_file = (self.workdir / "scrapyd.log").open("ab")
        kwargs = {"cwd": self.workdir, "env": self.environment(), "stdout": self.log_file, "stderr": subprocess.STDOUT}
        if os.name == "nt":
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            kwargs["start_new_session"] = True  # own process group, so children can be stopped together
        self.proc = subprocess.Popen([sys.executable, "-m", "scrapyd"], **kwargs)
        self._wait_until_ready()

    def stop(self):
        proc, self.proc = self.proc, None
        if proc is not None and proc.poll() is None:
            self._terminate_tree(proc, kill=False)
            try:
                proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self._terminate_tree(proc, kill=True)
                proc.wait(timeout=20)
        if self.log_file is not None:
            self.log_file.close()
            self.log_file = None
        self._wait_until_stopped()

    @staticmethod
    def _terminate_tree(proc, *, kill):
        """Terminate the instance and the crawl processes it started."""
        if os.name == "nt":
            # The launcher's children are not in a job object, so kill the whole tree.
            args = ["taskkill", "/T", "/PID", str(proc.pid)]
            if kill:
                args.insert(1, "/F")
            subprocess.run(args, check=False, capture_output=True)
        else:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL if kill else signal.SIGTERM)

    def restart(self):
        """Stop and start the instance, discarding all in-memory state."""
        self.stop()
        self.start()

    def close(self):
        # Let gated jobs finish, so that no crawl process outlives the test.
        for release in self.release_files:
            release.touch(exist_ok=True)
        self.stop()

    def _wait_until_ready(self):
        def ready():
            if self.proc.poll() is not None:
                raise AssertionError(f"Scrapyd exited with {self.proc.returncode}:\n{self.log_tail()}")
            try:
                self.get("daemonstatus.json")
            except requests.ConnectionError:
                return False
            return True

        self.wait_until(ready, "Scrapyd to accept connections")

    def _wait_until_stopped(self):
        def stopped():
            with socket.socket() as sock:
                sock.settimeout(0.2)
                return sock.connect_ex(("127.0.0.1", self.port)) != 0

        self.wait_until(stopped, "Scrapyd to stop", timeout=20)

    # HTTP API

    def get(self, path, **params):
        response = requests.get(f"http://127.0.0.1:{self.port}/{path}", params=params, timeout=30)
        return self._decode(response)

    def post(self, path, data):
        response = requests.post(f"http://127.0.0.1:{self.port}/{path}", data=data, timeout=30)
        return self._decode(response)

    @staticmethod
    def _decode(response):
        data = response.json()
        data.pop("node_name", None)
        return data

    def ok(self, path, **params):
        data = self.get(path, **params)
        assert data.pop("status") == "ok", data
        return data

    def try_get(self, path, **params):
        """Like ``get``, but return None if the instance is not reachable."""
        try:
            return self.get(path, **params)
        except (requests.ConnectionError, requests.Timeout):
            return None

    def schedule(self, jobid=None, *, spider=ECHO_SPIDER, version=None, settings=(), args=(), project=PROJECT):
        """Schedule a job, returning the ID assigned by the API."""
        data = [("project", project), ("spider", spider)]
        if jobid is not None:
            data.append(("jobid", jobid))
        if version is not None:
            data.append(("_version", version))
        data.extend(("setting", setting) for setting in settings)
        data.extend(args)
        response = self.post("schedule.json", data)
        assert response["status"] == "ok", response
        if jobid is not None:
            assert response["jobid"] == jobid, response
        else:
            assert response["jobid"], response
        return response["jobid"]

    def status(self, jobid, *, project=PROJECT):
        data = self.ok("status.json", job=jobid, project=project)
        return data["currstate"]

    def jobs(self, *, project=PROJECT):
        return self.ok("listjobs.json", project=project)

    def pending(self, *, project=PROJECT):
        return self.jobs(project=project)["pending"]

    def finished(self, *, project=PROJECT):
        return self.jobs(project=project)["finished"]

    def list_versions(self, *, project=PROJECT):
        return self.ok("listversions.json", project=project)["versions"]

    def list_spiders(self, *, version=None, project=PROJECT):
        params = {"project": project}
        if version is not None:
            params["_version"] = version
        return self.ok("listspiders.json", **params)["spiders"]

    # Waiting

    def wait_until(self, predicate, message, timeout=TIMEOUT):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return
            time.sleep(0.05)
        raise AssertionError(f"timed out waiting for {message}:\n{self.log_tail()}")

    def wait_for_status(self, jobid, expected, *, project=PROJECT, timeout=TIMEOUT):
        def current():
            data = self.try_get("status.json", job=jobid, project=project)
            return None if data is None else data["currstate"]

        self.wait_until(lambda: current() == expected, f"job {jobid!r} to be {expected!r}", timeout)

    # Jobs

    def start_gate(self):
        """Start a job that holds the only launcher slot until its release file exists."""
        release = self.workdir / "gate-release"
        self.release_files.append(release)
        jobid = self.schedule("gate-job", spider=GATE_SPIDER, args=[("wait_for", str(release))])
        self.wait_for_status(jobid, "running")
        return jobid, release

    def spawn_argv(self, jobid, *, project=PROJECT):
        """Return the command line the Launcher passed to the OS for ``jobid``.

        The Launcher logs it with ``Process started: project=... job=... args=[...]``.
        """
        want_job = repr(jobid)
        want_project = repr(project)

        def find():
            for line in self.log_text().splitlines():
                match = PROCESS_STARTED_RE.search(line)
                if not match or f"project={want_project} " not in line or f"job={want_job} " not in line:
                    continue
                return ast.literal_eval(match.group(1))
            return None

        self.wait_until(lambda: find() is not None, f"job {jobid!r} to be started")
        return find()

    def items(self, jobid, *, spider=ECHO_SPIDER, project=PROJECT):
        """Return the items written to the finished crawl process's feed file."""
        self.wait_for_status(jobid, "finished", project=project)
        path = self.workdir / "items" / project / spider / f"{jobid}.jl"
        self.wait_until(path.exists, f"the item feed of job {jobid!r}")
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]

    def queue_messages(self, *, project=PROJECT):
        """Return the pending messages persisted in the spider queue database."""
        database = self.workdir / "dbs" / f"{project}.db"
        if not database.exists():
            return []
        # Scrapyd may be writing at the same time, so wait for the write lock.
        with sqlite3.connect(database, timeout=30) as conn:
            rows = conn.execute("SELECT message FROM spider_queue ORDER BY id").fetchall()
        return [json.loads(bytes(row[0]).decode("ascii")) for row in rows]

    # Logs

    def log_text(self):
        path = self.workdir / "scrapyd.log"
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8", errors="replace")

    def log_tail(self, lines=40):
        return "\n".join(self.log_text().splitlines()[-lines:])
