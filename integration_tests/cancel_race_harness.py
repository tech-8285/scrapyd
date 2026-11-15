"""
Harness for integration tests that replay a cancel request against the dequeueing, process start and finishing
callbacks of the same job.

A test starts a real Scrapyd server, in a temporary directory, that runs real Scrapy crawl processes for a local
project, and drives it only through the HTTP API. The server's components are the documented extension points
(``poller`` and ``launcher``): subclasses that can *hold* one job at a precise point for as long as a test wants, so
that a realistic interleaving, which would otherwise last microseconds, can be replayed deterministically.

Holds are files, because the test and the server are separate processes:

- the test creates ``hold-<stage>-<job>`` before scheduling the job;
- the server creates ``<stage>-held-<job>`` when it reaches the held point, which the test waits for;
- the test creates ``release-<job>`` to let the server continue.

The stages are:

- ``deliver``: the job left the spider queue, but was not handed to a launcher slot yet (the poller's in-flight
  window);
- ``spawn``: the launcher received the job, but its crawl process was not spawned yet;
- ``finish``: the crawl process exited, but the job was not recorded as finished yet.
"""

import contextlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import requests
from twisted.internet import defer, reactor

from scrapyd.launcher import Launcher
from scrapyd.poller import QueuePoller

BASEDIR = Path(__file__).absolute().parent.parent

PROJECT = "racebot"

# Both spiders yield one item per request and chain requests, so their items file grows while they crawl. `slow`
# lasts long enough to cancel it mid-crawl; `quick` is short enough to also let it finish.
SPIDER_SOURCE = """\
import scrapy


class _ChainedSpider(scrapy.Spider):
    rounds = 200
    download_delay = 0.0

    async def start(self):
        yield self._request(0)

    def _request(self, i):
        return scrapy.Request(
            f"data:text/html,<html><body>item {i}</body></html>",
            callback=self.parse,
            cb_kwargs={"i": i},
            dont_filter=True,
        )

    def parse(self, response, i):
        yield {"job": getattr(self, "_job", None), "i": i}
        if i + 1 < int(self.rounds):
            yield self._request(i + 1)


class SlowSpider(_ChainedSpider):
    name = "slow"
    rounds = 80
    download_delay = 0.05


class QuickSpider(_ChainedSpider):
    name = "quick"
    rounds = 5
"""


def _gate_path(directory, name):
    return Path(directory) / name


class GateMixin:
    """Hold a job at a named stage until the test creates its release file."""

    gate_dir = None

    def _held(self, stage, job):
        return _gate_path(self.gate_dir, f"hold-{stage}-{job}").exists()

    def _hold(self, stage, job, callback):
        """Create the ``<stage>-held-<job>`` marker, then call ``callback`` once the job is released."""
        _gate_path(self.gate_dir, f"{stage}-held-{job}").touch()
        release = _gate_path(self.gate_dir, f"release-{job}")

        def poll():
            if release.exists():
                deferred.callback(None)
            else:
                reactor.callLater(0.01, poll)

        deferred = defer.Deferred()
        poll()

        def run(_):
            return callback()

        deferred.addCallback(run)
        return deferred


class GatedQueuePoller(GateMixin, QueuePoller):
    def __init__(self, config):
        super().__init__(config)
        self.gate_dir = Path(config.get("gate_dir")).absolute()

    def _deliver(self, project, message):
        job = message.get("_job")
        if job is None or not self._held("deliver", job):
            return super()._deliver(project, message)
        return self._hold("deliver", job, lambda: super(GatedQueuePoller, self)._deliver(project, message))


class GatedLauncher(GateMixin, Launcher):
    def __init__(self, config, app):
        super().__init__(config, app)
        self.gate_dir = Path(config.get("gate_dir")).absolute()

    def _spawn_process(self, message, slot):
        job = message["_job"]
        if not self._held("spawn", job):
            return super()._spawn_process(message, slot)
        return self._hold("spawn", job, lambda: super(GatedLauncher, self)._spawn_process(message, slot))

    def _process_finished(self, result, slot):
        process = self.processes.get(slot)
        if process is None or not self._held("finish", process.job):
            return super()._process_finished(result, slot)
        return self._hold("finish", process.job, lambda: super(GatedLauncher, self)._process_finished(result, slot))


def get_free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class RaceServer:
    """A real Scrapyd server, with a local Scrapy project and file-based holds, driven only through its HTTP API."""

    def __init__(self, root, *, max_proc=1):
        self.root = Path(root)
        self.max_proc = max_proc
        self.gates = self.root / "gates"
        self.items_dir = self.root / "items"
        self.logs_dir = self.root / "logs"
        self.log_file = self.root / "scrapyd.log"
        self.http_port = None
        self.process = None
        self._write_project()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.http_port}"

    def _write_project(self):
        # The local project, in the [settings] section of both scrapy.cfg (which Scrapy reads to find the settings
        # module, and to add the project to sys.path) and scrapyd.conf (see the ``settings`` section in
        # docs/config.rst).
        (self.root / "scrapy.cfg").write_text(f"[settings]\n{PROJECT} = {PROJECT}.settings\n")
        package = self.root / PROJECT
        (package / "spiders").mkdir(parents=True, exist_ok=True)
        (package / "__init__.py").write_text("")
        (package / "settings.py").write_text(f'SPIDER_MODULES = ["{PROJECT}.spiders"]\n')
        (package / "spiders" / "__init__.py").write_text("")
        (package / "spiders" / "crawl.py").write_text(SPIDER_SOURCE)

    def _write_config(self, http_port):
        self.gates.mkdir(parents=True, exist_ok=True)
        self.conf_file = self.root / "scrapyd.conf"
        self.conf_file.write_text(
            f"""\
[scrapyd]
bind_address = 127.0.0.1
http_port = {http_port}
max_proc = {self.max_proc}
poll_interval = 0.05
items_dir = {self.items_dir}
logs_dir = {self.logs_dir}
dbs_dir = {self.root / "dbs"}
eggs_dir = {self.root / "eggs"}
jobstorage = scrapyd.jobstorage.SqliteJobStorage
poller = integration_tests.cancel_race_harness.GatedQueuePoller
launcher = integration_tests.cancel_race_harness.GatedLauncher
gate_dir = {self.gates}

[settings]
{PROJECT} = {PROJECT}.settings
"""
        )

    def start(self):
        self.http_port = get_free_port()
        self._write_config(self.http_port)

        env = os.environ.copy()
        for key in [key for key in env if key.startswith("SCRAPYD_")]:
            del env[key]
        # Scrapyd and the crawl processes import the local project from the test's root.
        env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(BASEDIR), str(self.root), env.get("PYTHONPATH")]))
        # Scrapyd also reads ~/.scrapyd.conf, which would make the test depend on the developer's machine.
        env["HOME"] = str(self.root)

        if self.log_file.exists():
            self.log_file.unlink()
        kwargs = {}
        if os.name == "nt":
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            kwargs["start_new_session"] = True
        self._log = self.log_file.open("wb")
        self.process = subprocess.Popen(
            [sys.executable, "-m", "scrapyd"],
            cwd=self.root,
            env=env,
            stdout=self._log,
            stderr=subprocess.STDOUT,
            **kwargs,
        )

        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"Scrapyd exited:\n{self.log_file.read_text()}")
            try:
                response = requests.get(f"{self.url}/daemonstatus.json", timeout=1)
            except requests.RequestException:
                time.sleep(0.05)
            else:
                if response.status_code == 200:
                    return self
        raise RuntimeError(f"Scrapyd did not start:\n{self.log_file.read_text()}")

    def stop(self):
        if self.process is None:
            return
        if os.name == "nt":
            taskkill = shutil.which("taskkill")
            subprocess.run(
                [taskkill, "/F", "/T", "/PID", str(self.process.pid)],
                check=False,
                capture_output=True,
            )
        else:
            # The crawl processes are children: kill the whole process group, so that a crawl that outlives Scrapyd
            # doesn't write to a directory that a test is about to inspect or remove.
            with contextlib.suppress(ProcessLookupError):
                os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
        self.process.wait()
        self.process = None
        self._log.close()

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            with socket.socket() as sock:
                if sock.connect_ex(("127.0.0.1", self.http_port)) != 0:
                    return
            time.sleep(0.02)

    def restart(self):
        self.stop()
        return self.start()

    # HTTP API

    def post(self, path, **data):
        response = requests.post(f"{self.url}/{path}", data=data, timeout=30)
        response.raise_for_status()
        return response.json()

    def get(self, path, **params):
        response = requests.get(f"{self.url}/{path}", params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def schedule(self, spider, job, **settings):
        data = self.post(
            "schedule.json",
            project=PROJECT,
            spider=spider,
            jobid=job,
            setting=[f"{key}={value}" for key, value in settings.items()],
        )
        assert data == {"node_name": data["node_name"], "status": "ok", "jobid": job}, data
        return job

    def cancel(self, job, **data):
        return self.post("cancel.json", project=PROJECT, job=job, **data)

    def status(self, job):
        return self.get("status.json", project=PROJECT, job=job)["currstate"]

    def daemonstatus(self):
        return self.get("daemonstatus.json")

    def states(self, job):
        """Return the list of job lists in which ``job`` appears: at most one of pending, running and finished."""
        data = self.get("listjobs.json", project=PROJECT)
        return [state for state in ("pending", "running", "finished") if job in {row["id"] for row in data[state]}]

    # Files produced by the real crawl

    def items(self, spider, job):
        path = self.items_dir / PROJECT / spider / f"{job}.jl"
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines() if line]

    def log(self, spider, job):
        path = self.logs_dir / PROJECT / spider / f"{job}.log"
        return path.read_text() if path.exists() else ""

    def crawled(self, spider, job):
        """Return whether the crawl process created its log file, which it does when it starts."""
        return (self.logs_dir / PROJECT / spider / f"{job}.log").exists()

    # Holds

    def hold(self, stage, job):
        (self.gates / f"hold-{stage}-{job}").touch()
        return self

    def release(self, job):
        (self.gates / f"release-{job}").touch()

    def wait_held(self, stage, job, timeout=60):
        marker = self.gates / f"{stage}-held-{job}"
        self.wait_for(marker.exists, timeout=timeout, message=f"the server to hold {stage} of {job}")

    def wait_for(self, predicate, timeout=60, interval=0.02, message="condition"):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(interval)
        raise AssertionError(f"Timed out waiting for {message}. Server log:\n{self.log_file.read_text()}")

    def wait_idle(self, timeout=60):
        """Wait until no job is pending or running."""

        def idle():
            data = self.daemonstatus()
            return data["pending"] == 0 and data["running"] == 0

        self.wait_for(idle, timeout=timeout, message="an idle server")
