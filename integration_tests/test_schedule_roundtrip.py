"""Integration tests for the scheduling → persistent queue → crawl process path.

These tests start a dedicated Scrapyd process (on a dynamically allocated
port, with its own eggs/dbs/logs/items directories), so that they can restart
it without interfering with the Scrapyd instance shared by the other
integration tests.
"""

import json
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
import requests

BASE_DIR = Path(__file__).absolute().parent.parent
FIXTURES_DIR = BASE_DIR / "tests" / "fixtures"

PROJECT = "echoargs"
SPIDER = "echoargs"
# echoargs.egg (marker "one") and echoargs2.egg (marker "two") contain the same
# spider, differing only in the reported marker, so that tests can tell which
# project version the crawl process actually ran. The default version is the
# greatest one (2.0), so 1.0 is used as the explicit, non-default version.
VERSION_EXPLICIT = "1.0"
VERSION_DEFAULT = "2.0"
MARKER_EXPLICIT = "one"
MARKER_DEFAULT = "two"

SCRAPYD_CONF = """\
[scrapyd]
http_port = {port}
bind_address = 127.0.0.1
poll_interval = 0.1
max_proc = 1
eggs_dir = eggs
dbs_dir = dbs
logs_dir = logs
items_dir = items
"""


def wait_for(predicate, description, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.1)
    raise TimeoutError(f"Timed out waiting for: {description}")


class ScrapydServer:
    def __init__(self, workdir):
        self.workdir = workdir
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.process = None

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}"

    def start(self):
        (self.workdir / "scrapyd.conf").write_text(SCRAPYD_CONF.format(port=self.port))
        log_file = (self.workdir / "scrapyd.log").open("ab")
        self.process = subprocess.Popen(
            [sys.executable, "-m", "scrapyd"],
            cwd=self.workdir,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        try:
            wait_for(self.is_up, "scrapyd to accept connections")
        except TimeoutError:
            raise RuntimeError((self.workdir / "scrapyd.log").read_text()) from None

    def is_up(self):
        if self.process.poll() is not None:
            raise RuntimeError(f"scrapyd exited with {self.process.returncode}")
        try:
            return requests.get(f"{self.url}/daemonstatus.json", timeout=1).status_code == 200
        except requests.RequestException:
            return False

    def stop(self):
        self.process.terminate()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()

    def restart(self):
        self.stop()
        self.start()


@pytest.fixture
def scrapyd(tmp_path):
    server = ScrapydServer(tmp_path)
    server.start()
    yield server
    server.stop()


def add_version(server, version, egg_basename):
    with (FIXTURES_DIR / f"{egg_basename}.egg").open("rb") as f:
        response = requests.post(
            f"{server.url}/addversion.json",
            data={"project": PROJECT, "version": version},
            files={"egg": f},
            timeout=30,
        )
    data = response.json()
    assert data["status"] == "ok", data


@pytest.fixture
def scrapyd_with_versions(scrapyd):
    add_version(scrapyd, VERSION_EXPLICIT, "echoargs")
    add_version(scrapyd, VERSION_DEFAULT, "echoargs2")
    return scrapyd


def schedule(server, params):
    """POST to schedule.json. ``params`` is a list of tuples, to allow repeated parameters (setting)."""
    response = requests.post(f"{server.url}/schedule.json", data=params, timeout=30)
    data = response.json()
    assert data["status"] == "ok", data
    return data["jobid"]


def listjobs(server):
    response = requests.get(f"{server.url}/listjobs.json", timeout=30)
    data = response.json()
    assert data["status"] == "ok", data
    return data


def wait_finished(server, jobid):
    def get_entry():
        return next((job for job in listjobs(server)["finished"] if job["id"] == jobid), None)

    return wait_for(get_entry, f"job {jobid} to finish")


def get_text(server, url_path):
    """GET a log or items file, waiting for it to appear and decoding it as UTF-8.

    A job is reported as running slightly before its crawl process creates its
    log file, hence the retry. Scrapy writes logs and items as UTF-8, which
    requests cannot be relied on to detect for text/plain responses.
    """

    def get():
        try:
            response = requests.get(f"{server.url}{url_path}", timeout=30)
        except requests.RequestException:
            return None
        return response.content.decode("utf-8") if response.status_code == 200 else None

    return wait_for(get, f"{url_path} to be served")


def get_json_lines(server, url_path):
    return [json.loads(line) for line in get_text(server, url_path).splitlines()]


def test_scheduled_parameters_reach_crawl_process(scrapyd_with_versions):
    """The scope, settings, version and jobid accepted by schedule.json must
    reach the crawl process unchanged, and match the reported job state."""
    server = scrapyd_with_versions
    jobid = "itest-params-0001"
    scope = "采集范围:北京・上海・São Paulo・🕷️"

    assert (
        schedule(
            server,
            [
                ("project", PROJECT),
                ("spider", SPIDER),
                ("_version", VERSION_EXPLICIT),
                ("jobid", jobid),
                ("scope", scope),
                # Repeated setting parameters, including a repeated key (last one wins).
                ("setting", "MY_SETTING=第一次"),
                ("setting", "MY_SETTING=第二次"),
                ("setting", "OTHER_SETTING=other"),
            ],
        )
        == jobid
    )

    # The job state reported by the webservice corresponds to the request.
    entry = wait_finished(server, jobid)
    assert entry["project"] == PROJECT
    assert entry["spider"] == SPIDER
    assert entry["log_url"] is not None
    assert entry["items_url"] is not None

    response = requests.get(f"{server.url}/status.json", params={"job": jobid, "project": PROJECT}, timeout=30)
    assert response.json()["currstate"] == "finished"

    # The scraped items record exactly what the crawl process received.
    (item,) = get_json_lines(server, entry["items_url"])
    assert item == {
        "marker": MARKER_EXPLICIT,  # the explicitly selected version ran
        "scope": scope,
        "job": jobid,
        "version_arg": VERSION_EXPLICIT,
        "version_env": VERSION_EXPLICIT,
        "my_setting": "第二次",
        "other_setting": "other",
    }

    # The job log reports the same received parameters.
    log_text = get_text(server, entry["log_url"])
    assert "echoargs received: " in log_text
    assert json.dumps(item, ensure_ascii=False, sort_keys=True) in log_text


def test_pending_jobs_survive_restart_without_crossing(scrapyd_with_versions):
    """Two concurrently submitted jobs with similar parameters must not see
    their parameters overwritten or crossed by a scheduler restart, and
    explicit version selection must not change the default version behavior."""
    server = scrapyd_with_versions
    scope_a = "采集范围:东城区"
    scope_b = "采集范围:东城区・西城区"  # similar to scope_a, to catch parameter crossing

    # Job A occupies the single launcher slot (max_proc = 1) for a while.
    assert (
        schedule(
            server,
            [
                ("project", PROJECT),
                ("spider", SPIDER),
                ("_version", VERSION_EXPLICIT),
                ("jobid", "itest-restart-a"),
                ("scope", scope_a),
                ("setting", "MY_SETTING=甲"),
                ("delay", "30"),
            ],
        )
        == "itest-restart-a"
    )
    wait_for(
        lambda: any(job["id"] == "itest-restart-a" for job in listjobs(server)["running"]),
        "job itest-restart-a to be running",
    )

    # The running job A received its own parameters. The log file is created
    # before the spider reports its parameters, so wait for the report.
    log_a = wait_for(
        lambda: (
            text
            if "echoargs received: " in (text := get_text(server, f"/logs/{PROJECT}/{SPIDER}/itest-restart-a.log"))
            else None
        ),
        "job itest-restart-a to report its parameters",
    )
    assert json.dumps(scope_a, ensure_ascii=False) in log_a
    assert json.dumps(scope_b, ensure_ascii=False) not in log_a

    # Job B, submitted while job A is running, stays pending in the persistent queue.
    assert (
        schedule(
            server,
            [
                ("project", PROJECT),
                ("spider", SPIDER),
                ("_version", VERSION_DEFAULT),
                ("jobid", "itest-restart-b"),
                ("scope", scope_b),
                ("setting", "MY_SETTING=乙"),
            ],
        )
        == "itest-restart-b"
    )
    (pending,) = listjobs(server)["pending"]
    assert pending["id"] == "itest-restart-b"
    assert pending["version"] == VERSION_DEFAULT
    assert pending["settings"] == {"MY_SETTING": "乙"}
    assert pending["args"] == {"scope": scope_b}

    # Restart the scheduler service. The pending job must survive in the
    # persistent (SQLite) queue, with its own parameters.
    server.restart()

    entry_b = wait_finished(server, "itest-restart-b")
    (item_b,) = get_json_lines(server, entry_b["items_url"])
    assert item_b == {
        "marker": MARKER_DEFAULT,
        "scope": scope_b,  # not scope_a: no parameter crossing
        "job": "itest-restart-b",
        "version_arg": VERSION_DEFAULT,
        "version_env": VERSION_DEFAULT,
        "my_setting": "乙",  # not "甲": no overwriting by job A's settings
        "other_setting": None,
    }

    # A job scheduled without _version still uses the default (greatest)
    # version: explicit version selection did not change the default behavior.
    assert (
        schedule(
            server,
            [
                ("project", PROJECT),
                ("spider", SPIDER),
                ("jobid", "itest-restart-c"),
                ("scope", "采集范围:默认版本"),
            ],
        )
        == "itest-restart-c"
    )
    entry_c = wait_finished(server, "itest-restart-c")
    (item_c,) = get_json_lines(server, entry_c["items_url"])
    assert item_c == {
        "marker": MARKER_DEFAULT,
        "scope": "采集范围:默认版本",
        "job": "itest-restart-c",
        "version_arg": None,  # no _version spider argument
        "version_env": None,  # no SCRAPYD_EGG_VERSION environment variable
        "my_setting": None,
        "other_setting": None,
    }
