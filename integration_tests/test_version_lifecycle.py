import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
import requests

FIXTURES = Path(__file__).absolute().parent.parent / "tests" / "fixtures"
with (FIXTURES / "versioned_a.egg").open("rb") as f:
    EGG_A = f.read()
with (FIXTURES / "versioned_b.egg").open("rb") as f:
    EGG_B = f.read()

PROJECT = "versionedproject"
OTHER_PROJECT = "versionedproject-other"
SPIDER = "versioned"
MARKER_A = "versioned-a"
MARKER_B = "versioned-b"
AUTH = ("hello12345", "67890world")


def _ephemeral_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    """Spawn an isolated Scrapyd instance, so this test never pollutes the shared instance used by other tests."""
    working_dir = tmp_path_factory.mktemp("scrapyd-version-lifecycle")
    port = _ephemeral_port()
    (working_dir / "scrapyd.conf").write_text(
        f"[scrapyd]\nusername = {AUTH[0]}\npassword = {AUTH[1]}\nhttp_port = {port}\nbind_address = 127.0.0.1\n"
    )
    (working_dir / "logs").mkdir()

    process = subprocess.Popen(
        [sys.executable, "-m", "scrapyd"],
        cwd=working_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    base_url = f"http://127.0.0.1:{port}"
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            response = requests.get(f"{base_url}/daemonstatus.json", auth=AUTH)
            if response.status_code == 200:
                break
        except requests.ConnectionError:
            time.sleep(0.2)
    else:
        process.kill()
        output, _ = process.communicate()
        raise RuntimeError(f"Scrapyd did not start:\n{output.decode()}")

    yield base_url

    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def req(server, method, path, **kwargs):
    response = getattr(requests, method)(f"{server}{path}", auth=AUTH, **kwargs)
    assert response.status_code == 200, f"200 != {response.status_code}"
    return response


def assert_error(server, method, path, message, **kwargs):
    data = req(server, method, path, **kwargs).json()
    data.pop("node_name")
    assert data == {"status": "error", "message": message}


def add_version(server, project, version, egg):
    data = req(
        server,
        "post",
        "/addversion.json",
        data={"project": project, "version": version},
        files={"egg": ("project.egg", egg, "application/octet-stream")},
    ).json()
    assert data["status"] == "ok"
    assert data["project"] == project
    assert data["version"] == version
    assert data["spiders"] == 1
    return data


def schedule(server, project, jobid, version=None):
    data = {"project": project, "spider": SPIDER, "jobid": jobid}
    if version is not None:
        data["_version"] = version
    return req(server, "post", "/schedule.json", data=data).json()


def wait_for_job(server, project, jobid, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = req(server, "get", "/status.json", params={"project": project, "job": jobid}).json()
        if data["currstate"] == "finished":
            return
        if data["currstate"] is None:
            raise AssertionError(f"job {jobid!r} of project {project!r} is unknown to Scrapyd")
        time.sleep(0.5)
    raise TimeoutError(f"job {jobid!r} of project {project!r} did not finish within {timeout}s")


def crawl(server, project, jobid, marker, version=None):
    """Schedule a job and verify, from the scraped record, which egg version was actually loaded."""
    assert schedule(server, project, jobid, version=version)["status"] == "ok"
    wait_for_job(server, project, jobid)
    log = req(server, "get", f"/logs/{project}/{SPIDER}/{jobid}.log").text
    assert f"version marker: {marker}" in log
    assert f"{{'marker': '{marker}'}}" in log


def list_versions(server, project):
    return req(server, "get", "/listversions.json", params={"project": project}).json()["versions"]


def list_spiders(server, project, version=None):
    params = {"project": project}
    if version is not None:
        params["_version"] = version
    return req(server, "get", "/listspiders.json", params=params).json()["spiders"]


def delete_version(server, project, version):
    data = req(server, "post", "/delversion.json", data={"project": project, "version": version}).json()
    assert data["status"] == "ok"


def delete_project(server, project):
    data = req(server, "post", "/delproject.json", data={"project": project}).json()
    assert data["status"] == "ok"


def test_version_lifecycle_spider_directory_matches_running_version(server):
    # The two eggs expose a spider with the same name, but each yields a different record ("marker"). The loaded
    # content, rather than the listspiders.json output alone, is used to determine which version actually runs.
    try:
        # Upload two versions of the main project. "2.0" is the highest version and thus the default version.
        add_version(server, PROJECT, "1.0", EGG_A)
        add_version(server, PROJECT, "2.0", EGG_B)
        # An independent project whose spiders and records must never be affected by the main project's changes.
        add_version(server, OTHER_PROJECT, "1.0", EGG_B)

        # Cross-query the directory before any job runs.
        assert list_versions(server, PROJECT) == ["1_0", "2_0"]
        assert list_versions(server, OTHER_PROJECT) == ["1_0"]
        assert list_spiders(server, PROJECT) == [SPIDER]
        assert list_spiders(server, PROJECT, "1.0") == [SPIDER]
        assert list_spiders(server, PROJECT, "2.0") == [SPIDER]
        assert list_spiders(server, OTHER_PROJECT) == [SPIDER]

        # Schedule the explicit (lower) version and the default (highest) version together, plus the other project.
        # The records scraped by each job prove which egg was actually loaded.
        crawl(server, PROJECT, "initial-explicit-v1", MARKER_A, version="1.0")
        crawl(server, PROJECT, "initial-default", MARKER_B)
        crawl(server, OTHER_PROJECT, "initial-other", MARKER_B)

        # Delete the default version, after the directory was queried and jobs were launched (that is, after the
        # spider-list cache was populated). Scheduling or listing the deleted version must not reuse its cache.
        delete_version(server, PROJECT, "2.0")
        assert list_versions(server, PROJECT) == ["1_0"]
        assert_error(
            server,
            "post",
            "/schedule.json",
            "version '2.0' not found",
            data={"project": PROJECT, "spider": SPIDER, "_version": "2.0", "jobid": "deleted-version"},
        )
        assert_error(
            server,
            "get",
            "/listspiders.json",
            "version '2.0' not found",
            params={"project": PROJECT, "_version": "2.0"},
        )

        # The default version must fall back to the surviving "1.0", without reusing the deleted "2.0" cache.
        crawl(server, PROJECT, "after-delete-default", MARKER_A)
        # The other project is unaffected by the deletion.
        assert list_versions(server, OTHER_PROJECT) == ["1_0"]
        assert list_spiders(server, OTHER_PROJECT) == [SPIDER]
        crawl(server, OTHER_PROJECT, "after-delete-other", MARKER_B)

        # Re-upload the deleted version, then replace an existing version ("1.0") with a different egg, after its
        # cache was populated by earlier jobs.
        add_version(server, PROJECT, "2.0", EGG_B)
        assert list_versions(server, PROJECT) == ["1_0", "2_0"]
        add_version(server, PROJECT, "1.0", EGG_B)
        # Both the explicit version and the default now load the replacement egg, without reusing the stale cache.
        crawl(server, PROJECT, "replaced-explicit-v1", MARKER_B, version="1.0")
        crawl(server, PROJECT, "replaced-default", MARKER_B)

        # Delete the project outright, after directory queries and jobs populated the cache.
        delete_project(server, PROJECT)
        assert req(server, "get", "/listprojects.json").json()["projects"] == [OTHER_PROJECT]
        assert_error(
            server,
            "post",
            "/schedule.json",
            f"project '{PROJECT}' not found",
            data={"project": PROJECT, "spider": SPIDER, "jobid": "deleted-project"},
        )
        assert_error(
            server,
            "get",
            "/listspiders.json",
            f"project '{PROJECT}' not found",
            params={"project": PROJECT},
        )

        # The other project still schedules and runs its own spider and records.
        crawl(server, OTHER_PROJECT, "after-project-delete-other", MARKER_B)

        # Re-create the project with the first egg. A fully evicted cache must load the freshly uploaded egg,
        # rather than any spider left over from the deleted project.
        add_version(server, PROJECT, "1.0", EGG_A)
        assert list_spiders(server, PROJECT) == [SPIDER]
        crawl(server, PROJECT, "readded-default", MARKER_A)
    finally:
        # Leave the isolated Scrapyd instance clean, even if an assertion failed above.
        for project in (PROJECT, OTHER_PROJECT):
            req(server, "post", "/delproject.json", data={"project": project})
        assert req(server, "get", "/listprojects.json").json()["projects"] == []
