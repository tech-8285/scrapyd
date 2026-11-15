"""Replayable, local crawl schedules.

These tests drive a real Scrapyd server (in a subprocess) and a real Scrapy project: the ``records`` project appends
the job ID to a file when a crawl starts. They assert that the order in which crawls actually run matches the order of
the pending jobs that the webservices report, and that the order is reproducible after a restart.
"""

import io
import time

import pytest
import requests

from tests import get_egg_data
from tests.mockserver import MockScrapydServer

POLL_INTERVAL = 0.1
# A long poll interval leaves the pending jobs in the queue, so that a test can inspect them, or stop the server, before
# the launcher starts any crawl.
NO_POLLING = 1000
TIMEOUT = 60


def write_config(path, poll_interval):
    path.write_text(f"[scrapyd]\nmax_proc = 1\npoll_interval = {poll_interval}\n")


@pytest.fixture
def server(chdir):
    write_config(chdir / "scrapyd.conf", POLL_INTERVAL)
    with MockScrapydServer() as server:
        yield server


def deploy(server, project="records"):
    response = requests.post(
        server.urljoin("addversion.json"),
        data={"project": project, "version": "0.1"},
        files={"egg": io.BytesIO(get_egg_data("records"))},
        timeout=TIMEOUT,
    )
    assert response.json()["status"] == "ok", response.text


def schedule(server, jobid, priority=0, sleep=0, chdir=None, project="records"):
    data = {"project": project, "spider": "record", "jobid": jobid, "priority": priority}
    if sleep:
        data["sleep"] = sleep
    if chdir:
        data["record"] = str(chdir / "records.txt")

    response = requests.post(server.urljoin("schedule.json"), data=data, timeout=TIMEOUT)
    assert response.json()["status"] == "ok", response.text


def listjobs(server, project="records"):
    params = {} if project is None else {"project": project}
    response = requests.get(server.urljoin("listjobs.json"), params=params, timeout=TIMEOUT)
    return response.json()


def status(server, job):
    response = requests.get(server.urljoin("status.json"), params={"job": job}, timeout=TIMEOUT)
    return response.json()["currstate"]


def jobs(server, key, project="records"):
    return [job["id"] for job in listjobs(server, project)[key]]


def wait_for(condition, message):
    deadline = time.monotonic() + TIMEOUT
    while time.monotonic() < deadline:
        if value := condition():
            return value
        time.sleep(POLL_INTERVAL)
    raise AssertionError(f"Timed out waiting for {message}")


def wait_until_running(server, job):
    wait_for(lambda: status(server, job) == "running", f"job {job!r} to run")


def wait_until_finished(server, count, project="records"):
    wait_for(lambda: len(jobs(server, "finished", project)) >= count, f"{count} finished jobs")


def records(chdir):
    return (chdir / "records.txt").read_text().split()


def test_pending_order_and_actual_crawl_order(server, chdir):
    deploy(server)

    # The running job occupies the only slot, so that the pending jobs can be inspected.
    schedule(server, "j0", sleep=1.5, chdir=chdir)
    wait_until_running(server, "j0")

    schedule(server, "j1", chdir=chdir)
    schedule(server, "j2", chdir=chdir)
    schedule(server, "j3", priority=1, chdir=chdir)

    # listjobs reports the order in which the jobs will run: higher priority first, then acceptance order.
    assert jobs(server, "pending") == ["j3", "j1", "j2"]
    assert jobs(server, "running") == ["j0"]
    assert status(server, "j3") == "pending"

    wait_until_finished(server, 4)

    # The actual crawl order matches the queried order.
    assert records(chdir) == ["j0", "j3", "j1", "j2"]
    assert sorted(jobs(server, "finished")) == ["j0", "j1", "j2", "j3"]
    assert status(server, "j3") == "finished"


def test_priority_update_while_another_job_runs(server, chdir):
    deploy(server)

    schedule(server, "j0", sleep=1.5, chdir=chdir)
    wait_until_running(server, "j0")

    # Accepted in this order at priority 0, so j1 runs before j2.
    schedule(server, "j1", chdir=chdir)
    schedule(server, "j2", chdir=chdir)
    assert jobs(server, "pending") == ["j1", "j2"]

    # Scheduling a pending job again updates its priority, and it moves ahead of the lower-priority jobs.
    schedule(server, "j2", priority=5, chdir=chdir)
    assert jobs(server, "pending") == ["j2", "j1"]

    # Scheduling it again with the same priority changes nothing.
    schedule(server, "j2", priority=5, chdir=chdir)
    assert jobs(server, "pending") == ["j2", "j1"]

    # A running job cannot be re-prioritized: it is rejected, instead of being dispatched a second time.
    response = requests.post(
        server.urljoin("schedule.json"),
        data={
            "project": "records",
            "spider": "record",
            "jobid": "j0",
            "priority": 9,
            "record": str(chdir / "records.txt"),
        },
        timeout=TIMEOUT,
    )
    assert response.json()["status"] == "error"
    assert response.json()["message"] == "job 'j0' is already running"

    wait_until_finished(server, 3)

    assert records(chdir) == ["j0", "j2", "j1"]


def test_dispatch_order_survives_restart(chdir):
    # The server accepts jobs, but only polls at startup (when the queues are empty), so no crawl starts.
    write_config(chdir / "scrapyd.conf", NO_POLLING)
    with MockScrapydServer() as server:
        deploy(server)
        schedule(server, "j1", chdir=chdir)
        schedule(server, "j2", priority=2, chdir=chdir)
        schedule(server, "j3", chdir=chdir)

        assert jobs(server, "pending") == ["j2", "j1", "j3"]
        assert not (chdir / "records.txt").exists()

    # After the restart, the accepted jobs are replayed in the same order: the persisted sequence orders them, rather
    # than the order in which the project's queue happens to read them.
    write_config(chdir / "scrapyd.conf", POLL_INTERVAL)
    with MockScrapydServer() as server:
        # A job accepted after the restart runs after the jobs that were accepted before it.
        schedule(server, "j4", chdir=chdir)

        wait_until_finished(server, 4)

    assert records(chdir) == ["j2", "j1", "j3", "j4"]


def test_dispatch_order_across_projects(chdir):
    # Two projects accept jobs while the launcher doesn't poll, so that all jobs are pending at once.
    write_config(chdir / "scrapyd.conf", NO_POLLING)
    with MockScrapydServer() as server:
        # Two projects share the launcher's slots, so their pending jobs are ordered relative to each other.
        deploy(server, "alpha")
        deploy(server, "beta")

        schedule(server, "a1", project="alpha", chdir=chdir)
        schedule(server, "b1", project="beta", chdir=chdir)
        schedule(server, "a2", project="alpha", chdir=chdir)
        schedule(server, "b2", project="beta", priority=1, chdir=chdir)

        # listjobs returns the pending jobs of all projects in dispatch order: priority first, then acceptance order.
        assert jobs(server, "pending", project=None) == ["b2", "a1", "b1", "a2"]

        # Adding a backlog to one project doesn't push the other project's pending jobs back.
        for index in range(4):
            schedule(server, f"alpha-extra-{index}", project="alpha", chdir=chdir)

        assert jobs(server, "pending", project=None)[:4] == ["b2", "a1", "b1", "a2"]
        assert [job["project"] for job in listjobs(server, None)["pending"]][:4] == ["beta", "alpha", "beta", "alpha"]

    # The jobs run in the same order after the restart, and the backlog of one project doesn't starve the other.
    write_config(chdir / "scrapyd.conf", POLL_INTERVAL)
    with MockScrapydServer() as server:
        wait_until_finished(server, 8, project=None)

    assert records(chdir) == [
        "b2",
        "a1",
        "b1",
        "a2",
        "alpha-extra-0",
        "alpha-extra-1",
        "alpha-extra-2",
        "alpha-extra-3",
    ]
