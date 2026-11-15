"""
Replay cancel requests against the interleavings of dequeueing, process start and the finishing callback.

Each test starts a real Scrapyd server, with a real local Scrapy project, and only talks to its HTTP API. See
:mod:`integration_tests.cancel_race_harness` for how a test holds a job at a precise point.

The assertions distinguish:

- the *reported* state: ``cancel.json``'s ``prevstate``, ``status.json``'s ``currstate``, and which of
  ``pending``/``running``/``finished`` contains the job in ``listjobs.json``;
- the *real* crawl behavior: whether the crawl process created its log file, which items it wrote, and what Scrapy
  reported in that log.

A cancelled job that never started must be gone from every list (the documented behavior of cancelling a pending job:
it is removed, so ``status.json`` returns ``None``), and must never start crawling afterwards. A cancellation that
loses the race to a process that already crawled must leave a finished job with its complete output. Neither may leave
the job in two lists at once.
"""

import pytest

from integration_tests.cancel_race_harness import RaceServer


@pytest.fixture
def server(tmp_path):
    race_server = RaceServer(tmp_path)
    try:
        yield race_server.start()
    finally:
        race_server.stop()


def assert_one_state(server, job):
    """Assert that the job is reported in at most one of pending, running and finished, and return its lists."""
    states = server.states(job)
    assert len(states) <= 1, f"{job} is reported in {states} (expected at most one list)"
    return states


def assert_gone(server, job):
    """Assert that a cancelled job is reported nowhere, and can't be cancelled again."""
    assert assert_one_state(server, job) == []
    assert server.status(job) is None
    assert server.cancel(job)["prevstate"] is None


def assert_finished(server, job, spider, *, items=None, log=None, wait=True):
    """
    Assert that the job is finished, with a single consistent record, and return its items.

    ``items`` is a required number of items, for a crawl that must have completed. ``log`` is a string the crawl log
    must contain.
    """
    if wait:
        server.wait_for(lambda: server.states(job) == ["finished"], message=f"{job} to finish")
    assert assert_one_state(server, job) == ["finished"]
    assert server.status(job) == "finished"
    assert server.cancel(job)["prevstate"] is None
    assert server.crawled(spider, job), "a finished job's crawl process must have created its log file"
    job_items = server.items(spider, job)
    if items is not None:
        assert len(job_items) == items
    if log is not None:
        assert log in server.log(spider, job)
    return job_items


def test_cancel_before_dequeue(server):
    """The only slot is busy, so a second job is still in its spider queue when it is cancelled."""
    server.schedule("slow", "blocker")
    server.wait_for(lambda: server.status("blocker") == "running", message="the blocker to run")

    server.schedule("quick", "victim")
    server.wait_for(lambda: server.states("victim") == ["pending"], message="the victim to be pending")

    # The victim is in the queue: the cancellation removes it, as documented.
    assert server.cancel("victim")["prevstate"] == "pending"
    assert_gone(server, "victim")

    # The blocker was dispatched normally: its crawl process started, and was cancelled while running.
    server.wait_for(lambda: "Spider opened" in server.log("slow", "blocker"), message="the blocker to crawl")
    assert server.cancel("blocker")["prevstate"] == "running"
    server.wait_idle()

    assert_finished(server, "blocker", "slow", log="Received SIGINT")
    # The victim never crawled, not even after the slot was freed.
    assert not server.crawled("quick", "victim")
    assert server.items("quick", "victim") == []


@pytest.mark.parametrize("stage", ["deliver", "spawn"])
def test_cancel_after_dequeue_before_process_start(server, stage):
    """The job left the spider queue, but its crawl process did not start when it is cancelled."""
    server.hold(stage, "victim")
    server.schedule("quick", "victim")
    server.wait_held(stage, "victim")

    # While in flight, the job is reported as pending: it is scheduled to run, so it must not be absent from every
    # list, and a cancellation must find it.
    assert server.states("victim") == ["pending"]
    assert server.status("victim") == "pending"
    assert not server.crawled("quick", "victim")

    assert server.cancel("victim")["prevstate"] == "pending"

    server.release("victim")
    server.wait_idle()

    # Before the fix, the job was absent from every list at this point, the cancellation answered with a null
    # prevstate, and the job then spawned and crawled anyway.
    assert_gone(server, "victim")
    assert not server.crawled("quick", "victim"), "a cancelled job must not start its crawl process"
    assert server.items("quick", "victim") == []


def test_cancel_while_running(server):
    """The crawl process started and is crawling; the cancellation signals it, and its partial output is kept."""
    server.schedule("slow", "victim")
    server.wait_for(lambda: server.status("victim") == "running", message="the victim to run")
    server.wait_for(lambda: "Spider opened" in server.log("slow", "victim"), message="the victim to crawl")

    assert server.cancel("victim")["prevstate"] == "running"

    # The crawl started, and was interrupted by a signal instead of finishing on its own.
    items = assert_finished(server, "victim", "slow", log="Received SIGINT")
    assert "Spider closed (finished)" not in server.log("slow", "victim")
    assert len(items) < 80, "the crawl must have been interrupted, not completed"

    server.wait_idle()


def test_cancel_after_process_ended_before_finish_callback(server):
    """
    The crawl process exited, but its finishing callback had not run. Cancelling signals a process that no longer
    exists: the request must stay a no-op success instead of failing with ProcessExitedAlready.
    """
    server.hold("finish", "victim")
    server.schedule("quick", "victim")
    server.wait_held("finish", "victim")

    # The finishing callback did not run yet, so the process is still reported as running, though its crawl is over.
    assert assert_one_state(server, "victim") == ["running"]
    assert server.crawled("quick", "victim")
    items = server.items("quick", "victim")

    assert server.cancel("victim")["prevstate"] == "running"
    # The cancel signal reached a process that no longer exists, but the process is still reported as running until
    # its finishing callback runs.
    assert server.states("victim") == ["running"]
    assert "Unhandled Error" not in server.log_file.read_text()

    server.release("victim")
    server.wait_idle()

    # The crawl completed before the cancellation was accepted, so it is finished with its complete output.
    assert assert_finished(server, "victim", "quick", items=len(items), log="Spider closed (finished)") == items


@pytest.mark.parametrize("stage", ["deliver", "spawn"])
def test_repeated_cancel_then_restart_does_not_recrawl(server, stage):
    """
    Cancel a job that never started, repeatedly, then restart Scrapyd. The job must not silently crawl again, and must
    not present contradictory terminal states. Other jobs remain dispatchable.
    """
    server.hold(stage, "victim")
    server.schedule("quick", "victim")
    server.wait_held(stage, "victim")
    assert server.cancel("victim")["prevstate"] == "pending"
    server.release("victim")
    server.wait_idle()

    for _ in range(3):
        assert_gone(server, "victim")

    server.restart()

    # The cancellation survived the restart: the job is reported nowhere, and is never redispatched.
    assert_gone(server, "victim")
    assert not server.crawled("quick", "victim")
    assert server.items("quick", "victim") == []
    server.wait_idle()

    server.schedule("quick", "healthy")
    server.wait_for(lambda: server.states("healthy") == ["finished"], message="the healthy job to finish")
    assert_finished(server, "healthy", "quick", items=5)
    assert_gone(server, "victim")


def test_repeated_cancel_running_then_restart(server):
    """
    Cancel a running job repeatedly, then restart Scrapyd. It is finished once, with one set of records, and is not
    crawled again.
    """
    server.schedule("slow", "victim")
    server.wait_for(lambda: server.status("victim") == "running", message="the victim to run")
    server.wait_for(lambda: "Spider opened" in server.log("slow", "victim"), message="the victim to crawl")

    assert server.cancel("victim")["prevstate"] == "running"
    server.wait_idle()
    items = assert_finished(server, "victim", "slow", log="Received SIGINT")

    for _ in range(3):
        assert server.cancel("victim")["prevstate"] is None
        assert server.states("victim") == ["finished"]

    log = server.log("slow", "victim")
    server.restart()

    assert assert_one_state(server, "victim") == ["finished"]
    assert server.status("victim") == "finished"
    assert server.cancel("victim")["prevstate"] is None
    assert server.items("slow", "victim") == items
    assert server.log("slow", "victim") == log

    server.schedule("quick", "healthy")
    server.wait_for(lambda: server.states("healthy") == ["finished"], message="the healthy job to finish")
    assert_finished(server, "healthy", "quick", items=5)


def test_cancel_does_not_disturb_other_jobs(server):
    """Cancel a job in the in-flight window while another job is queued behind it: the other job still runs."""
    server.hold("deliver", "victim")
    server.schedule("quick", "victim")
    server.wait_held("deliver", "victim")
    server.schedule("quick", "survivor")

    assert server.cancel("victim")["prevstate"] == "pending"
    server.release("victim")

    server.wait_for(lambda: server.states("survivor") == ["finished"], message="the survivor to finish")
    assert_finished(server, "survivor", "quick", items=5)
    assert "Spider closed (finished)" in server.log("quick", "survivor")

    assert_gone(server, "victim")
    assert not server.crawled("quick", "victim")


def test_cancel_running_job_frees_slot_for_queued_job(server):
    """Cancelling a running job frees the only slot, and the queued job is then dispatched normally."""
    server.schedule("slow", "victim")
    server.wait_for(lambda: server.status("victim") == "running", message="the victim to run")
    server.wait_for(lambda: "Spider opened" in server.log("slow", "victim"), message="the victim to crawl")
    server.schedule("quick", "queued")
    server.wait_for(lambda: server.states("queued") == ["pending"], message="the queued job to be pending")

    assert server.cancel("victim")["prevstate"] == "running"

    server.wait_for(lambda: server.states("queued") == ["finished"], message="the queued job to finish")
    assert_finished(server, "queued", "quick", items=5)

    assert_finished(server, "victim", "slow", log="Received SIGINT")
