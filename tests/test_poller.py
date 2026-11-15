from pathlib import Path

import pytest
from twisted.internet.defer import Deferred
from zope.interface.verify import verifyObject

from scrapyd.config import Config
from scrapyd.interfaces import IPoller
from scrapyd.poller import QueuePoller
from scrapyd.utils import get_spider_queues


@pytest.fixture
def poller(tmpdir):
    eggs_dir = Path(tmpdir) / "eggs"
    dbs_dir = Path(tmpdir) / "dbs"
    config = Config(values={"eggs_dir": str(eggs_dir), "dbs_dir": str(dbs_dir)})
    (eggs_dir / "mybot1").mkdir(parents=True)
    (eggs_dir / "mybot2").mkdir(parents=True)
    return QueuePoller(config)


def test_interface(poller):
    verifyObject(IPoller, poller)


# Need sorted(), because os.listdir() in FilesystemEggStorage.list_projects() uses an arbitrary order.
def test_list_projects_update_projects(poller):
    assert sorted(poller.queues) == ["mybot1", "mybot2"]

    (Path(poller.config.get("eggs_dir")) / "settings_raise").mkdir(parents=True)

    assert sorted(poller.queues) == ["mybot1", "mybot2"]

    poller.update_projects()

    assert sorted(poller.queues) == ["mybot1", "mybot2", "settings_raise"]


def test_poll_next(poller):
    queues = get_spider_queues(poller.config)

    scenario = {"mybot1": "spider1", "mybot2": "spider2"}
    for project, spider in scenario.items():
        queues[project].add(spider)

    deferred1 = poller.next()
    deferred2 = poller.next()

    assert isinstance(deferred1, Deferred)
    assert not hasattr(deferred1, "result")
    assert isinstance(deferred2, Deferred)
    assert not hasattr(deferred2, "result")

    value = poller.poll()

    assert isinstance(value, Deferred)
    assert hasattr(value, "result")
    assert getattr(value, "called", False)
    assert value.result is None

    assert hasattr(deferred1, "result")
    assert getattr(deferred1, "called", False)
    assert hasattr(deferred2, "result")
    assert getattr(deferred2, "called", False)

    # os.listdir() in FilesystemEggStorage.list_projects() uses an arbitrary order.
    project_a = deferred1.result["_project"]
    spider_a = scenario.pop(project_a)
    project_b, spider_b = scenario.popitem()

    assert deferred1.result["_spider"] == spider_a
    assert deferred2.result == {"_project": project_b, "_spider": spider_b}


def test_poll_empty(poller):
    value = poller.poll()

    assert isinstance(value, Deferred)
    assert hasattr(value, "result")
    assert getattr(value, "called", False)
    assert value.result is None


def get_dispatched(messages):
    return [(message["_project"], message["_spider"]) for message in messages]


def poll(poller, max_proc):
    """Return the messages that the poller dispatches, given ``max_proc`` free slots."""
    deferreds = [poller.next() for _ in range(max_proc)]
    poller.poll()
    return [deferred.result for deferred in deferreds if deferred.called]


def test_poll_equal_priority_order_across_projects(poller):
    queues = poller.queues

    # Interleave the acceptance of equal-priority jobs in two projects.
    queues["mybot1"].add("first")
    queues["mybot2"].add("second")
    queues["mybot1"].add("third")
    queues["mybot2"].add("fourth")

    messages = poll(poller, 4)

    # Dispatch order follows acceptance order, not project order.
    assert get_dispatched(messages) == [
        ("mybot1", "first"),
        ("mybot2", "second"),
        ("mybot1", "third"),
        ("mybot2", "fourth"),
    ]


def test_poll_backlog_does_not_starve_other_project(poller):
    queues = poller.queues

    # The other project's job is accepted first, and then a project builds a large backlog.
    queues["mybot2"].add("other")
    for i in range(10):
        queues["mybot1"].add(f"spider{i}")

    # Draining one project before another would start "spider0" first, even though "other" was accepted earlier.
    messages = poll(poller, 2)

    assert get_dispatched(messages) == [("mybot2", "other"), ("mybot1", "spider0")]


def test_poll_shared_slots_eventually_run_every_job(poller):
    queues = poller.queues
    queues["mybot1"].add("first")
    queues["mybot2"].add("second")
    for i in range(5):
        queues["mybot1"].add(f"spider{i}")
    queues["mybot2"].add("last")

    # With one slot, all jobs must run, in acceptance order. No job waits forever behind another project's backlog.
    dispatched = []
    while len(dispatched) < 8:
        dispatched.extend(poll(poller, 1))

    assert get_dispatched(dispatched) == [
        ("mybot1", "first"),
        ("mybot2", "second"),
        ("mybot1", "spider0"),
        ("mybot1", "spider1"),
        ("mybot1", "spider2"),
        ("mybot1", "spider3"),
        ("mybot1", "spider4"),
        ("mybot2", "last"),
    ]


def test_poll_priority_before_acceptance_time(poller):
    queues = poller.queues

    queues["mybot1"].add("low", priority=0)
    queues["mybot2"].add("high", priority=10)
    queues["mybot1"].add("medium", priority=5)

    messages = poll(poller, 3)

    assert get_dispatched(messages) == [("mybot2", "high"), ("mybot1", "medium"), ("mybot1", "low")]


def test_poll_respects_capacity(poller):
    for i in range(5):
        poller.queues["mybot1"].add(f"spider{i}")

    deferreds = [poller.next() for _ in range(2)]
    poller.poll()

    assert [deferred.result["_spider"] for deferred in deferreds] == ["spider0", "spider1"]

    # Capacity that isn't backed by a waiting Deferred is not used.
    assert poller.queues["mybot1"].count() == 3


def test_poll_no_capacity(poller):
    poller.queues["mybot1"].add("spider")

    poller.poll()

    # Nothing starts if the launcher hasn't asked for a message.
    assert poller.queues["mybot1"].count() == 1


def test_poll_same_job_twice_dispatches_once(poller):
    queue = poller.queues["mybot1"]
    queue.add("spider", _job="j1")
    queue.add("spider", _job="j1")

    messages = poll(poller, 2)

    assert get_dispatched(messages) == [("mybot1", "spider")]
    assert queue.count() == 0


def test_poll_replays_accepted_order_after_restart(tmp_path):
    eggs_dir = tmp_path / "eggs"
    dbs_dir = tmp_path / "dbs"
    config = Config(values={"eggs_dir": str(eggs_dir), "dbs_dir": str(dbs_dir)})
    for project in ("mybot1", "mybot2"):
        (eggs_dir / project).mkdir(parents=True)

    first = QueuePoller(config)
    first.queues["mybot1"].add("first")
    first.queues["mybot2"].add("second")
    first.queues["mybot1"].add("third")
    first.queues["mybot2"].add("fourth")

    # A new Scrapyd process (a restart) must dispatch the same order, which is rebuilt from the accepted jobs.
    second = QueuePoller(config)

    assert get_dispatched(poll(second, 4)) == [
        ("mybot1", "first"),
        ("mybot2", "second"),
        ("mybot1", "third"),
        ("mybot2", "fourth"),
    ]


def test_poll_priority_update_keeps_position_among_peers(poller):
    queues = poller.queues
    queues["mybot1"].add("first", _job="j1")
    queues["mybot1"].add("second", _job="j2")

    # Update the pending job in place. It keeps the position it was first given among equal-priority jobs.
    queues["mybot1"].add("first", _job="j1", priority=0)

    messages = poll(poller, 2)

    assert get_dispatched(messages) == [("mybot1", "first"), ("mybot1", "second")]


def test_poll_queue_without_head(tmp_path):
    class QueueWithoutHead:
        def __init__(self):
            self.messages = []

        def add(self, name, priority=0.0, **kwargs):
            self.messages.append({"name": name, **kwargs})

        def count(self):
            return len(self.messages)

        def pop(self):
            return self.messages.pop(0)

    eggs_dir = tmp_path / "eggs"
    config = Config(values={"eggs_dir": str(eggs_dir), "dbs_dir": str(tmp_path / "dbs")})
    for project in ("mybot1", "mybot2"):
        (eggs_dir / project).mkdir(parents=True)

    poller = QueuePoller(config)
    poller.queues = {"mybot1": QueueWithoutHead(), "mybot2": QueueWithoutHead()}
    poller.queues["mybot1"].add("first")
    poller.queues["mybot2"].add("second")

    # A custom ISpiderQueue implementation without the head method still starts jobs, in project order.
    messages = poll(poller, 2)

    assert sorted(get_dispatched(messages)) == [("mybot1", "first"), ("mybot2", "second")]


def test_poll_queue_emptied_by_another_instance(tmp_path):
    class RacyQueue:
        """A queue whose pending job can vanish, like when two Scrapyd instances share a database."""

        def __init__(self, racy):
            self.messages = []
            self.racy = racy

        def add(self, name, priority=0.0, **kwargs):
            self.messages.append({"name": name, **kwargs})

        def count(self):
            # The other instance took the job between the count and the pop.
            return 1 if self.messages or self.racy else 0

        def head(self):
            return (0.0, 0.0)

        def pop(self):
            if self.racy:
                # Another instance already removed the row. The row disappears without being returned.
                self.messages.clear()
                return None
            return self.messages.pop(0) if self.messages else None

    eggs_dir = tmp_path / "eggs"
    config = Config(values={"eggs_dir": str(eggs_dir), "dbs_dir": str(tmp_path / "dbs")})
    for project in ("mybot1", "mybot2"):
        (eggs_dir / project).mkdir(parents=True)

    poller = QueuePoller(config)
    poller.queues = {"mybot1": RacyQueue(racy=True), "mybot2": RacyQueue(racy=False)}
    poller.queues["mybot1"].add("taken")
    poller.queues["mybot2"].add("available")

    # A job that another instance already popped is skipped, and the poll continues with the other project instead
    # of failing or looping forever.
    messages = poll(poller, 2)

    assert get_dispatched(messages) == [("mybot2", "available")]
