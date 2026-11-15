from pathlib import Path

import pytest
from twisted.internet.defer import Deferred
from zope.interface import implementer
from zope.interface.verify import verifyObject

from scrapyd.config import Config
from scrapyd.interfaces import IPoller, ISpiderQueue
from scrapyd.poller import QueuePoller
from scrapyd.spiderqueue import SqliteSpiderQueue
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

    # Jobs of equal priority run in the order in which they were accepted, across projects.
    assert deferred1.result == {"_project": "mybot1", "_spider": "spider1"}
    assert deferred2.result == {"_project": "mybot2", "_spider": "spider2"}


def test_poll_priority_across_projects(poller):
    queues = get_spider_queues(poller.config)

    # The first project has a backlog of low-priority jobs. Higher-priority jobs of another project must not wait for
    # the backlog.
    for _ in range(5):
        queues["mybot1"].add("spider1", _job="backlog")
    queues["mybot2"].add("spider2", priority=1.0, _job="priority")

    deferreds = [poller.next() for _ in range(2)]
    poller.poll()

    assert [deferred.result for deferred in deferreds] == [
        {"_project": "mybot2", "_spider": "spider2", "_job": "priority"},
        {"_project": "mybot1", "_spider": "spider1", "_job": "backlog"},
    ]


def test_poll_fifo_equal_priority(poller):
    queues = get_spider_queues(poller.config)

    # The jobs are accepted in an order that doesn't match the order of the projects.
    queues["mybot1"].add("spider1", _job="j1")
    queues["mybot2"].add("spider2", _job="j2")
    queues["mybot2"].add("spider2", _job="j3")

    deferreds = [poller.next() for _ in range(3)]
    poller.poll()

    assert [deferred.result["_job"] for deferred in deferreds] == ["j1", "j2", "j3"]


def test_poll_restart(tmpdir):
    eggs_dir = Path(tmpdir) / "eggs"
    dbs_dir = Path(tmpdir) / "dbs"
    config = Config(values={"eggs_dir": str(eggs_dir), "dbs_dir": str(dbs_dir)})
    for project in ("mybot1", "mybot2"):
        (eggs_dir / project).mkdir(parents=True)

    queues = get_spider_queues(config)
    queues["mybot2"].add("spider2", _job="j2")
    queues["mybot1"].add("spider1", _job="j1")

    # Simulate a restart: a new poller reads the pending jobs from the persisted queues.
    poller = QueuePoller(config)
    deferreds = [poller.next() for _ in range(2)]
    poller.poll()

    assert [deferred.result["_job"] for deferred in deferreds] == ["j2", "j1"]

    queues = get_spider_queues(config)
    queues["mybot1"].add("spider1", _job="j3")

    deferred = poller.next()
    poller.poll()

    # The new job runs after the jobs that were already pending.
    assert deferred.result["_job"] == "j3"


@implementer(ISpiderQueue)
class PeeklessSpiderQueue:
    """A custom spider queue that doesn't implement ``peek()``, like queues written for prior versions."""

    def __init__(self, config, project):
        self.queue = SqliteSpiderQueue(config, project)

    def add(self, name, priority=0.0, **spider_args):
        return self.queue.add(name, priority=priority, **spider_args)

    def pop(self):
        return self.queue.pop()

    def list(self):
        return self.queue.list()

    def count(self):
        return self.queue.count()

    def remove(self, func):
        return self.queue.remove(func)

    def clear(self):
        self.queue.clear()


def test_poll_custom_queue(tmpdir):
    eggs_dir = Path(tmpdir) / "eggs"
    dbs_dir = Path(tmpdir) / "dbs"
    config = Config(values={"eggs_dir": str(eggs_dir), "dbs_dir": str(dbs_dir)})
    for project in ("mybot1", "mybot2"):
        (eggs_dir / project).mkdir(parents=True)

    poller = QueuePoller(config)
    poller.queues = {
        "mybot1": PeeklessSpiderQueue(config, "mybot1"),
        "mybot2": PeeklessSpiderQueue(config, "mybot2"),
    }
    poller.queues["mybot1"].add("spider1", _job="j1")
    poller.queues["mybot2"].add("spider2", _job="j2")

    deferreds = [poller.next(), poller.next()]
    poller.poll()

    # The projects are polled in order, as in prior versions.
    assert [deferred.result["_job"] for deferred in deferreds] == ["j1", "j2"]


def test_poll_empty(poller):
    value = poller.poll()

    assert isinstance(value, Deferred)
    assert hasattr(value, "result")
    assert getattr(value, "called", False)
    assert value.result is None
