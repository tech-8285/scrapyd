from pathlib import Path

import pytest
from zope.interface.verify import verifyObject

from scrapyd.config import Config
from scrapyd.interfaces import ISpiderScheduler
from scrapyd.scheduler import SpiderScheduler
from scrapyd.utils import get_spider_queues


@pytest.fixture
def scheduler(tmpdir):
    eggs_dir = Path(tmpdir) / "eggs"
    dbs_dir = Path(tmpdir) / "dbs"
    config = Config(values={"eggs_dir": str(eggs_dir), "dbs_dir": str(dbs_dir)})
    (eggs_dir / "mybot1").mkdir(parents=True)
    (eggs_dir / "mybot2").mkdir(parents=True)
    return SpiderScheduler(config)


def test_interface(scheduler):
    verifyObject(ISpiderScheduler, scheduler)


# Need sorted(), because os.listdir() in FilesystemEggStorage.list_projects() uses an arbitrary order.
def test_list_projects_update_projects(scheduler):
    assert sorted(scheduler.list_projects()) == ["mybot1", "mybot2"]

    (Path(scheduler.config.get("eggs_dir")) / "settings_raise").mkdir(parents=True)

    assert sorted(scheduler.list_projects()) == ["mybot1", "mybot2"]

    scheduler.update_projects()

    assert sorted(scheduler.list_projects()) == ["mybot1", "mybot2", "settings_raise"]


def test_schedule(scheduler):
    queues = get_spider_queues(scheduler.config)
    mybot1_queue = queues["mybot1"]
    mybot2_queue = queues["mybot2"]

    assert not mybot1_queue.count()
    assert not mybot2_queue.count()

    scheduler.schedule("mybot1", "myspider1", 2, a="b")
    scheduler.schedule("mybot2", "myspider2", 1, c="d")
    scheduler.schedule("mybot2", "myspider3", 10, e="f")

    assert mybot1_queue.pop() == {"name": "myspider1", "a": "b"}
    assert mybot2_queue.pop() == {"name": "myspider3", "e": "f"}
    assert mybot2_queue.pop() == {"name": "myspider2", "c": "d"}


def test_schedule_order_is_persistent(scheduler):
    scheduler.schedule("mybot1", "myspider1", 0, _job="j1")
    scheduler.schedule("mybot2", "myspider2", 0, _job="j2")

    # A restart creates a new scheduler and new queues, which read the pending jobs from the databases.
    scheduler = SpiderScheduler(scheduler.config)
    queues = get_spider_queues(scheduler.config)

    scheduler.schedule("mybot2", "myspider3", 0, _job="j3")

    assert queues["mybot1"].list() == [{"name": "myspider1", "_job": "j1"}]
    assert queues["mybot2"].list() == [
        {"name": "myspider2", "_job": "j2"},
        {"name": "myspider3", "_job": "j3"},
    ]


def test_schedule_priority_update(scheduler):
    queues = get_spider_queues(scheduler.config)

    assert scheduler.schedule("mybot1", "myspider1", 0, _job="j1") is True
    scheduler.schedule("mybot1", "myspider2", 0, _job="j2")

    # Scheduling a pending job again updates its priority, without changing its place among jobs of equal priority.
    assert scheduler.schedule("mybot1", "myspider1", 5, _job="j1") is False

    assert queues["mybot1"].list() == [
        {"name": "myspider1", "_job": "j1"},
        {"name": "myspider2", "_job": "j2"},
    ]

    scheduler.schedule("mybot1", "myspider3", 0, _job="j3")

    assert queues["mybot1"].pop()["_job"] == "j1"
    assert queues["mybot1"].pop()["_job"] == "j2"
    assert queues["mybot1"].pop()["_job"] == "j3"
