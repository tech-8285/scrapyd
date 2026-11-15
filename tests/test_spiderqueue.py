import pytest
from twisted.internet.defer import inlineCallbacks, maybeDeferred
from zope.interface.verify import verifyObject

from scrapyd.config import Config
from scrapyd.interfaces import ISpiderQueue
from scrapyd.spiderqueue import SqliteSpiderQueue

spider_args = {
    "arg1": "val1",
    "arg2": 2,
    "arg3": "\N{SNOWMAN}",
}
expected = spider_args.copy()
expected["name"] = "spider1"


@pytest.fixture
def spiderqueue():
    return SqliteSpiderQueue(Config(values={"dbs_dir": ":memory:"}), "mybot")


def test_interface(spiderqueue):
    verifyObject(ISpiderQueue, spiderqueue)


@inlineCallbacks
def test_pop(spiderqueue):
    yield maybeDeferred(spiderqueue.add, "spider0", 5)
    yield maybeDeferred(spiderqueue.add, "spider1", 10, **spider_args)
    yield maybeDeferred(spiderqueue.add, "spider1", 0)

    assert (yield maybeDeferred(spiderqueue.count)) == 3

    assert (yield maybeDeferred(spiderqueue.pop)) == expected

    assert (yield maybeDeferred(spiderqueue.count)) == 2


@inlineCallbacks
def test_list(spiderqueue):
    assert (yield maybeDeferred(spiderqueue.list)) == []

    yield maybeDeferred(spiderqueue.add, "spider1", 10, **spider_args)
    yield maybeDeferred(spiderqueue.add, "spider1", 10, **spider_args)

    assert (yield maybeDeferred(spiderqueue.list)) == [expected, expected]


@inlineCallbacks
def test_remove(spiderqueue):
    yield maybeDeferred(spiderqueue.add, "spider0", 5)
    yield maybeDeferred(spiderqueue.add, "spider1", 10, **spider_args)
    yield maybeDeferred(spiderqueue.add, "spider1", 0)

    assert (yield maybeDeferred(spiderqueue.count)) == 3

    assert (yield maybeDeferred(spiderqueue.remove, lambda message: message["name"] == "spider1")) == 2

    assert (yield maybeDeferred(spiderqueue.count)) == 1


@inlineCallbacks
def test_clear(spiderqueue):
    assert (yield maybeDeferred(spiderqueue.count)) == 0

    yield maybeDeferred(spiderqueue.add, "spider1", 10, **spider_args)
    yield maybeDeferred(spiderqueue.add, "spider1", 10, **spider_args)

    assert (yield maybeDeferred(spiderqueue.count)) == 2

    yield maybeDeferred(spiderqueue.clear)

    assert (yield maybeDeferred(spiderqueue.count)) == 0


@inlineCallbacks
def test_add_same_job_in_place(spiderqueue):
    yield maybeDeferred(spiderqueue.add, "spider1", 0, _job="j1", arg1="old")
    yield maybeDeferred(spiderqueue.add, "spider2", 0, _job="j2")

    # Repeating a schedule request must not dispatch the same job twice.
    yield maybeDeferred(spiderqueue.add, "spider1", 2, _job="j1", arg1="new")

    assert (yield maybeDeferred(spiderqueue.count)) == 2
    # The higher priority is applied, but the job keeps the position it was first given.
    assert (yield maybeDeferred(spiderqueue.list)) == [
        {"name": "spider1", "_job": "j1", "arg1": "new"},
        {"name": "spider2", "_job": "j2"},
    ]
    priority, _ = yield maybeDeferred(spiderqueue.head)
    assert priority == 2


@inlineCallbacks
def test_add_without_job(spiderqueue):
    yield maybeDeferred(spiderqueue.add, "spider1")
    yield maybeDeferred(spiderqueue.add, "spider1")

    # Without a job ID, the queue can't know whether the two messages are the same job.
    assert (yield maybeDeferred(spiderqueue.count)) == 2


@inlineCallbacks
def test_head(spiderqueue):
    assert (yield maybeDeferred(spiderqueue.head)) is None

    yield maybeDeferred(spiderqueue.add, "spider0", 5)
    yield maybeDeferred(spiderqueue.add, "spider1", 10, **spider_args)

    priority, accepted_at = yield maybeDeferred(spiderqueue.head)

    assert priority == 10
    assert (yield maybeDeferred(spiderqueue.pop)) == expected

    # The head is not removed, and it is the job that pop() returns.
    priority, next_accepted_at = yield maybeDeferred(spiderqueue.head)
    assert priority == 5
    assert next_accepted_at < accepted_at
    assert (yield maybeDeferred(spiderqueue.count)) == 1
