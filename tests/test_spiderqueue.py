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


def test_add_returns_whether_added(spiderqueue):
    assert spiderqueue.add("spider1", 0, _job="j1") is True
    _, sequence = spiderqueue.peek()

    # The job keeps its position among jobs of equal priority, and only its priority is updated.
    assert spiderqueue.add("spider1", 5, _job="j1") is False

    assert spiderqueue.count() == 1
    # The dispatch key is (-priority, sequence): a smaller key is dispatched earlier.
    assert spiderqueue.peek() == (-5.0, sequence)


def test_add_orders_equal_priorities(spiderqueue):
    spiderqueue.add("spider1", 0, _job="j1")
    spiderqueue.add("spider2", 0, _job="j2")
    spiderqueue.add("spider3", 1, _job="j3")

    assert spiderqueue.list() == [
        {"name": "spider3", "_job": "j3"},
        {"name": "spider1", "_job": "j1"},
        {"name": "spider2", "_job": "j2"},
    ]


def test_peek_empty(spiderqueue):
    assert spiderqueue.peek() is None


def test_clear_resets_order(spiderqueue):
    spiderqueue.add("spider1", 0, _job="j1")
    spiderqueue.clear()
    spiderqueue.add("spider2", 0, _job="j2")

    assert spiderqueue.list() == [{"name": "spider2", "_job": "j2"}]
