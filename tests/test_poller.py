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


class PausedPoller(QueuePoller):
    """A poller whose delivery of a dequeued message waits, so that a test can act while the message is in flight."""

    def __init__(self, config):
        super().__init__(config)
        self.paused = Deferred()

    def _deliver(self, project, message):
        return self.paused.addCallback(lambda _: QueuePoller._deliver(self, project, message))  # noqa: SLF001


@pytest.fixture
def paused_poller(tmpdir):
    eggs_dir = Path(tmpdir) / "eggs"
    dbs_dir = Path(tmpdir) / "dbs"
    config = Config(values={"eggs_dir": str(eggs_dir), "dbs_dir": str(dbs_dir)})
    (eggs_dir / "mybot1").mkdir(parents=True)
    return PausedPoller(config)


def test_poll_reports_the_message_until_a_launcher_slot_takes_it(poller):
    poller.queues["mybot1"].add("spider1", _job="j1")

    deferred = poller.next()
    value = poller.poll()

    # The message was handed to a launcher slot's Deferred, but the launcher did not take it yet: the window in which
    # it is neither pending nor running extends until the launcher calls take_launched().
    assert value.called
    assert deferred.result == {"_project": "mybot1", "_spider": "spider1", "_job": "j1"}
    assert poller.launching_messages() == [("mybot1", {"name": "spider1", "_job": "j1"})]

    assert poller.take_launched("mybot1", "j1") is False
    assert poller.launching == {}
    assert poller.launching_messages() == []


def test_in_flight_message_is_reported_until_taken(paused_poller):
    paused_poller.queues["mybot1"].add("spider1", _job="j1")
    scheduled = paused_poller.next()

    value = paused_poller.poll()

    # The message left the queue, but was not handed to a launcher slot: it is neither pending nor running, but it
    # must still be reported (as a message that will start), and it can still be cancelled.
    assert not value.called
    assert not scheduled.called
    assert paused_poller.queues["mybot1"].count() == 0
    assert paused_poller.launching_messages() == [("mybot1", {"name": "spider1", "_job": "j1"})]

    # A launcher slot taking the message ends the window.
    assert paused_poller.take_launched("mybot1", "j1") is False
    assert paused_poller.launching == {}


def test_cancel_launching_prevents_delivery(paused_poller):
    paused_poller.queues["mybot1"].add("spider1", _job="j1")
    scheduled = paused_poller.next()
    paused_poller.poll()

    assert paused_poller.cancel_launching("mybot1", "j1") is True
    # The message is no longer reported, and can't be cancelled twice.
    assert paused_poller.launching_messages() == []
    assert paused_poller.cancel_launching("mybot1", "j1") is False

    # When delivery would have completed, the cancellation wins: no launcher slot ever receives the message.
    paused_poller.paused.callback(None)
    assert not scheduled.called
    assert paused_poller.queues["mybot1"].count() == 0


def test_take_launched_reports_a_cancellation_that_arrived_first(paused_poller):
    paused_poller.queues["mybot1"].add("spider1", _job="j1")
    paused_poller.next()
    paused_poller.poll()

    # The launcher took the message, and is about to spawn the process...
    assert paused_poller.take_launched("mybot1", "j1") is False
    # ... but a cancellation cannot find it anymore, and reports that there is nothing to cancel.
    assert paused_poller.cancel_launching("mybot1", "j1") is False
    paused_poller.paused.callback(None)
    assert paused_poller.launching == {}


def test_in_flight_message_does_not_hide_pending_messages(paused_poller):
    paused_poller.queues["mybot1"].add("spider1", _job="j1")
    paused_poller.queues["mybot1"].add("spider1", _job="j2")
    paused_poller.next()
    paused_poller.next()

    paused_poller.poll()

    # Only the first message is in flight; the second is still pending in the queue.
    assert [message["_job"] for _, message in paused_poller.launching_messages()] == ["j1"]
    assert [message["_job"] for message in paused_poller.queues["mybot1"].list()] == ["j2"]
    assert paused_poller.cancel_launching("mybot1", "j1") is True
    assert [message["_job"] for message in paused_poller.queues["mybot1"].list()] == ["j2"]
