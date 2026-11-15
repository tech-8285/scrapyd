import datetime
import sqlite3

import pytest

from scrapyd.sqlite import JsonSqlitePriorityQueue, SqliteFinishedJobs
from tests import get_finished_job


@pytest.fixture
def jsonsqlitepriorityqueue():
    return JsonSqlitePriorityQueue()


@pytest.fixture
def sqlitefinishedjobs():
    q = SqliteFinishedJobs(":memory:")
    q.add(get_finished_job("p1", "s1", "j1", end_time=datetime.datetime(2001, 2, 3, 4, 5, 6, 7)))
    q.add(get_finished_job("p2", "s2", "j2", end_time=datetime.datetime(2001, 2, 3, 4, 5, 6, 8)))
    q.add(get_finished_job("p3", "s3", "j3", end_time=datetime.datetime(2001, 2, 3, 4, 5, 6, 9)))
    return q


def test_jsonsqlitepriorityqueue_empty(jsonsqlitepriorityqueue):
    assert jsonsqlitepriorityqueue.pop() is None


def test_jsonsqlitepriorityqueue_one(jsonsqlitepriorityqueue):
    msg = "a message"
    jsonsqlitepriorityqueue.put(msg)

    assert "_id" not in msg
    assert jsonsqlitepriorityqueue.pop() == msg
    assert jsonsqlitepriorityqueue.pop() is None


def test_jsonsqlitepriorityqueue_multiple(jsonsqlitepriorityqueue):
    msg1 = "first message"
    msg2 = "second message"
    jsonsqlitepriorityqueue.put(msg1)
    jsonsqlitepriorityqueue.put(msg2)
    out = []
    out.append(jsonsqlitepriorityqueue.pop())
    out.append(jsonsqlitepriorityqueue.pop())

    assert msg1 in out
    assert msg2 in out
    assert jsonsqlitepriorityqueue.pop() is None


def test_jsonsqlitepriorityqueue_priority(jsonsqlitepriorityqueue):
    msg1 = "message 1"
    msg2 = "message 2"
    msg3 = "message 3"
    msg4 = "message 4"
    jsonsqlitepriorityqueue.put(msg1, priority=1.0)
    jsonsqlitepriorityqueue.put(msg2, priority=5.0)
    jsonsqlitepriorityqueue.put(msg3, priority=3.0)
    jsonsqlitepriorityqueue.put(msg4, priority=2.0)

    assert jsonsqlitepriorityqueue.pop() == msg2
    assert jsonsqlitepriorityqueue.pop() == msg3
    assert jsonsqlitepriorityqueue.pop() == msg4
    assert jsonsqlitepriorityqueue.pop() == msg1


def test_jsonsqlitepriorityqueue_iter_len_clear(jsonsqlitepriorityqueue):
    assert len(jsonsqlitepriorityqueue) == 0
    assert list(jsonsqlitepriorityqueue) == []

    msg1 = "message 1"
    msg2 = "message 2"
    msg3 = "message 3"
    msg4 = "message 4"
    jsonsqlitepriorityqueue.put(msg1, priority=1.0)
    jsonsqlitepriorityqueue.put(msg2, priority=5.0)
    jsonsqlitepriorityqueue.put(msg3, priority=3.0)
    jsonsqlitepriorityqueue.put(msg4, priority=2.0)

    assert len(jsonsqlitepriorityqueue) == 4
    assert list(jsonsqlitepriorityqueue) == [(msg2, 5.0), (msg3, 3.0), (msg4, 2.0), (msg1, 1.0)]

    jsonsqlitepriorityqueue.clear()

    assert len(jsonsqlitepriorityqueue) == 0
    assert list(jsonsqlitepriorityqueue) == []


def test_jsonsqlitepriorityqueue_remove(jsonsqlitepriorityqueue):
    assert len(jsonsqlitepriorityqueue) == 0
    assert list(jsonsqlitepriorityqueue) == []

    msg1 = "good message 1"
    msg2 = "bad message 2"
    msg3 = "good message 3"
    msg4 = "bad message 4"
    jsonsqlitepriorityqueue.put(msg1)
    jsonsqlitepriorityqueue.put(msg2)
    jsonsqlitepriorityqueue.put(msg3)
    jsonsqlitepriorityqueue.put(msg4)
    jsonsqlitepriorityqueue.remove(lambda x: x.startswith("bad"))

    assert list(jsonsqlitepriorityqueue) == [(msg1, 0.0), (msg3, 0.0)]


@pytest.mark.parametrize(
    "value",
    [
        "native ascii str",
        "\xa3",
        123,
        1.2,
        True,
        ["a", "list", 1],
        {"a": "dict"},
    ],
)
def test_jsonsqlitepriorityqueue_types(jsonsqlitepriorityqueue, value):
    jsonsqlitepriorityqueue.put(value)

    assert jsonsqlitepriorityqueue.pop() == value


def test_jsonsqlitepriorityqueue_equal_priority_fifo(jsonsqlitepriorityqueue):
    messages = [{"name": "spider", "_job": f"j{i}"} for i in range(5)]
    for message in messages:
        jsonsqlitepriorityqueue.put(message)

    assert [jsonsqlitepriorityqueue.pop() for _ in messages] == messages


def test_jsonsqlitepriorityqueue_equal_priority_fifo_after_reopen(tmp_path):
    database = str(tmp_path / "queue.db")
    messages = [{"name": "spider", "_job": f"j{i}"} for i in range(5)]

    queue = JsonSqlitePriorityQueue(database)
    for message in messages:
        queue.put(message)

    # An ordering that isn't persisted would be at the mercy of the query plan, or of the insertion IDs of a rewritten
    # table. Re-open the database, like a restart would.
    queue = JsonSqlitePriorityQueue(database)

    assert list(queue) == [(message, 0.0) for message in messages]
    assert [queue.pop() for _ in messages] == messages


def test_jsonsqlitepriorityqueue_priority_is_global_and_stable(jsonsqlitepriorityqueue):
    messages = [{"name": "spider", "_job": f"j{i}"} for i in range(6)]
    for message, priority in zip(messages, [0, 5, 0, 5, 5, 0], strict=True):
        jsonsqlitepriorityqueue.put(message, priority=priority)

    # Descending priority, then ascending acceptance time.
    assert [message["_job"] for message, _ in jsonsqlitepriorityqueue] == ["j1", "j3", "j4", "j0", "j2", "j5"]


def test_jsonsqlitepriorityqueue_put_pending_job_in_place(jsonsqlitepriorityqueue):
    jsonsqlitepriorityqueue.put({"name": "spider1", "_job": "j1"})
    jsonsqlitepriorityqueue.put({"name": "spider2", "_job": "j2"})
    jsonsqlitepriorityqueue.put({"name": "spider3", "_job": "j3"})

    # Repeating the request updates the pending job, and it keeps the position that it was first given.
    jsonsqlitepriorityqueue.put({"name": "spider1", "_job": "j1", "arg": "new"})

    assert len(jsonsqlitepriorityqueue) == 3
    assert [message["_job"] for message, _ in jsonsqlitepriorityqueue] == ["j1", "j2", "j3"]
    assert jsonsqlitepriorityqueue.pop() == {"name": "spider1", "_job": "j1", "arg": "new"}

    # A new priority moves the job, but it keeps its acceptance time among peers of the new priority.
    jsonsqlitepriorityqueue.put({"name": "spider3", "_job": "j3"}, priority=5.0)
    jsonsqlitepriorityqueue.put({"name": "spider4", "_job": "j4"}, priority=5.0)

    assert [message["_job"] for message, _ in jsonsqlitepriorityqueue] == ["j3", "j4", "j2"]


def test_jsonsqlitepriorityqueue_put_in_place_after_reopen(tmp_path):
    database = str(tmp_path / "queue.db")
    queue = JsonSqlitePriorityQueue(database)
    queue.put({"name": "spider", "_job": "j1"})
    queue.put({"name": "spider", "_job": "j2"})

    queue = JsonSqlitePriorityQueue(database)
    queue.put({"name": "spider", "_job": "j1"}, priority=5.0)

    assert [message["_job"] for message, _ in queue] == ["j1", "j2"]
    assert [queue.pop()["_job"], queue.pop()["_job"]] == ["j1", "j2"]


def test_jsonsqlitepriorityqueue_put_same_job_twice_dispatches_once(jsonsqlitepriorityqueue):
    message = {"name": "spider", "_job": "j1"}
    jsonsqlitepriorityqueue.put(message)
    jsonsqlitepriorityqueue.put(message)

    assert len(jsonsqlitepriorityqueue) == 1
    assert jsonsqlitepriorityqueue.pop() == message
    assert jsonsqlitepriorityqueue.pop() is None


def test_jsonsqlitepriorityqueue_put_after_pop(jsonsqlitepriorityqueue):
    message = {"name": "spider", "_job": "j1"}
    jsonsqlitepriorityqueue.put(message)
    assert jsonsqlitepriorityqueue.pop() == message

    # A job that is no longer pending can be scheduled again.
    jsonsqlitepriorityqueue.put(message)
    assert len(jsonsqlitepriorityqueue) == 1
    assert jsonsqlitepriorityqueue.pop() == message


@pytest.mark.parametrize("message", [{"name": "spider"}, {"name": "spider", "_job": None}, "no job id"])
def test_jsonsqlitepriorityqueue_put_without_job_id(jsonsqlitepriorityqueue, message):
    jsonsqlitepriorityqueue.put(message)
    jsonsqlitepriorityqueue.put(message)

    # Without a job ID, the queue can't know whether two messages are the same job.
    assert len(jsonsqlitepriorityqueue) == 2


def test_jsonsqlitepriorityqueue_head(jsonsqlitepriorityqueue):
    assert jsonsqlitepriorityqueue.head() is None

    jsonsqlitepriorityqueue.put({"name": "spider", "_job": "j1"})
    jsonsqlitepriorityqueue.put({"name": "spider", "_job": "j2"}, priority=5.0)

    priority, accepted_at = jsonsqlitepriorityqueue.head()

    assert priority == 5.0
    # The head is not removed.
    assert len(jsonsqlitepriorityqueue) == 2
    # The head is the job that pop() returns.
    assert jsonsqlitepriorityqueue.pop() == {"name": "spider", "_job": "j2"}
    # The next head is the job that was accepted first.
    priority, next_accepted_at = jsonsqlitepriorityqueue.head()
    assert priority == 0.0
    assert next_accepted_at < accepted_at


def test_jsonsqlitepriorityqueue_list_matches_pop_order(jsonsqlitepriorityqueue):
    messages = [{"name": "spider", "_job": f"j{i}"} for i in range(4)]
    for message, priority in zip(messages, [0, 3, 3, 0], strict=True):
        jsonsqlitepriorityqueue.put(message, priority=priority)

    listed = [message for message, _ in jsonsqlitepriorityqueue]
    popped = [jsonsqlitepriorityqueue.pop() for _ in messages]

    assert listed == popped


def test_jsonsqlitepriorityqueue_migrate_legacy_table(tmp_path):
    database = str(tmp_path / "queue.db")

    # Create the table as an older version of Scrapyd created it, including a duplicated pending job.
    conn = sqlite3.connect(database)
    conn.execute("CREATE TABLE spider_queue (id integer PRIMARY KEY, priority real key, message blob)")
    legacy = [
        (0.0, '{"name": "spider1", "_job": "j1"}'),
        (0.0, '{"name": "spider2", "_job": "j2"}'),
        (0.0, '{"name": "spider1", "_job": "j1"}'),
        (5.0, '{"name": "spider3", "_job": "j3"}'),
    ]
    for priority, message in legacy:
        conn.execute(
            "INSERT INTO spider_queue (priority, message) VALUES (?, ?)",
            (priority, sqlite3.Binary(message.encode("ascii"))),
        )
    conn.commit()
    conn.close()

    queue = JsonSqlitePriorityQueue(database, "spider_queue")

    # The duplicated pending job is collapsed to its first occurrence, and the legacy insertion order is kept.
    assert [message["_job"] for message, _ in queue] == ["j3", "j1", "j2"]
    assert [queue.pop()["_job"], queue.pop()["_job"], queue.pop()["_job"]] == ["j3", "j1", "j2"]
    assert queue.pop() is None

    # The migration is idempotent, and the new job can't be added twice.
    queue = JsonSqlitePriorityQueue(database, "spider_queue")
    queue.put({"name": "spider4", "_job": "j4"})
    queue.put({"name": "spider4", "_job": "j4"})

    assert len(queue) == 1


def test_jsonsqlitepriorityqueue_migrate_twice(tmp_path):
    database = str(tmp_path / "queue.db")

    queue = JsonSqlitePriorityQueue(database)
    queue.put({"name": "spider", "_job": "j1"})

    # A second instance must not disturb the existing rows.
    queue = JsonSqlitePriorityQueue(database)

    assert [message["_job"] for message, _ in queue] == ["j1"]


def test_sqlitefinishedjobs_add(sqlitefinishedjobs):
    assert len(sqlitefinishedjobs) == 3


def test_sqlitefinishedjobs_clear_all(sqlitefinishedjobs):
    sqlitefinishedjobs.clear()

    assert len(sqlitefinishedjobs) == 0


def test_sqlitefinishedjobs_clear_keep_0(sqlitefinishedjobs):
    sqlitefinishedjobs.clear(finished_to_keep=0)

    assert len(sqlitefinishedjobs) == 0


def test_sqlitefinishedjobs_clear_keep_2(sqlitefinishedjobs):
    sqlitefinishedjobs.clear(finished_to_keep=2)

    assert len(sqlitefinishedjobs) == 2


def test_sqlitefinishedjobs__iter__(sqlitefinishedjobs):
    actual = list(sqlitefinishedjobs)

    assert (actual[0][0], actual[0][1]) == ("p3", "s3")
    assert (actual[1][0], actual[1][1]) == ("p2", "s2")
    assert (actual[2][0], actual[2][1]) == ("p1", "s1")
