import datetime
import sqlite3

import pytest

from scrapyd.sqlite import JsonSqlitePriorityQueue, SqliteFinishedJobs, SqliteSequence
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

    # Equal priorities are popped in the order in which they were added.
    assert jsonsqlitepriorityqueue.pop() == msg1
    assert jsonsqlitepriorityqueue.pop() == msg2
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


def test_jsonsqlitepriorityqueue_fifo_same_priority(jsonsqlitepriorityqueue):
    messages = [{"name": f"s{i}", "_job": f"j{i}"} for i in range(5)]
    for message in messages:
        assert jsonsqlitepriorityqueue.put(message) is True

    jsonsqlitepriorityqueue.put({"name": "late", "_job": "j-late"}, priority=1.0)

    # Higher priorities come first; equal priorities keep the order in which the messages were added.
    assert list(jsonsqlitepriorityqueue) == [
        ({"name": "late", "_job": "j-late"}, 1.0),
        *[(message, 0.0) for message in messages],
    ]


def test_jsonsqlitepriorityqueue_sequence(jsonsqlitepriorityqueue):
    # A sequence supplied by the caller orders messages of equal priority, regardless of insertion and row ID order.
    jsonsqlitepriorityqueue.put({"name": "a", "_job": "j2"}, sequence=20)
    jsonsqlitepriorityqueue.put({"name": "b", "_job": "j1"}, sequence=10)
    jsonsqlitepriorityqueue.put({"name": "c", "_job": "j3"}, sequence=30)

    assert [message["_job"] for message, _ in jsonsqlitepriorityqueue] == ["j1", "j2", "j3"]
    assert jsonsqlitepriorityqueue.pop()["_job"] == "j1"
    assert jsonsqlitepriorityqueue.pop()["_job"] == "j2"
    assert jsonsqlitepriorityqueue.pop()["_job"] == "j3"


def test_jsonsqlitepriorityqueue_duplicate_job(jsonsqlitepriorityqueue):
    assert jsonsqlitepriorityqueue.put({"name": "s1", "_job": "j1"}, priority=1.0) is True
    assert jsonsqlitepriorityqueue.put({"name": "s2", "_job": "j2"}, priority=1.0) is True

    # The duplicate keeps its position among messages of equal priority, and only its priority is updated.
    assert jsonsqlitepriorityqueue.put({"name": "s1", "_job": "j1"}, priority=5.0) is False

    assert len(jsonsqlitepriorityqueue) == 2
    assert jsonsqlitepriorityqueue.pop()["_job"] == "j1"
    assert jsonsqlitepriorityqueue.pop()["_job"] == "j2"


def test_jsonsqlitepriorityqueue_peek(jsonsqlitepriorityqueue):
    assert jsonsqlitepriorityqueue.peek() is None

    jsonsqlitepriorityqueue.put({"name": "s1", "_job": "j1"}, sequence=7)
    jsonsqlitepriorityqueue.put({"name": "s2", "_job": "j2"}, sequence=8)

    # The dispatch key is (-priority, sequence).
    assert jsonsqlitepriorityqueue.peek() == (0.0, 7)

    jsonsqlitepriorityqueue.put({"name": "s3", "_job": "j3"}, priority=2.0, sequence=9)

    assert jsonsqlitepriorityqueue.peek() == (-2.0, 9)
    assert jsonsqlitepriorityqueue.pending() == [
        ({"name": "s3", "_job": "j3"}, (-2.0, 9)),
        ({"name": "s1", "_job": "j1"}, (0.0, 7)),
        ({"name": "s2", "_job": "j2"}, (0.0, 8)),
    ]


def test_jsonsqlitepriorityqueue_reopen(tmpdir):
    # Messages are ordered after a restart, because the order is stored in the database.
    database = str(tmpdir / "mybot.db")

    q = JsonSqlitePriorityQueue(database)
    q.put({"name": "s1", "_job": "j1"})
    q.put({"name": "s2", "_job": "j2"})

    assert q.pop()["_job"] == "j1"

    q = JsonSqlitePriorityQueue(database)
    q.put({"name": "s4", "_job": "j4"})
    q.put({"name": "s5", "_job": "j5"})

    # New messages are ordered after the messages that were already pending, and higher priorities still come first.
    assert [message["_job"] for message, _ in q] == ["j2", "j4", "j5"]

    q.put({"name": "s6", "_job": "j6"}, priority=1.0)

    assert [message["_job"] for message, _ in q] == ["j6", "j2", "j4", "j5"]


def test_jsonsqlitepriorityqueue_migrate(tmpdir):
    # A queue created by an older Scrapyd version has no sequence or job columns.
    database = str(tmpdir / "mybot.db")
    conn = sqlite3.connect(database)
    conn.execute("CREATE TABLE queue (id integer PRIMARY KEY, priority real key, message blob)")
    for _id, message in enumerate(['{"name": "s1", "_job": "j1"}', '{"name": "s1", "_job": "j1"}']):
        conn.execute("INSERT INTO queue (priority, message) VALUES (?, ?)", (0.0, message.encode()))
    conn.execute("INSERT INTO queue (priority, message) VALUES (?, ?)", (0.0, b'{"name": "s2", "_job": "j2"}'))
    conn.commit()
    conn.close()

    q = JsonSqlitePriorityQueue(database)

    # The duplicate job is removed, and the older messages keep their order.
    assert [message["_job"] for message, _ in q] == ["j1", "j2"]

    q.put({"name": "s3", "_job": "j3"})

    assert [message["_job"] for message, _ in q] == ["j1", "j2", "j3"]


def test_sqlitesequence(tmpdir):
    database = str(tmpdir / "sequence.db")
    sequence = SqliteSequence(database)

    assert [sequence.next() for _ in range(3)] == [1, 2, 3]

    # The sequence continues after a restart.
    sequence = SqliteSequence(database)

    assert sequence.next() == 4
