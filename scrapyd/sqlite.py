import datetime
import itertools
import json
import sqlite3
import threading
from pathlib import Path


def _connection_string(config, database):
    dbs_dir = config.get("dbs_dir", "dbs")
    if dbs_dir == ":memory:":
        return dbs_dir

    dbs_path = Path(dbs_dir)
    if not dbs_path.exists():
        dbs_path.mkdir(parents=True, exist_ok=True)
    return str(dbs_path / f"{database}.db")


# The database argument is "jobs" (in SqliteJobStorage), or a project (in SqliteSpiderQueue) from get_spider_queues(),
# which gets projects from get_project_list(), which gets projects from egg storage. We check for directory traversal
# in egg storage, instead.
def initialize(cls, config, database, table, *args):
    return cls(_connection_string(config, database), table, *args)


# When dbs_dir is ":memory:", each queue has its own database, so the queues of a process share a process-wide
# sequence instead. A single instance is used, because each project's queue is created separately.
_memory_sequence = None


def initialize_sequence(config):
    """Return the sequence that orders pending jobs across all projects.

    The sequence is persisted, so that the dispatch order of the pending jobs after a restart is the same as before:
    newly accepted jobs are ordered after the jobs that were already accepted.
    """
    global _memory_sequence  # noqa: PLW0603

    if config.get("dbs_dir", "dbs") == ":memory:":
        if _memory_sequence is None:
            _memory_sequence = MemorySequence()
        return _memory_sequence

    return initialize(SqliteSequence, config, "queue_sequence", "queue_sequence")


class MemorySequence:
    """
    A process-wide sequence, for when pending jobs don't share a persisted sequence.
    """

    def __init__(self, start=1):
        self._counter = itertools.count(start)
        self._lock = threading.Lock()

    def next(self):
        with self._lock:
            return next(self._counter)


# https://docs.python.org/3/library/sqlite3.html#sqlite3-adapter-converter-recipes
def adapt_datetime(val):
    return val.strftime("%Y-%m-%d %H:%M:%S.%f")


def convert_datetime(val):
    return datetime.datetime.strptime(val.decode(), "%Y-%m-%d %H:%M:%S.%f")


sqlite3.register_adapter(datetime.datetime, adapt_datetime)
sqlite3.register_converter("datetime", convert_datetime)


class SqliteMixin:
    def __init__(self, database, table):
        self.database = database or ":memory:"
        self.table = table
        # Regarding check_same_thread, see http://twistedmatrix.com/trac/ticket/4040
        self.conn = sqlite3.connect(self.database, check_same_thread=False)
        # Wait, instead of failing, if another Scrapyd instance is writing to the same database.
        self.conn.execute("PRAGMA busy_timeout = 5000")

    def __len__(self):
        return self.conn.execute(f"SELECT COUNT(*) FROM {self.table}").fetchone()[0]

    # SQLite JSON is enabled by default since 3.38.0 (2022-02-22), and JSONB is available since 3.45.0 (2024-01-15).
    # https://sqlite.org/json1.html
    def encode(self, obj):
        return sqlite3.Binary(json.dumps(obj).encode("ascii"))

    def decode(self, obj):
        return json.loads(bytes(obj).decode("ascii"))


class SqliteSequence(SqliteMixin):
    """
    A sequence of accepted jobs, shared by the queues of all projects, so that pending jobs have a total order, even
    across restarts.
    """

    def __init__(self, database=None, table="queue_sequence"):
        super().__init__(database, table)

        self.conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (id integer PRIMARY KEY, value integer NOT NULL)")
        # The row is never deleted, so the sequence continues after a restart.
        self.conn.execute(f"INSERT OR IGNORE INTO {table} (id, value) VALUES (1, 0)")
        self.conn.commit()

    def next(self):
        # Update and read in one write transaction, so that two processes cannot receive the same sequence value.
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute(f"UPDATE {self.table} SET value = value + 1 WHERE id = 1")
            value = self.conn.execute(f"SELECT value FROM {self.table} WHERE id = 1").fetchone()[0]
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        self.conn.execute("COMMIT")
        return value


class JsonSqlitePriorityQueue(SqliteMixin):
    """
    SQLite priority queue. It relies on SQLite concurrency support for providing atomic inter-process operations.

    Messages are popped by descending priority. Messages of equal priority are popped in the order in which they were
    added (FIFO), using a sequence that is passed to :meth:`put` and stored with the message. Without a sequence, the
    queue's own monotonic counter is used. Either way, the order survives a restart of the process.

    A message with a ``_job`` key can be pending only once: adding the same job again updates its priority, instead of
    dispatching it twice.

    .. versionadded:: 1.0.0
    .. versionchanged:: 1.7.0
       Order messages of equal priority by the order in which they were added, and keep the order across restarts.
       Add the ``sequence`` parameter to :meth:`put`. Return whether :meth:`put` added or updated the message.
    """

    def __init__(self, database=None, table="queue", sequence=None):
        super().__init__(database, table)

        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table} "
            "(id integer PRIMARY KEY, priority real key, message blob, sequence integer, job text)"
        )
        # Queues created by older versions have no sequence and job columns.
        columns = {column[1] for column in self.conn.execute(f"PRAGMA table_info({table})")}
        if "sequence" not in columns:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN sequence integer")
        if "job" not in columns:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN job text")
            self._backfill_jobs()

        # Order the messages of older versions, and then the messages of this version, in the order they were added.
        self._sequence = sequence
        if self._sequence is None:
            self._sequence = MemorySequence(start=self._max_sequence() + 1)
        self._backfill_sequence()

        # A job can be pending only once per project, so that a repeated or retried schedule request cannot dispatch
        # the same job twice. Queues created by older versions might already contain duplicate jobs, so index them only
        # after keeping the message that was accepted first for each job ID.
        index = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?", (f"{table}_job",)
        ).fetchone()
        if index is None:
            self._deduplicate_jobs()
            self.conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {table}_job ON {table} (job) WHERE job IS NOT NULL")
        self.conn.commit()

    def _max_sequence(self):
        return self.conn.execute(f"SELECT COALESCE(MAX(sequence), 0) FROM {self.table}").fetchone()[0]

    def _backfill_jobs(self):
        for _id, message in self.conn.execute(f"SELECT id, message FROM {self.table}").fetchall():
            job = self._job(self.decode(message))
            if job is not None:
                self.conn.execute(f"UPDATE {self.table} SET job = ? WHERE id = ?", (job, _id))
        self.conn.commit()

    def _backfill_sequence(self):
        rows = self.conn.execute(f"SELECT id FROM {self.table} WHERE sequence IS NULL ORDER BY id").fetchall()
        if not rows:
            return

        for (_id,) in rows:
            self.conn.execute(
                f"UPDATE {self.table} SET sequence = ? WHERE id = ?",
                (self._sequence.next(), _id),
            )
        self.conn.commit()

    def _deduplicate_jobs(self):
        # Keep the message that was accepted first for each job ID.
        self.conn.execute(
            f"DELETE FROM {self.table} "
            f"WHERE job IS NOT NULL AND id NOT IN (SELECT MIN(id) FROM {self.table} WHERE job IS NOT NULL GROUP BY job)"
        )
        self.conn.commit()

    def _order_by(self):
        # IDs are not reused, so the ID breaks ties deterministically, including for unsequenced messages.
        return "priority DESC, sequence IS NULL, sequence ASC, id ASC"

    @staticmethod
    def _dispatch_key(priority, sequence):
        # Comparable across queues; a smaller key is dispatched earlier: higher priority first, then acceptance order.
        # Unsequenced rows sort first, like the `sequence IS NULL` term of the ordering.
        return (-priority, sequence if sequence is not None else float("-inf"))

    def _job(self, message):
        return message.get("_job") if isinstance(message, dict) else None

    def put(self, message, priority=0.0, sequence=None):
        """Add a message.

        If a message with the same ``_job`` is already pending, only its priority is updated, and the message keeps its
        position among messages of equal priority. Return ``True`` if the message was added, and ``False`` if the
        priority of a pending message was updated instead.
        """
        if sequence is None:
            sequence = self._sequence.next()

        job = self._job(message)
        try:
            self.conn.execute(
                f"INSERT INTO {self.table} (priority, message, sequence, job) VALUES (?, ?, ?, ?)",
                (priority, self.encode(message), sequence, job),
            )
        except sqlite3.IntegrityError:
            self.conn.rollback()
            if job is None or not self._update_priority(job, priority):
                raise
            return False

        self.conn.commit()
        return True

    def _update_priority(self, job, priority):
        if job is None:
            return 0

        updated = self.conn.execute(f"UPDATE {self.table} SET priority = ? WHERE job = ?", (priority, job)).rowcount
        self.conn.commit()
        return updated

    def pop(self):
        row = self.conn.execute(f"SELECT id, message FROM {self.table} ORDER BY {self._order_by()} LIMIT 1").fetchone()
        if row is None:
            return None
        _id, message = row

        # If a row vanished, try again.
        if not self.conn.execute(f"DELETE FROM {self.table} WHERE id = ?", (_id,)).rowcount:
            self.conn.rollback()
            return self.pop()

        self.conn.commit()
        return self.decode(message)

    def peek(self):
        """Return the dispatch key of the next message, or ``None`` if the queue is empty.

        Dispatch keys are comparable, and a smaller key means that the message is dispatched earlier.
        """
        row = self.conn.execute(
            f"SELECT priority, sequence FROM {self.table} ORDER BY {self._order_by()} LIMIT 1"
        ).fetchone()
        return None if row is None else self._dispatch_key(*row)

    def pending(self):
        """Return the ``(message, dispatch key)`` pairs, in the order in which they would be popped."""
        return [
            (self.decode(message), self._dispatch_key(priority, sequence))
            for message, priority, sequence in self.conn.execute(
                f"SELECT message, priority, sequence FROM {self.table} ORDER BY {self._order_by()}"
            )
        ]

    def remove(self, func):
        deleted = 0
        for _id, message in self.conn.execute(f"SELECT id, message FROM {self.table}").fetchall():
            if func(self.decode(message)):
                # If a row vanished, try again.
                if not self.conn.execute(f"DELETE FROM {self.table} WHERE id = ?", (_id,)).rowcount:
                    self.conn.rollback()
                    return self.remove(func)
                deleted += 1

        self.conn.commit()
        return deleted

    def clear(self):
        self.conn.execute(f"DELETE FROM {self.table}")
        self.conn.commit()

    def __iter__(self):
        return (
            (self.decode(message), priority)
            for message, priority in self.conn.execute(
                f"SELECT message, priority FROM {self.table} ORDER BY {self._order_by()}"
            )
        )


class SqliteFinishedJobs(SqliteMixin):
    """
    SQLite finished jobs.

    .. versionadded:: 1.3.0
       Job storage was previously in-memory only.
    """

    def __init__(self, database=None, table="finished_jobs"):
        super().__init__(database, table)

        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table} "
            "(id integer PRIMARY KEY, project text, spider text, job text, start_time datetime, end_time datetime)"
        )

    def add(self, job):
        self.conn.execute(
            f"INSERT INTO {self.table} (project, spider, job, start_time, end_time) VALUES (?, ?, ?, ?, ?)",
            (job.project, job.spider, job.job, job.start_time, job.end_time),
        )
        self.conn.commit()

    def clear(self, finished_to_keep=None):
        where = ""
        if finished_to_keep:
            limit = len(self) - finished_to_keep
            if limit <= 0:
                return  # nothing to delete
            where = f"WHERE id <= (SELECT max(id) FROM (SELECT id FROM {self.table} ORDER BY end_time LIMIT {limit}))"

        self.conn.execute(f"DELETE FROM {self.table} {where}")
        self.conn.commit()

    def __iter__(self):
        return (
            (
                project,
                spider,
                job,
                datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S.%f"),
                datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S.%f"),
            )
            for project, spider, job, start_time, end_time in self.conn.execute(
                f"SELECT project, spider, job, start_time, end_time FROM {self.table} ORDER BY end_time DESC"
            )
        )
