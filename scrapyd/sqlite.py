import datetime
import json
import sqlite3
import time
from pathlib import Path


# The database argument is "jobs" (in SqliteJobStorage), or a project (in SqliteSpiderQueue) from get_spider_queues(),
# which gets projects from get_project_list(), which gets projects from egg storage. We check for directory traversal
# in egg storage, instead.
def initialize(cls, config, database, table):
    dbs_dir = config.get("dbs_dir", "dbs")
    if dbs_dir == ":memory:":
        connection_string = dbs_dir
    else:
        dbs_path = Path(dbs_dir)
        if not dbs_path.exists():
            dbs_path.mkdir(parents=True, exist_ok=True)
        connection_string = str(dbs_path / f"{database}.db")

    return cls(connection_string, table)


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

    def __len__(self):
        return self.conn.execute(f"SELECT COUNT(*) FROM {self.table}").fetchone()[0]

    # SQLite JSON is enabled by default since 3.38.0 (2022-02-22), and JSONB is available since 3.45.0 (2024-01-15).
    # https://sqlite.org/json1.html
    def encode(self, obj):
        return sqlite3.Binary(json.dumps(obj).encode("ascii"))

    def decode(self, obj):
        return json.loads(bytes(obj).decode("ascii"))


class JsonSqlitePriorityQueue(SqliteMixin):
    """
    SQLite priority queue. It relies on SQLite concurrency support for providing atomic inter-process operations.

    Jobs are ordered by descending priority, then by ascending acceptance time, then by insertion ID. The acceptance
    time is persisted, so the order of equal-priority jobs survives a restart.

    Adding a message again with the same ``_job`` ID updates that pending job in place, keeping the acceptance time that
    it received when it was first accepted.

    .. versionadded:: 1.0.0
    .. versionchanged:: 1.7.0
       Persist the acceptance time of each job. Order equal-priority jobs by it, instead of leaving the order
       unspecified. Add the ``head`` method, and stop the ``put`` method from adding a second pending job for the same
       ``_job`` ID.
    """

    def __init__(self, database=None, table="queue"):
        super().__init__(database, table)

        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table} "
            "(id integer PRIMARY KEY, priority real key, accepted_at integer, job_key text, message blob)"
        )
        self._migrate()

    def _migrate(self):
        """Add the ordering columns to a queue that was created by an older version of Scrapyd."""
        columns = {row[1] for row in self.conn.execute(f"PRAGMA table_info({self.table})")}
        if "accepted_at" not in columns:
            self.conn.execute(f"ALTER TABLE {self.table} ADD COLUMN accepted_at integer")
        if "job_key" not in columns:
            self.conn.execute(f"ALTER TABLE {self.table} ADD COLUMN job_key text")

        # Legacy rows have no acceptance time. The insertion ID (rowid) reflects the order in which they were accepted.
        self.conn.execute(f"UPDATE {self.table} SET accepted_at = id WHERE accepted_at IS NULL")

        # Legacy rows have no job key, and the same job might have been added more than once. Keep the first pending
        # copy, so that a job that is still pending is not dispatched twice.
        seen = set()
        for rowid, message in self.conn.execute(
            f"SELECT id, message FROM {self.table} WHERE job_key IS NULL ORDER BY id"
        ).fetchall():
            job_key = self._job_key(self.decode(message))
            if job_key is not None:
                if job_key in seen:
                    self.conn.execute(f"DELETE FROM {self.table} WHERE id = ?", (rowid,))
                    continue
                seen.add(job_key)
            self.conn.execute(f"UPDATE {self.table} SET job_key = ? WHERE id = ?", (job_key, rowid))

        # A NULL job key never conflicts, so messages that have no job ID are still allowed to repeat.
        self.conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {self.table}_job_key ON {self.table} (job_key)")
        self.conn.execute(
            f"CREATE INDEX IF NOT EXISTS {self.table}_order ON {self.table} (priority DESC, accepted_at, id)"
        )
        self.conn.commit()

    @staticmethod
    def _job_key(message):
        """Return a unique key for the message's job ID, or None if the message has no job ID."""
        if not isinstance(message, dict):
            return None
        job = message.get("_job")
        if job is None:
            return None
        # JSON encoding avoids ambiguity between, for example, the int 1 and the str "1".
        return json.dumps(job)

    def _next_accepted_at(self):
        """Return a strictly increasing acceptance time, so that equal-priority jobs can be ordered reproducibly."""
        last = self.conn.execute(f"SELECT max(accepted_at) FROM {self.table}").fetchone()[0] or 0
        return max(time.time_ns(), last + 1)

    def put(self, message, priority=0.0):
        # Adding a pending job again updates it in place, keeping its acceptance time (and therefore its position
        # among equal-priority jobs). See the class' docstring.
        self.conn.execute(
            f"INSERT INTO {self.table} (priority, accepted_at, job_key, message) VALUES (?, ?, ?, ?) "
            "ON CONFLICT (job_key) DO UPDATE SET priority = excluded.priority, message = excluded.message",
            (priority, self._next_accepted_at(), self._job_key(message), self.encode(message)),
        )
        self.conn.commit()

    def _select_next(self):
        return self.conn.execute(
            f"SELECT id, message FROM {self.table} ORDER BY priority DESC, accepted_at, id LIMIT 1"
        ).fetchone()

    def head(self):
        """
        Return the priority and acceptance time of the next message to be returned by :meth:`pop`, without removing it.

        Return None if the queue is empty.

        .. versionadded:: 1.7.0
        """
        row = self.conn.execute(
            f"SELECT priority, accepted_at FROM {self.table} ORDER BY priority DESC, accepted_at, id LIMIT 1"
        ).fetchone()
        return None if row is None else (row[0], row[1])

    def pop(self):
        row = self._select_next()
        if row is None:
            return None
        _id, message = row

        # If a row vanished, try again.
        if not self.conn.execute(f"DELETE FROM {self.table} WHERE id = ?", (_id,)).rowcount:
            self.conn.rollback()
            return self.pop()

        self.conn.commit()
        return self.decode(message)

    def remove(self, func):
        deleted = 0
        for _id, message in self.conn.execute(f"SELECT id, message FROM {self.table}"):
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
                f"SELECT message, priority FROM {self.table} ORDER BY priority DESC, accepted_at, id"
            )
        )

    def order(self):
        """
        Return the ``(priority, accepted_at)`` ordering key of each pending message, in the order of :meth:`__iter__`.

        The keys of messages in different queues can be compared with each other.

        .. versionadded:: 1.7.0
        """
        return list(
            self.conn.execute(
                f"SELECT priority, accepted_at FROM {self.table} ORDER BY priority DESC, accepted_at, id"
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
