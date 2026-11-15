import datetime
import json
import sqlite3
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

    .. versionadded:: 1.0.0
    """

    def __init__(self, database=None, table="queue"):
        super().__init__(database, table)

        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table} (id integer PRIMARY KEY, priority real key, message blob)"
        )

    def put(self, message, priority=0.0):
        self.conn.execute(
            f"INSERT INTO {self.table} (priority, message) VALUES (?, ?)",
            (priority, self.encode(message)),
        )
        self.conn.commit()

    def pop(self):
        row = self.conn.execute(f"SELECT id, message FROM {self.table} ORDER BY priority DESC LIMIT 1").fetchone()
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
                f"SELECT message, priority FROM {self.table} ORDER BY priority DESC"
            )
        )


class SqliteLaunchRegistryStore(SqliteMixin):
    """
    SQLite storage for the launch registry, tracking a job between dequeue and process start.

    The states are ``launching`` (popped from the spider queue, process not known to have started), ``running`` (the
    process started), ``failed`` (the process could not be started) and ``orphaned`` (the process started under a
    previous Scrapyd process and is no longer alive).
    """

    def __init__(self, database=None, table="launch_registry"):
        super().__init__(database, table)

        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table} ("
            "project text NOT NULL, job text NOT NULL, message blob NOT NULL, priority real NOT NULL, "
            "state text NOT NULL, pid integer, error text, start_time datetime, end_time datetime, "
            "PRIMARY KEY (project, job))"
        )
        self.conn.commit()

    def claim(self, project, queue):
        """
        Atomically pop the highest-priority row of the spider ``queue`` and insert it in a launching state.

        When the queue is in another database, it is attached to this connection so that the delete and the insert
        commit in a single transaction: a crash in between cannot remove the job without recording it. Return the
        popped message, or ``None`` if the queue is empty or its row was claimed concurrently.
        """
        if queue.q.conn is self.conn:
            return self._claim(project, queue, queue.q.table, None)
        return self._claim_attached(project, queue)

    def _claim_attached(self, project, queue):
        attached = "attached_spider_queue"
        self.conn.execute("ATTACH DATABASE ? AS attached_spider_queue", (queue.q.database,))
        try:
            return self._claim(project, queue, queue.q.table, attached)
        finally:
            self.conn.execute("DETACH DATABASE attached_spider_queue")

    def _claim(self, project, queue, queue_table, attached):
        prefix = f"{attached}." if attached else ""
        row = self.conn.execute(
            f"SELECT id, priority, message FROM {prefix}{queue_table} ORDER BY priority DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        _id, priority, encoded = row
        message = self.decode(encoded)

        try:
            with self.conn:  # BEGIN/COMMIT, rolling back on an exception
                deleted = self.conn.execute(
                    f"DELETE FROM {prefix}{queue_table} WHERE id = ?", (_id,)
                ).rowcount
                if not deleted:
                    # Another process claimed the row first; nothing was changed.
                    return None
                self.conn.execute(
                    f"INSERT INTO {self.table} (project, job, message, priority, state, start_time) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (project, message["_job"], encoded, priority, "launching", datetime.datetime.now()),
                )
        except sqlite3.IntegrityError:
            # The job is already recorded (for example, a duplicate job ID). Leave the queue row untouched.
            self.conn.rollback()
            return None
        return message

    def requeue(self, entry, queue):
        """Put a never-started job back in its spider ``queue`` and remove its record, in one transaction."""
        if entry.state not in ("launching", "running"):
            raise ValueError(f"Cannot requeue a job in state {entry.state!r}")

        if queue.q.conn is self.conn:
            self._requeue(entry, queue.q.table, None)
        else:
            self.conn.execute("ATTACH DATABASE ? AS attached_spider_queue", (queue.q.database,))
            try:
                self._requeue(entry, queue.q.table, "attached_spider_queue")
            finally:
                self.conn.execute("DETACH DATABASE attached_spider_queue")
        return entry.message

    def _requeue(self, entry, queue_table, attached):
        prefix = f"{attached}." if attached else ""
        with self.conn:
            self.conn.execute(
                f"DELETE FROM {self.table} WHERE project = ? AND job = ?", (entry.project, entry.job)
            )
            self.conn.execute(
                f"INSERT INTO {prefix}{queue_table} (priority, message) VALUES (?, ?)",
                (entry.priority, self.encode(entry.message)),
            )

    def mark_running(self, project, job, pid):
        with self.conn:
            self.conn.execute(
                f"UPDATE {self.table} SET state = 'running', pid = ? WHERE project = ? AND job = ?",
                (pid, project, job),
            )

    def finish(self, project, job):
        with self.conn:
            self.conn.execute(f"DELETE FROM {self.table} WHERE project = ? AND job = ?", (project, job))

    def fail(self, project, job, error):
        with self.conn:
            self.conn.execute(
                f"UPDATE {self.table} SET state = 'failed', error = ?, end_time = ? WHERE project = ? AND job = ?",
                (str(error), datetime.datetime.now(), project, job),
            )

    def orphan(self, project, job):
        with self.conn:
            self.conn.execute(
                f"UPDATE {self.table} SET state = 'orphaned', end_time = ? WHERE project = ? AND job = ?",
                (datetime.datetime.now(), project, job),
            )

    def get(self, project, job):
        row = self.conn.execute(
            f"SELECT project, job, message, priority, state, pid, error, start_time, end_time "
            f"FROM {self.table} WHERE project = ? AND job = ?",
            (project, job),
        ).fetchone()
        return self._row_to_entry(row) if row is not None else None

    def active(self, project=None):
        sql = (
            f"SELECT project, job, message, priority, state, pid, error, start_time, end_time FROM {self.table} "
            "WHERE state IN ('launching', 'running')"
        )
        params = []
        if project is not None:
            sql += " AND project = ?"
            params.append(project)
        sql += " ORDER BY start_time"
        for row in self.conn.execute(sql, params):
            yield self._row_to_entry(row)

    def count_active(self):
        return self.conn.execute(
            f"SELECT COUNT(*) FROM {self.table} WHERE state IN ('launching', 'running')"
        ).fetchone()[0]

    def __iter__(self):
        for row in self.conn.execute(
            f"SELECT project, job, message, priority, state, pid, error, start_time, end_time "
            f"FROM {self.table} ORDER BY start_time"
        ):
            yield self._row_to_entry(row)

    def clear(self):
        self.conn.execute(f"DELETE FROM {self.table}")
        self.conn.commit()

    def _row_to_entry(self, row):
        from scrapyd.launchregistry import LaunchEntry  # noqa: PLC0415 avoid a circular import

        project, job, encoded, priority, state, pid, error, start_time, end_time = row
        return LaunchEntry(
            project=project,
            job=job,
            message=self.decode(encoded),
            priority=priority,
            state=state,
            pid=pid,
            error=error,
            start_time=_parse_datetime(start_time),
            end_time=_parse_datetime(end_time),
        )


def _parse_datetime(value):
    if value is None or isinstance(value, datetime.datetime):
        return value
    return datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")


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
