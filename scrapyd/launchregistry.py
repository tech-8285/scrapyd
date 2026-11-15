"""
Launch registries record jobs between the moment they leave the spider queue and the moment their crawl process is
known to have started (or to have failed to start).

This closes the gap in which an accepted job is neither pending (already popped from the spider queue) nor running (the
crawl process did not start yet). A failure or a service restart in that window would otherwise silently lose the job.
"""

import datetime
import os

from zope.interface import implementer

from scrapyd import sqlite
from scrapyd.interfaces import ILaunchRegistry

# State of a job recorded in a launch registry.
LAUNCHING = "launching"  # left the spider queue; the crawl process is not known to have started
RUNNING = "running"  # the crawl process started (connectionMade)
FAILED = "failed"  # the crawl process could not be started
ORPHANED = "orphaned"  # the process started under a previous Scrapyd process and is no longer alive

# Active states still need a slot: a launching job is about to occupy one, and a running job occupies one.
ACTIVE_STATES = (LAUNCHING, RUNNING)


def pid_alive(pid):
    """Return whether a process with ``pid`` exists. A pid of None means the process never reported its pid."""
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # The process exists, but we are not allowed to signal it.
        return True
    return True


class LaunchEntry:
    def __init__(self, project, job, message, priority, state, pid=None, error=None, start_time=None, end_time=None):
        self.project = project
        self.job = job
        self.message = message
        self.priority = priority
        self.state = state
        self.pid = pid
        self.error = error
        self.start_time = start_time
        self.end_time = end_time

    @property
    def spider(self):
        return self.message.get("name")


@implementer(ILaunchRegistry)
class SqliteLaunchRegistry:
    """
    SQLite-backed launch registry.

    The registry lives in its own database (``launch_registry.db`` by default). A claim attaches the spider queue's
    database and deletes the queue row and inserts the registry row in a single transaction, so that a crash in between
    can neither silently lose the job nor leave it in both places.
    """

    def __init__(self, config):
        self.config = config
        self.records = sqlite.initialize(sqlite.SqliteLaunchRegistryStore, config, "launch_registry", "launch_registry")

    def claim(self, project, queue):
        return self.records.claim(project, queue)

    def requeue(self, entry, queues):
        if entry.state not in ACTIVE_STATES:
            raise ValueError(f"Cannot requeue a job in state {entry.state!r}")
        queue = queues[entry.project]
        return self.records.requeue(entry, queue)

    def mark_running(self, project, job, pid):
        self.records.mark_running(project, job, pid)

    def finish(self, project, job):
        self.records.finish(project, job)

    def fail(self, project, job, error):
        self.records.fail(project, job, error)

    def orphan(self, project, job):
        self.records.orphan(project, job)

    def recover(self, queues):
        requeued = 0
        for entry in list(self.active()):
            if entry.state == LAUNCHING or entry.pid is None:
                # The process never proved it started: re-running is safe, because it never crawled.
                if entry.project in queues:
                    self.requeue(entry, queues)
                    requeued += 1
                else:
                    # The project no longer exists: record an explicit terminal state instead of dropping the job.
                    self.fail(entry.project, entry.job, "project no longer available at recovery")
            elif pid_alive(entry.pid):
                # The crawl process survived the Scrapyd restart and still owns this job: do not run it twice.
                continue
            else:
                # The process started, but is gone and never reached the finished-job storage: make the end explicit.
                self.records.orphan(entry.project, entry.job)
        return requeued

    def get(self, project, job):
        return self.records.get(project, job)

    def active(self, project=None):
        yield from self.records.active(project)

    def count_active(self):
        return self.records.count_active()

    def list(self):
        yield from self.records

    def clear(self):
        self.records.clear()


@implementer(ILaunchRegistry)
class MemoryLaunchRegistry:
    """An in-memory launch registry for non-persistent deployments (the default job storage is in-memory as well)."""

    def __init__(self, config=None):
        self.entries = {}

    def claim(self, project, queue):
        if queue.count() == 0:
            return None
        popped = queue.pop()
        if popped is None:
            return None
        job = popped["_job"]
        if (project, job) in self.entries:
            # A duplicate job ID: restore the popped message rather than overwrite the existing record.
            self._add_back(queue, popped)
            return None
        self.entries[(project, job)] = LaunchEntry(
            project=project,
            job=job,
            message=popped,
            priority=0.0,
            state=LAUNCHING,
            start_time=datetime.datetime.now(),
        )
        return popped

    def requeue(self, entry, queues):
        if entry.state not in ACTIVE_STATES:
            raise ValueError(f"Cannot requeue a job in state {entry.state!r}")
        self.entries.pop((entry.project, entry.job), None)
        self._add_back(queues[entry.project], entry.message)
        return entry.message

    def mark_running(self, project, job, pid):
        entry = self.entries.get((project, job))
        if entry is not None:
            entry.state = RUNNING
            entry.pid = pid

    def finish(self, project, job):
        self.entries.pop((project, job), None)

    def fail(self, project, job, error):
        entry = self.entries.get((project, job))
        if entry is None:
            return
        entry.state = FAILED
        entry.error = str(error)
        entry.end_time = datetime.datetime.now()

    def orphan(self, project, job):
        entry = self.entries.get((project, job))
        if entry is None:
            return
        entry.state = ORPHANED
        entry.end_time = datetime.datetime.now()

    def recover(self, queues):
        # Nothing survives a process restart in memory; there is nothing to reconcile.
        return 0

    def get(self, project, job):
        return self.entries.get((project, job))

    def active(self, project=None):
        for entry in list(self.entries.values()):
            if entry.state in ACTIVE_STATES and (project is None or entry.project == project):
                yield entry

    def count_active(self):
        return sum(1 for entry in self.entries.values() if entry.state in ACTIVE_STATES)

    def list(self):
        yield from self.entries.values()

    def clear(self):
        self.entries.clear()

    @staticmethod
    def _add_back(queue, message):
        message = message.copy()
        name = message.pop("name")
        queue.add(name, **message)
