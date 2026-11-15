from zope.interface import implementer

from scrapyd import sqlite
from scrapyd.interfaces import ISpiderQueue


@implementer(ISpiderQueue)
class SqliteSpiderQueue:
    def __init__(self, config, project, table="spider_queue"):
        self.sequence = sqlite.initialize_sequence(config)
        self.q = sqlite.initialize(sqlite.JsonSqlitePriorityQueue, config, project, table, self.sequence)

    def add(self, name, priority=0.0, **spider_args):
        message = spider_args.copy()
        message["name"] = name
        return self.q.put(message, priority=priority, sequence=self.sequence.next())

    def pop(self):
        return self.q.pop()

    def count(self):
        return len(self.q)

    def list(self):
        return [message for message, _ in self.q]

    def remove(self, func):
        return self.q.remove(func)

    def clear(self):
        self.q.clear()

    def peek(self):
        """Return the dispatch key of the next pending job, or ``None`` if there are none.

        Dispatch keys of different projects are comparable, and a smaller key is dispatched earlier. The poller compares
        the keys of all projects, so that the highest-priority job runs first, regardless of its project, and so that a
        project with a backlog cannot starve the other projects.
        """
        return self.q.peek()

    def pending(self):
        """Return the ``(message, dispatch key)`` pairs, in the order in which the jobs would be dispatched."""
        return self.q.pending()
