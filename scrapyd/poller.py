from twisted.internet.defer import DeferredQueue, inlineCallbacks, maybeDeferred
from zope.interface import implementer

from scrapyd.interfaces import IPoller
from scrapyd.utils import get_spider_queues


@implementer(IPoller)
class QueuePoller:
    def __init__(self, config):
        self.config = config
        self.update_projects()
        self.dq = DeferredQueue()

    @inlineCallbacks
    def poll(self):
        # If the "waiting" backlog is empty, the maximum number of Scrapy processes are running.
        while self.dq.waiting:
            project, message = yield self._pop_next()
            # There are no pending jobs.
            if message is None:
                return

            message["_project"] = project
            message["_spider"] = message.pop("name")
            # Pop a dummy item from the "waiting" backlog, and fire the message's callbacks.
            self.dq.put(message)

    @inlineCallbacks
    def _pop_next(self):
        """Return the ``(project, message)`` of the pending job that should start next.

        Return ``(None, None)`` if there are no pending jobs.

        The spider queues order their pending jobs deterministically: by descending priority, then by the order in which
        the jobs were accepted. Comparing the next pending job of every project therefore starts the highest-priority
        job across all projects, and breaks priority ties by acceptance order. So, the dispatch order is reproducible,
        including after a restart, and a project with a backlog cannot starve the other projects.
        """
        peek = [getattr(queue, "peek", None) for queue in self.queues.values()]
        if not all(peek):
            return (yield self._pop_next_in_order())

        # Retry a bounded number of times, in case another Scrapyd instance sharing a spider queue takes the job that
        # was peeked, making the comparison of the peeked jobs stale.
        for _ in range(len(self.queues) + 1):
            project = yield self._next_project()
            if project is None:
                return None, None

            message = yield maybeDeferred(self.queues[project].pop)
            if message is not None:
                return project, message

        return None, None

    @inlineCallbacks
    def _next_project(self):
        """Return the project whose next pending job should start, by comparing each project's next pending job."""
        heads = []
        for project, queue in self.queues.items():
            key = yield maybeDeferred(queue.peek)
            if key is not None:
                # A smaller dispatch key runs earlier: higher priority first, then acceptance order. The project name
                # breaks a tie, if two queues unexpectedly share a dispatch key.
                heads.append((key, project))

        return min(heads)[1] if heads else None

    @inlineCallbacks
    def _pop_next_in_order(self):
        """Poll the projects in order, for custom spider queues that don't support ``peek()``."""
        for project, queue in self.queues.items():
            if (yield maybeDeferred(queue.count)):
                message = yield maybeDeferred(queue.pop)
                if message is not None:
                    return project, message

        return None, None

    def next(self):
        """
        Add a dummy item to the "waiting" backlog (based on Twisted's implementation of DeferredQueue).
        """
        return self.dq.get()

    def update_projects(self):
        self.queues = get_spider_queues(self.config)
