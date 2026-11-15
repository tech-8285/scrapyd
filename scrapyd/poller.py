from twisted.internet.defer import DeferredQueue, inlineCallbacks, maybeDeferred
from zope.interface import implementer

from scrapyd.interfaces import IPoller
from scrapyd.utils import get_spider_queues


@implementer(IPoller)
class QueuePoller:
    """
    Poll projects' spider queues, and start the jobs that have the highest priority.

    Jobs are ordered by descending priority, then by ascending acceptance time. Projects are therefore interleaved
    rather than drained one after the other, so that a project with a large backlog cannot starve equal-priority jobs
    in other projects that share the same process slots.

    .. versionchanged:: 1.7.0
       Order jobs across projects by priority and acceptance time, instead of by project.
    """

    def __init__(self, config):
        self.config = config
        self._polled = set()
        self.update_projects()
        self.dq = DeferredQueue()

    @inlineCallbacks
    def poll(self):
        self._polled = set()

        # Each waiting Deferred represents a free process slot.
        while self.dq.waiting:
            candidate = yield self._next_candidate()
            if candidate is None:
                return

            project, queue = candidate
            # The message can be None if, for example, two Scrapyd instances share a spider queue database.
            message = yield maybeDeferred(queue.pop)
            if message is None:
                # Another instance took the job. Skip this queue for the rest of this poll, and try the next one.
                self._polled.add(project)
                continue

            message = message.copy()
            message["_project"] = project
            message["_spider"] = message.pop("name")
            # Pop a dummy item from the "waiting" backlog. and fire the message's callbacks.
            self.dq.put(message)

    @inlineCallbacks
    def _next_candidate(self):
        """
        Return the ``(project, queue)`` pair whose queue holds the job that should start next, or None if no queue has
        a pending job.
        """
        best = None
        for project, queue in self.queues.items():
            if project in self._polled:
                continue
            if not (yield maybeDeferred(queue.count)):
                continue

            head = yield maybeDeferred(self._head, queue)
            priority, accepted_at = head if head is not None else (0.0, 0.0)
            key = (-priority, accepted_at, project)

            if best is None or key < best[0]:
                best = (key, project, queue)

        if best is None:
            return None
        _, project, queue = best
        return project, queue

    @staticmethod
    def _head(queue):
        try:
            return queue.head()
        except AttributeError:
            # Custom ISpiderQueue implementations might not implement head(). Fall back to project order.
            return None

    def next(self):
        """
        Add a dummy item to the "waiting" backlog (based on Twisted's implementation of DeferredQueue).
        """
        return self.dq.get()

    def update_projects(self):
        self.queues = get_spider_queues(self.config)
