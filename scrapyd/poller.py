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
        # Messages that left a spider queue but whose crawl process did not start yet, keyed by (project, job). They
        # are neither pending (they already left the queue) nor running (there is no process yet), so unless they are
        # tracked here, a cancellation request that lands in this window reports that there is nothing to cancel, and
        # the job then starts and crawls anyway.
        #
        # A message is tracked from the moment its queue row is deleted until the launcher takes it to start (or fail
        # to start) its process. While every launcher slot is busy, the message waits in ``dq``, still tracked.
        self.launching = {}
        # Jobs cancelled while they were dequeued but not started. A launcher slot that would start one must not.
        self.cancelled = set()

    @inlineCallbacks
    def poll(self):
        for project, queue in self.queues.items():
            while (yield maybeDeferred(queue.count)):
                # If the "waiting" backlog is empty (that is, if the maximum number of Scrapy processes are running):
                if not self.dq.waiting:
                    return
                message = yield maybeDeferred(queue.pop)
                # The message can be None if, for example, two Scrapyd instances share a spider queue database.
                if message is None:
                    break

                message = message.copy()
                key = self._key(project, message)
                if key is not None:
                    # The queue no longer contains the message: record it before handing it over, so that it can be
                    # reported and cancelled while it waits to be started.
                    self.launching[key] = message
                yield maybeDeferred(self._deliver, project, message)

    def _deliver(self, project, message):
        """
        Hand a message that left its spider queue to a launcher slot, unless it was cancelled in the meantime.

        Subclasses can return a Deferred to delay the delivery, which models, for example, a spider queue whose
        ``pop`` involves another service. The message is then reported as pending and is cancellable until delivered.
        """
        key = self._key(project, message)
        if key is not None and key in self.cancelled:
            self.cancelled.discard(key)
            self.launching.pop(key, None)
            return

        delivered = message.copy()
        delivered["_project"] = project
        delivered["_spider"] = delivered.pop("name")
        # Fire the "waiting" backlog's callback, which starts the crawl process (or, if every slot is busy, queues
        # the message until a slot asks for it).
        self.dq.put(delivered)

    @staticmethod
    def _key(project, message):
        try:
            return project, message["_job"]
        except KeyError:
            return None

    def take_launched(self, project, job):
        """
        Stop tracking the message of ``job``, because a launcher slot is about to start (or fail to start) it.

        Return whether the job was cancelled while it was dequeued but not started, in which case it must not start.
        """
        key = (project, job)
        self.launching.pop(key, None)
        if key in self.cancelled:
            self.cancelled.discard(key)
            return True
        return False

    def launching_messages(self, project=None):
        """Yield ``(project, message)`` for messages that left a spider queue but did not start a process yet."""
        return [
            (launched_project, message)
            for (launched_project, _), message in self.launching.items()
            if project is None or launched_project == project
        ]

    def cancel_launching(self, project, job):
        """
        Cancel a message of ``job`` that left its spider queue but did not start a process yet.

        Return whether such a message was found. A found message is never started, which is consistent with
        cancelling a job that is still pending in its spider queue.
        """
        key = (project, job)
        if key not in self.launching:
            return False
        self.launching.pop(key, None)
        self.cancelled.add(key)
        return True

    def next(self):
        """
        Add a dummy item to the "waiting" backlog (based on Twisted's implementation of DeferredQueue).
        """
        return self.dq.get()

    def update_projects(self):
        self.queues = get_spider_queues(self.config)
