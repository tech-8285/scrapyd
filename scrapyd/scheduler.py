from zope.interface import implementer

from scrapyd.interfaces import ISpiderScheduler
from scrapyd.utils import get_spider_queues


@implementer(ISpiderScheduler)
class SpiderScheduler:
    def __init__(self, config):
        self.config = config
        self.update_projects()

    def schedule(self, project, spider_name, priority=0.0, **spider_args):
        """Schedule a crawl, and return whether it was added to the spider queue.

        Scheduling a job whose ``_job`` ID is already pending updates its priority, and returns ``False``: the job
        keeps its place in the queue, so that a repeated or retried request neither dispatches the job twice nor
        reorders the jobs that were accepted with the same priority.
        """
        return self.queues[project].add(spider_name, priority=priority, **spider_args)

    def list_projects(self):
        return list(self.queues)

    def update_projects(self):
        self.queues = get_spider_queues(self.config)
