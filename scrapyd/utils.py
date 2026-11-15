from pathlib import Path

from scrapy.utils.misc import load_object
from twisted.python import filepath

from scrapyd.exceptions import DirectoryTraversalError


def initialize_component(config, setting, default, *args):
    path = config.get(setting, default)
    cls = load_object(path)
    return cls(config, *args)


def local_items(items_dir, parsed):
    return items_dir and parsed.scheme.lower() in ("", "file", str(Path(items_dir).drive).rstrip(":").lower())


def get_file_path(directory, project, spider, job):
    # https://docs.twisted.org/en/stable/api/twisted.python.filepath.FilePath.html
    try:
        return filepath.FilePath(directory).child(project).child(spider).child(job)
    except filepath.InsecurePath as e:
        raise DirectoryTraversalError(Path(project) / spider / job) from e


def get_spider_queues(config):
    """Return a dict of Spider Queues keyed by project name"""
    spiderqueue_cls = load_object(config.get("spiderqueue", "scrapyd.spiderqueue.SqliteSpiderQueue"))
    return {project: spiderqueue_cls(config, project) for project in get_project_list(config)}


def get_pending_jobs(queues):
    """Return the ``(project, message)`` pairs of the pending jobs, in the order in which they are dispatched.

    The spider queues of all projects order their jobs together, by comparing the dispatch key of each project's next
    pending job. See :meth:`scrapyd.interfaces.ISpiderQueue.peek`.
    """
    if not all(getattr(queue, "pending", None) for queue in queues.values()):
        # Custom spider queues might not implement pending(): list the projects and their jobs in order.
        return [(project, message) for project, queue in queues.items() for message in queue.list()]

    pairs = [(key, project, message) for project, queue in queues.items() for message, key in queue.pending()]
    return [(project, message) for _, project, message in sorted(pairs, key=lambda pair: (pair[0], pair[1]))]


def get_project_list(config):
    """Get list of projects by inspecting the eggs storage and the ones defined in
    the scrapy.cfg [settings] section
    """

    # The poller and scheduler use this function (via get_spider_queues), and they aren't initialized with the
    # application. So, we need to re-initialize this component here.
    eggstorage = initialize_component(config, "eggstorage", "scrapyd.eggstorage.FilesystemEggStorage")
    return eggstorage.list_projects() + [project for project, _ in config.items("settings", default=[])]
