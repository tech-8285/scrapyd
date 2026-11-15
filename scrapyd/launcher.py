import datetime
import multiprocessing
import sys
from itertools import chain

from twisted.application.service import Service
from twisted.internet import defer, error, protocol, reactor
from twisted.logger import Logger

from scrapyd import __version__
from scrapyd.exceptions import LauncherError
from scrapyd.interfaces import IEggStorage, IEnvironment, IJobStorage, IPoller

log = Logger()


def get_crawl_args(message):
    """Return the command-line arguments to use for the scrapy crawl process
    that will be started for this message
    """
    copied = message.copy()
    del copied["_project"]

    return [
        copied.pop("_spider"),
        *chain.from_iterable(["-s", f"{key}={value}"] for key, value in copied.pop("settings", {}).items()),
        *chain.from_iterable(["-a", f"{key}={value}"] for key, value in copied.items()),  # spider arguments
    ]


class Launcher(Service):
    name = "launcher"

    def __init__(self, config, app):
        self.processes = {}
        self.finished = app.getComponent(IJobStorage)
        self.max_proc = self._get_max_proc(config)
        self.runner = config.get("runner", "scrapyd.runner")
        self.app = app

    def startService(self):
        log.info(
            "Scrapyd {version} started: max_proc={max_proc!r}, runner={runner!r}",
            version=__version__,
            max_proc=self.max_proc,
            runner=self.runner,
            log_system="Launcher",
        )
        for slot in range(self.max_proc):
            self._get_message(slot)

    def _get_message(self, slot):
        poller = self.app.getComponent(IPoller)
        poller.next().addCallback(self._spawn_process, slot)
        log.debug("Process slot {slot} ready", slot=slot)

    def _spawn_process(self, message, slot):
        project = message["_project"]

        # A project or version can have been deleted after the message was queued, for example if a delete request
        # was processed between the poller's pop and this callback. Don't spawn an orphan crawl without an egg version.
        if self._is_orphan_message(message):
            log.info(
                "Skipping crawl of deleted project/version: project={project!r} job={job!r}",
                project=project,
                job=message.get("_job"),
            )
            # The message's slot never became a process, so make the slot wait for another message again.
            self._get_message(slot)
            return

        environment = self.app.getComponent(IEnvironment)
        message.setdefault("settings", {})
        message["settings"].update(environment.get_settings(message))

        env = environment.get_environment(message, slot)
        args = [sys.executable, "-m", self.runner, "crawl", *get_crawl_args(message)]

        process = ScrapyProcessProtocol(
            project, message["_spider"], message["_job"], env, args, version=message.get("_version")
        )
        process.deferred.addBoth(self._process_finished, slot)

        try:
            reactor.spawnProcess(process, sys.executable, args=args, env=env)
        except OSError as e:
            raise LauncherError(f"{e}: args={args!r}") from e

        self.processes[slot] = process
        log.debug("Process slot {slot} occupied", slot=slot)

    def _is_orphan_message(self, message):
        """Return whether the message targets a project or version that no longer exists."""
        poller = self.app.getComponent(IPoller)
        project = message["_project"]

        # A project that is no longer in the poller's queues was deleted (update_projects() is called on deletion).
        if project not in poller.queues:
            return True

        # A message without a version uses the default version, which exists as long as the project exists.
        version = message.get("_version")
        if version is None:
            return False

        # Projects can also be defined via a [settings] section without an egg. Only check egg storage in that case.
        eggstorage = self.app.getComponent(IEggStorage)
        egg_version, egg_file = eggstorage.get(project, version)
        try:
            return (egg_version, egg_file) == (None, None)
        finally:
            # get() opens the egg file, which we don't need here; close it to avoid leaking a file descriptor.
            if egg_file is not None:
                egg_file.close()

    def _process_finished(self, _, slot):
        process = self.processes.pop(slot)
        process.end_time = datetime.datetime.now()
        self.finished.add(process)
        log.debug("Process slot {slot} vacated", slot=slot)

        self._get_message(slot)

    def _get_max_proc(self, config):
        max_proc = config.getint("max_proc", 0)
        if max_proc:
            return max_proc

        try:
            cpus = multiprocessing.cpu_count()
        except NotImplementedError:  # Windows 17520a3
            cpus = 1
        return cpus * config.getint("max_proc_per_cpu", 4)


# https://docs.twisted.org/en/stable/api/twisted.internet.protocol.ProcessProtocol.html
class ScrapyProcessProtocol(protocol.ProcessProtocol):  # noqa: PLW1641 missing __hash__ method
    def __init__(self, project, spider, job, env, args, *, version=None):
        self.project = project
        self.spider = spider
        self.job = job
        # The egg version used by the crawl, or None for the project's default version.
        self.version = version
        self.pid = None
        self.start_time = datetime.datetime.now()
        self.end_time = None
        self.args = args
        self.env = env
        self.deferred = defer.Deferred()

    # For equality assertions in tests.
    def __eq__(self, other):
        return (
            self.project == other.project
            and self.spider == other.spider
            and self.job == other.job
            and self.version == other.version
            and self.pid == other.pid
            and self.start_time == other.start_time
            and self.end_time == other.end_time
            and self.args == other.args
            and self.env == other.env
        )

    # For error messages in tests.
    def __repr__(self):
        return (
            f"ScrapyProcessProtocol(project={self.project} spider={self.spider} job={self.job} pid={self.pid} "
            f"start_time={self.start_time} end_time={self.end_time} version={self.version} "
            f"args={self.args} env={self.env})"
        )

    def outReceived(self, data):
        log.info(data.rstrip(), log_system=f"Launcher,{self.pid}/stdout")

    def errReceived(self, data):
        log.error(data.rstrip(), log_system=f"Launcher,{self.pid}/stderr")

    def connectionMade(self):
        self.pid = self.transport.pid
        self.log("info", "Process started:")

    # https://docs.twisted.org/en/stable/core/howto/process.html#things-that-can-happen-to-your-processprotocol
    def processEnded(self, status):
        if isinstance(status.value, error.ProcessDone):
            self.log("info", "Process finished:")
        else:
            self.log("error", f"Process died: exitstatus={status.value.exitCode!r}")
        self.deferred.callback(self)

    def log(self, level, action):
        getattr(log, level)(
            "{action} project={project!r} spider={spider!r} job={job!r} pid={pid!r} args={args!r}",
            action=action,
            project=self.project,
            spider=self.spider,
            job=self.job,
            pid=self.pid,
            args=self.args,
        )
