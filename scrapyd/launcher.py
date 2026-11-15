import datetime
import multiprocessing
import sys
from itertools import chain

from twisted.application.service import Service
from twisted.internet import defer, error, protocol, reactor
from twisted.logger import Logger

from scrapyd import __version__
from scrapyd.interfaces import IEnvironment, IJobStorage, ILaunchRegistry, IPoller

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
        self.registry = app.getComponent(ILaunchRegistry)
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
        self._recover()
        for slot in range(self.max_proc):
            self._get_message(slot)

    def _recover(self):
        """
        Reconcile jobs that left the spider queue but never reached a known state in a previous process.

        Never-started jobs return to their spider queue (pending), proven-running survivors stay running, and processes
        that died without reaching the finished-job storage get an explicit terminal state.
        """
        poller = self.app.getComponent(IPoller)
        requeued = self.registry.recover(poller.queues)
        if requeued:
            log.warn(  # noqa: G010 Twisted's Logger provides warn(), not warning()
                "Recovered {count} job(s) that were dequeued but never started; they are pending again.",
                count=requeued,
                log_system="Launcher",
            )
        for entry in self.registry.active():
            if entry.state == "running":
                log.warn(  # noqa: G010 Twisted's Logger provides warn(), not warning()
                    "Job {job!r} of project {project!r} is still running as pid {pid}; it is not started again.",
                    job=entry.job,
                    project=entry.project,
                    pid=entry.pid,
                    log_system="Launcher",
                )

    def _get_message(self, slot):
        poller = self.app.getComponent(IPoller)
        poller.next().addCallback(self._spawn_process, slot)
        log.debug("Process slot {slot} ready", slot=slot)

    def _spawn_process(self, message, slot):
        project = message["_project"]
        job = message["_job"]
        environment = self.app.getComponent(IEnvironment)
        message.setdefault("settings", {})
        message["settings"].update(environment.get_settings(message))

        env = environment.get_environment(message, slot)
        args = [sys.executable, "-m", self.runner, "crawl", *get_crawl_args(message)]

        process = ScrapyProcessProtocol(project, message["_spider"], job, env, args, self.registry)
        process.deferred.addBoth(self._process_finished, slot)

        try:
            reactor.spawnProcess(process, sys.executable, args=args, env=env)
        except OSError as e:
            # The crawl process never started. Record an explicit terminal state, release the slot and let another
            # pending job use it: the job is neither silently lost nor holding a slot forever.
            self.registry.fail(project, job, e)
            log.failure(
                "Failed to start process for project={project!r} spider={spider!r} job={job!r}",
                project=project,
                spider=process.spider,
                job=job,
                log_system="Launcher",
            )
            self._get_message(slot)
            return

        self.processes[slot] = process
        log.debug("Process slot {slot} occupied", slot=slot)

    def _process_finished(self, _, slot):
        process = self.processes.pop(slot)
        process.end_time = datetime.datetime.now()
        self.registry.finish(process.project, process.job)
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
    def __init__(self, project, spider, job, env, args, registry=None):
        self.project = project
        self.spider = spider
        self.job = job
        self.pid = None
        self.start_time = datetime.datetime.now()
        self.end_time = None
        self.args = args
        self.env = env
        self.registry = registry
        self.deferred = defer.Deferred()

    # For equality assertions in tests.
    def __eq__(self, other):
        return (
            self.project == other.project
            and self.spider == other.spider
            and self.job == other.job
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
            f"start_time={self.start_time} end_time={self.end_time} args={self.args} env={self.env})"
        )

    def outReceived(self, data):
        log.info(data.rstrip(), log_system=f"Launcher,{self.pid}/stdout")

    def errReceived(self, data):
        log.error(data.rstrip(), log_system=f"Launcher,{self.pid}/stderr")

    def connectionMade(self):
        self.pid = self.transport.pid
        # The crawl process is now proven to have started. Promote the durable record so that a crash after this point
        # is not mistaken for a never-started (and therefore safely re-runnable) job on recovery.
        if self.registry is not None:
            self.registry.mark_running(self.project, self.job, self.pid)
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
