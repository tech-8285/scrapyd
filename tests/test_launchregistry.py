import datetime
import os

import pytest
from zope.interface.verify import verifyObject

from scrapyd.config import Config
from scrapyd.interfaces import ILaunchRegistry
from scrapyd.launchregistry import (
    FAILED,
    LAUNCHING,
    ORPHANED,
    RUNNING,
    MemoryLaunchRegistry,
    SqliteLaunchRegistry,
)
from scrapyd.spiderqueue import SqliteSpiderQueue


@pytest.fixture
def config(tmp_path):
    eggs_dir = tmp_path / "eggs"
    eggs_dir.mkdir()
    (eggs_dir / "mybot").mkdir()
    return Config(values={"eggs_dir": str(eggs_dir), "dbs_dir": str(tmp_path / "dbs")})


@pytest.fixture
def queue(config):
    return SqliteSpiderQueue(config, "mybot")


@pytest.fixture
def registry(config):
    return SqliteLaunchRegistry(config)


def _add(queue, job="j1", priority=0.0):
    queue.add("spider1", priority=priority, _job=job)


class TestSqliteLaunchRegistry:
    def test_interface(self, registry):
        verifyObject(ILaunchRegistry, registry)

    def test_claim_moves_job_from_queue_to_registry(self, registry, queue):
        _add(queue, priority=5.0)
        assert queue.count() == 1

        message = registry.claim("mybot", queue)

        assert message["_job"] == "j1"
        assert queue.count() == 0
        entry = registry.get("mybot", "j1")
        assert entry.state == LAUNCHING
        assert entry.priority == 5.0
        assert entry.spider == "spider1"
        assert isinstance(entry.start_time, datetime.datetime)

    def test_claim_empty_queue_returns_none(self, registry, queue):
        assert registry.claim("mybot", queue) is None

    def test_claim_duplicate_job_id_keeps_queue_row(self, registry, queue):
        _add(queue, "j1")
        assert registry.claim("mybot", queue)["_job"] == "j1"

        # A second row with the same job ID (for example, resubmitted while the first record is active).
        _add(queue, "j1")
        assert registry.claim("mybot", queue) is None
        assert queue.count() == 1  # the duplicate is still pending, not swallowed
        assert registry.count_active() == 1

    def test_claim_is_durable_across_connections(self, config, queue):
        _add(queue)
        SqliteLaunchRegistry(config).claim("mybot", queue)

        reopened = SqliteLaunchRegistry(config)
        entry = reopened.get("mybot", "j1")
        assert entry is not None
        assert entry.state == LAUNCHING

    def test_mark_running_records_pid(self, registry, queue):
        _add(queue)
        registry.claim("mybot", queue)

        registry.mark_running("mybot", "j1", 4321)

        entry = registry.get("mybot", "j1")
        assert entry.state == RUNNING
        assert entry.pid == 4321

    def test_finish_removes_record(self, registry, queue):
        _add(queue)
        registry.claim("mybot", queue)

        registry.finish("mybot", "j1")

        assert registry.get("mybot", "j1") is None

    def test_fail_records_explicit_terminal_state(self, registry, queue):
        _add(queue)
        registry.claim("mybot", queue)

        registry.fail("mybot", "j1", OSError("boom"))

        entry = registry.get("mybot", "j1")
        assert entry.state == FAILED
        assert "boom" in entry.error
        assert isinstance(entry.end_time, datetime.datetime)
        assert queue.count() == 0
        assert registry.count_active() == 0

    def test_recover_requeues_never_started_job(self, config, queue, registry):
        _add(queue, priority=9.0)
        registry.claim("mybot", queue)
        assert queue.count() == 0

        requeued = SqliteLaunchRegistry(config).recover({"mybot": SqliteSpiderQueue(config, "mybot")})

        assert requeued == 1
        restored = SqliteSpiderQueue(config, "mybot").list()
        assert restored == [{"name": "spider1", "_job": "j1"}]
        priority = SqliteSpiderQueue(config, "mybot").q.conn.execute("SELECT priority FROM spider_queue").fetchone()[0]
        assert priority == 9.0
        assert SqliteLaunchRegistry(config).get("mybot", "j1") is None

    def test_recover_orphans_dead_running_process(self, config, queue, registry):
        _add(queue)
        registry.claim("mybot", queue)
        registry.mark_running("mybot", "j1", 2147480000)  # a pid that is not alive

        requeued = SqliteLaunchRegistry(config).recover({"mybot": SqliteSpiderQueue(config, "mybot")})

        assert requeued == 0
        entry = SqliteLaunchRegistry(config).get("mybot", "j1")
        assert entry.state == ORPHANED
        assert isinstance(entry.end_time, datetime.datetime)

    def test_recover_keeps_live_running_process(self, config, queue, registry):
        _add(queue)
        registry.claim("mybot", queue)
        registry.mark_running("mybot", "j1", os.getpid())

        requeued = SqliteLaunchRegistry(config).recover({"mybot": SqliteSpiderQueue(config, "mybot")})

        assert requeued == 0
        entry = SqliteLaunchRegistry(config).get("mybot", "j1")
        assert entry.state == RUNNING

    def test_recover_marks_failed_when_project_gone(self, config, queue, registry):
        _add(queue)
        registry.claim("mybot", queue)

        registry.recover({})  # project no longer known

        assert registry.get("mybot", "j1").state == FAILED

    def test_active_filters_by_project_and_state(self, registry, queue):
        _add(queue, "j1")
        _add(queue, "j2")
        registry.claim("mybot", queue)
        registry.claim("mybot", queue)
        registry.fail("mybot", "j2", "boom")

        active = list(registry.active())
        assert [entry.job for entry in active] == ["j1"]
        assert [entry.job for entry in registry.active("other")] == []
        assert {entry.job for entry in registry.list()} == {"j1", "j2"}


class TestMemoryLaunchRegistry:
    @pytest.fixture
    def memory_queue(self):
        return SqliteSpiderQueue(Config(values={"dbs_dir": ":memory:"}), "mybot")

    def test_interface(self):
        verifyObject(ILaunchRegistry, MemoryLaunchRegistry())

    def test_claim_and_lifecycle(self, memory_queue):
        registry = MemoryLaunchRegistry()
        _add(memory_queue)

        assert registry.claim("mybot", memory_queue)["_job"] == "j1"
        assert registry.count_active() == 1
        registry.mark_running("mybot", "j1", os.getpid())
        assert registry.get("mybot", "j1").state == RUNNING
        registry.finish("mybot", "j1")
        assert registry.get("mybot", "j1") is None

    def test_recover_is_noop(self, memory_queue):
        registry = MemoryLaunchRegistry()
        _add(memory_queue)
        registry.claim("mybot", memory_queue)

        assert registry.recover({"mybot": memory_queue}) == 0
        assert registry.count_active() == 1
