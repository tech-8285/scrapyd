"""Integration tests: crawl parameters must survive scheduling, persistence and restart.

The chain under test is::

    schedule.json -> ISpiderScheduler.schedule -> ISpiderQueue (SQLite, persisted)
    -> IPoller.pop -> Launcher -> ``scrapy crawl`` arguments -> crawl process

Each test starts a real Scrapyd instance in a temporary directory, deploys a
fixture project with two egg versions, schedules jobs through the HTTP API, and
checks what the crawl process actually received.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from integration_tests.params_harness import (
    ECHO_SPIDER,
    GATE_SPIDER,
    PROJECT,
    Scrapyd,
    parse_crawl_argv,
)

# Settings that collide on purpose: the webservice collapses repeated `setting`
# parameters into a dict, so the last value must win at every hop.
SETTINGS = [
    ("PARAMS_TAG", "first"),
    ("PARAMS_SPLIT", "alpha=beta=gamma"),
    ("PARAMS_TAG", "second"),
    ("PARAMS_UNICODE", "采集范围"),
]
RESOLVED_SETTINGS = {"PARAMS_TAG": "second", "PARAMS_SPLIT": "alpha=beta=gamma", "PARAMS_UNICODE": "采集范围"}

# Keywords with non-ASCII, whitespace, punctuation, and characters that are special
# in URLs, shells, JSON, SQL and form encoding.
SCOPE_KEYWORDS = [
    "北京",
    "São Paulo",
    "München",
    "Dublin, IE",
    "quo'te",
    'quo"te',
    "semi;colon",
    "back\\slash",
    "a+b c",
]


def make_scope(keywords):
    """The scope argument, JSON encoded the way a client would send it."""
    return json.dumps(list(keywords), ensure_ascii=False)


SCOPE = make_scope(SCOPE_KEYWORDS)


@pytest.fixture
def scrapyd(tmp_path):
    instance = Scrapyd(tmp_path / "scrapyd")
    instance.prepare()
    instance.start()
    try:
        yield instance
    finally:
        instance.close()


def assert_items(items, *, jobid, version, scope, tag=None, settings=RESOLVED_SETTINGS):
    """Check the crawl process's items against the request."""
    received, records = items[0], items[1:]

    assert received == {
        "kind": "received",
        "job": jobid,
        "version_arg": version,
        "project": PROJECT,
        "version_env": version,
        "marker": version,
        "scope": scope,
        "scope_raw": make_scope(scope),
        "tag": tag,
        "settings": settings,
    }
    assert [(record["kind"], record["index"], record["keyword"]) for record in records] == [
        ("record", index, keyword) for index, keyword in enumerate(scope)
    ]
    assert {record["job"] for record in records} == {jobid}


def test_scope_survives_persistence_and_restart(scrapyd):
    """A scheduled job's parameters reach the crawl process unchanged, even when the
    job waits in the persistent queue across a restart of the scheduling service."""
    # The gate job holds the only launcher slot, so the job below stays pending:
    # it is written to, and later read back from, the SQLite queue.
    scrapyd.start_gate()

    jobid = "作业-007 ß"
    tag = "tok en+ß/ç"
    scrapyd.schedule(
        jobid,
        version="v2",
        settings=[f"{key}={value}" for key, value in SETTINGS],
        args=[("scope", SCOPE), ("tag", tag)],
    )

    # While pending, the API and the queue database both report the submitted values.
    scrapyd.wait_for_status(jobid, "pending")
    [message] = scrapyd.queue_messages()
    assert message["_job"] == jobid
    assert message["name"] == ECHO_SPIDER
    assert message["_version"] == "v2"
    assert message["settings"] == RESOLVED_SETTINGS
    assert message["scope"] == SCOPE
    assert message["tag"] == tag

    [pending] = scrapyd.pending()
    assert pending == {
        "id": jobid,
        "project": PROJECT,
        "spider": ECHO_SPIDER,
        "version": "v2",
        "settings": RESOLVED_SETTINGS,
        "args": {"scope": SCOPE, "tag": tag},
    }

    # Restarting Scrapyd discards all in-memory state (and kills the gate job).
    # The pending job can now only be started from the database.
    scrapyd.restart()
    scrapyd.wait_for_status(jobid, "finished")

    # The crawl process received the parameters as spider arguments, and the
    # non-ASCII scope survived byte for byte.
    assert_items(scrapyd.items(jobid), jobid=jobid, version="v2", scope=SCOPE_KEYWORDS, tag=tag)

    # The command line shows they were passed verbatim, without a shell.
    spider, passed_settings, passed_args = parse_crawl_argv(scrapyd.spawn_argv(jobid))
    assert spider == ECHO_SPIDER
    assert [pair for pair in passed_settings if pair[0].startswith("PARAMS_")] == list(RESOLVED_SETTINGS.items())
    assert dict(passed_args)["scope"] == SCOPE
    assert dict(passed_args)["tag"] == tag
    assert dict(passed_args)["_version"] == "v2"

    # The job is reported as finished under the same ID the API returned.
    [finished] = [job for job in scrapyd.finished() if job["id"] == jobid]
    assert finished["items_url"] == f"/items/{PROJECT}/{ECHO_SPIDER}/{jobid}.jl"
    assert scrapyd.pending() == []


@pytest.mark.parametrize(
    ("requested", "marker", "version_env", "version_arg"),
    [
        (None, "v2", None, None),  # omitted: the default (latest) version, nothing passed
        ("v1", "v1", "v1", "v1"),  # explicit: exactly that version
        ("v2", "v2", "v2", "v2"),  # explicit default: still passed explicitly
    ],
)
def test_version_selection_reaches_the_process(scrapyd, requested, marker, version_env, version_arg):
    """An explicit project version selects that egg, and omitting it preserves the
    default behavior (egg storage's latest version, no version passed)."""
    assert scrapyd.list_versions() == ["v1", "v2"]

    jobid = f"version-{requested or 'default'}"
    scrapyd.schedule(jobid, version=requested, args=[("scope", make_scope(["version-check"]))])
    scrapyd.wait_for_status(jobid, "finished")

    received = scrapyd.items(jobid)[0]
    assert received["marker"] == marker
    assert received["version_env"] == version_env
    assert received["version_arg"] == version_arg
    assert received["project"] == PROJECT
    assert received["scope"] == ["version-check"]

    # Version selection does not change the listing behavior either.
    assert scrapyd.list_spiders(version="v1") == sorted([ECHO_SPIDER, GATE_SPIDER])
    assert scrapyd.list_spiders(version="v2") == sorted([ECHO_SPIDER, GATE_SPIDER])
    assert scrapyd.list_spiders() == sorted([ECHO_SPIDER, GATE_SPIDER])


@pytest.mark.parametrize("order", ["ab", "ba"])
def test_concurrent_similar_jobs_do_not_share_parameters(scrapyd, order):
    """Two jobs that differ only in their parameters must not overwrite each other's
    values, nor receive each other's values, including across a restart."""
    common_scope = make_scope(["shared-keyword"])
    jobs = {
        "job-a": {
            "version": "v1",
            "settings": [("PARAMS_TAG", "alpha"), ("PARAMS_UNICODE", "第一")],
            "tag": "tag-a",
        },
        "job-b": {
            "version": "v2",
            "settings": [("PARAMS_TAG", "beta"), ("PARAMS_UNICODE", "第二")],
            "tag": "tag-b",
        },
    }
    ordered = [f"job-{letter}" for letter in order]

    # Hold the only launcher slot, so both jobs stay pending.
    scrapyd.start_gate()

    def submit(jobid):
        job = jobs[jobid]
        scrapyd.schedule(
            jobid,
            version=job["version"],
            settings=[f"{key}={value}" for key, value in job["settings"]],
            args=[("scope", common_scope), ("tag", job["tag"])],
        )

    # Submit both jobs at the same time, so their requests interleave in the service.
    with ThreadPoolExecutor(max_workers=len(ordered)) as executor:
        futures = [executor.submit(submit, jobid) for jobid in ordered]
        for future in futures:
            future.result()

    for jobid in ordered:
        scrapyd.wait_for_status(jobid, "pending")

    # Both are stored side by side, each with its own values.
    pending = {job["id"]: job for job in scrapyd.pending()}
    assert set(pending) == set(ordered)
    for jobid in ordered:
        job = jobs[jobid]
        assert pending[jobid] == {
            "id": jobid,
            "project": PROJECT,
            "spider": ECHO_SPIDER,
            "version": job["version"],
            "settings": dict(job["settings"]),
            "args": {"scope": common_scope, "tag": job["tag"]},
        }

    # Restart while both are pending: the gate job is killed, freeing the slot.
    scrapyd.restart()

    for jobid in ordered:
        scrapyd.wait_for_status(jobid, "finished")

    spawned = {}
    for jobid in ordered:
        job = jobs[jobid]
        items = scrapyd.items(jobid)
        assert_items(
            items,
            jobid=jobid,
            version=job["version"],
            scope=["shared-keyword"],
            tag=job["tag"],
            settings=dict(job["settings"]),
        )
        # The scope was identical, yet every item belongs to one job only.
        assert {item["job"] for item in items} == {jobid}
        spawned[jobid] = scrapyd.spawn_argv(jobid)

    for jobid in ordered:
        spider, passed_settings, passed_args = parse_crawl_argv(spawned[jobid])
        assert spider == ECHO_SPIDER
        # Each job's own settings, tag and version, not the other job's.
        assert [pair for pair in passed_settings if pair[0].startswith("PARAMS_")] == jobs[jobid]["settings"]
        assert dict(passed_args)["tag"] == jobs[jobid]["tag"]
        assert dict(passed_args)["_version"] == jobs[jobid]["version"]
        assert dict(passed_args)["scope"] == common_scope

    # Every job is finished exactly once, and no pending job is left behind.
    assert sorted(job["id"] for job in scrapyd.finished()) == sorted(ordered)
    assert scrapyd.pending() == []


def test_jobs_without_a_job_id_get_distinct_ids_and_parameters(scrapyd):
    """Jobs scheduled without a job ID are assigned unique IDs, and their parameters
    stay attached to the right ID."""
    first_id = scrapyd.schedule(args=[("scope", make_scope(["uno"]))])
    second_id = scrapyd.schedule(version="v1", args=[("scope", make_scope(["dos"]))])
    assert first_id != second_id

    for jobid in (first_id, second_id):
        scrapyd.wait_for_status(jobid, "finished")

    # The API-assigned IDs, not positional matching, are what the jobs report.
    assert scrapyd.items(first_id)[0]["scope"] == ["uno"]
    assert scrapyd.items(second_id)[0]["scope"] == ["dos"]
    assert scrapyd.items(first_id)[0]["job"] == first_id
    assert scrapyd.items(second_id)[0]["job"] == second_id
