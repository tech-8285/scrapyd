"""Test that the spider directory stays consistent with the actually running versions.

These tests upload, replace and delete versions of a project, and check that both the spider directory
(listspiders.json) and the spiders that actually run (verified through the items they scrape) match the
expected version, including that the spider list cache never serves stale content.

This module is named to sort after test_webservice.py, since it schedules real jobs, which permanently
affects the responses of daemonstatus.json, listjobs.json and listprojects.json for the rest of the run.

It requires Scrapyd to be run with ``items_dir`` set (for example, ``items_dir = items``), so that scraped
items can be fetched from the /items/ endpoint.
"""

import json
import time
from pathlib import Path

import pytest
import requests

from integration_tests import req

FIXTURES = Path(__file__).absolute().parent.parent / "tests" / "fixtures"

PROJECT = "quotesbot"
OTHER_PROJECT = "otherbot"

AUTH = ("hello12345", "67890world")


def load_egg(basename):
    with (FIXTURES / f"{basename}.egg").open("rb") as f:
        return f.read()


def get_json(method, path, **kwargs):
    response = req(method, path, **kwargs)
    data = response.json()
    data.pop("node_name")
    return data


def assert_ok(method, path, expected, **kwargs):
    assert get_json(method, path, **kwargs) == {"status": "ok", **expected}


def assert_error(method, path, message, **kwargs):
    assert get_json(method, path, **kwargs) == {"status": "error", "message": message}


def add_version(project, version, egg_basename, spiders):
    assert_ok(
        "post",
        "/addversion.json",
        {"project": project, "version": version, "spiders": spiders},
        data={"project": project, "version": version, "egg": load_egg(egg_basename)},
    )


def list_spiders(project, version=None):
    params = {"project": project}
    if version is not None:
        params["_version"] = version
    response = get_json("get", "/listspiders.json", params=params)
    assert response["status"] == "ok"
    return sorted(response["spiders"])


def schedule(project, spider, version=None):
    data = {"project": project, "spider": spider}
    if version is not None:
        data["_version"] = version
    response = get_json("post", "/schedule.json", data=data)
    assert response["status"] == "ok"
    return response["jobid"]


def wait_finished(project, jobid, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = get_json("get", "/listjobs.json", params={"project": project})
        for job in response["finished"]:
            if job["id"] == jobid:
                return job
        time.sleep(0.5)
    raise AssertionError(f"job {jobid} did not finish within {timeout}s")


def scraped_markers(project, jobid):
    """Return the markers scraped by a job, proving which egg version was actually loaded."""
    job = wait_finished(project, jobid)
    assert job["items_url"] is not None, "items_url is null: is items_dir configured?"
    response = req("get", job["items_url"])
    return sorted(json.loads(line)["marker"] for line in response.text.splitlines())


@pytest.fixture(scope="module", autouse=True)
def cleanup_projects():
    yield
    for project in (PROJECT, OTHER_PROJECT):
        requests.post("http://127.0.0.1:6800/delproject.json", auth=AUTH, data={"project": project}, timeout=10)


def test_upload_versions():
    add_version(PROJECT, "v1", "quotesbot_v1", spiders=2)
    add_version(PROJECT, "v2", "quotesbot_v2", spiders=2)
    add_version(OTHER_PROJECT, "v1", "otherbot", spiders=1)

    assert_ok("get", "/listversions.json", {"versions": ["v1", "v2"]}, params={"project": PROJECT})
    assert list_spiders(PROJECT, version="v1") == ["spider1", "spider2"]
    # The default version is the latest one (v2), in which spider2 was replaced by spider3.
    assert list_spiders(PROJECT) == ["spider1", "spider3"]
    assert list_spiders(OTHER_PROJECT) == ["spider1"]


def test_schedule_specific_and_default_versions():
    # The same spider name collects different records, depending on the version that actually runs.
    assert scraped_markers(PROJECT, schedule(PROJECT, "spider1", version="v1")) == ["quotesbot-v1-spider1"]
    assert scraped_markers(PROJECT, schedule(PROJECT, "spider1")) == ["quotesbot-v2-spider1"]
    # Baseline for the unrelated project, re-checked after the replacements and deletions below.
    assert scraped_markers(OTHER_PROJECT, schedule(OTHER_PROJECT, "spider1")) == ["otherbot-v1-spider1"]


def test_replace_version():
    # Re-upload version v2 with different content: spider3 is replaced by spider4.
    add_version(PROJECT, "v2", "quotesbot_v2_replacement", spiders=2)

    # The directory reflects the replacement, instead of serving the cached spider list.
    assert list_spiders(PROJECT) == ["spider1", "spider4"]
    assert list_spiders(PROJECT, version="v2") == ["spider1", "spider4"]
    assert list_spiders(PROJECT, version="v1") == ["spider1", "spider2"]

    # The default version runs the replacement egg, not the previously cached one.
    assert scraped_markers(PROJECT, schedule(PROJECT, "spider1")) == ["quotesbot-v2r-spider1"]
    # The removed spider is not schedulable anymore, from either the default or the replaced version.
    assert_error(
        "post",
        "/schedule.json",
        "spider 'spider3' not found",
        data={"project": PROJECT, "spider": "spider3"},
    )
    assert_error(
        "post",
        "/schedule.json",
        "spider 'spider3' not found",
        data={"project": PROJECT, "spider": "spider3", "_version": "v2"},
    )
    # The untouched version still loads its original content.
    assert scraped_markers(PROJECT, schedule(PROJECT, "spider1", version="v1")) == ["quotesbot-v1-spider1"]


def test_delete_version():
    assert_ok("post", "/delversion.json", {}, data={"project": PROJECT, "version": "v2"})

    assert_ok("get", "/listversions.json", {"versions": ["v1"]}, params={"project": PROJECT})
    # The default version falls back to v1, instead of serving the cached v2 spider list.
    assert list_spiders(PROJECT) == ["spider1", "spider2"]
    assert_error("get", "/listspiders.json", "version 'v2' not found", params={"project": PROJECT, "_version": "v2"})

    # The default version runs the v1 egg, not the deleted one.
    assert scraped_markers(PROJECT, schedule(PROJECT, "spider1")) == ["quotesbot-v1-spider1"]
    # Spiders that only existed in the deleted version are not schedulable from the cache.
    assert_error(
        "post",
        "/schedule.json",
        "spider 'spider4' not found",
        data={"project": PROJECT, "spider": "spider4"},
    )
    # The deleted version itself is not schedulable.
    assert_error(
        "post",
        "/schedule.json",
        "version 'v2' not found",
        data={"project": PROJECT, "spider": "spider1", "_version": "v2"},
    )


def test_other_project_unaffected():
    # The other project's directory and scraped records are unchanged by the operations above.
    assert_ok("get", "/listversions.json", {"versions": ["v1"]}, params={"project": OTHER_PROJECT})
    assert list_spiders(OTHER_PROJECT) == ["spider1"]
    assert_error(
        "post",
        "/schedule.json",
        "spider 'spider4' not found",
        data={"project": OTHER_PROJECT, "spider": "spider4"},
    )
    assert scraped_markers(OTHER_PROJECT, schedule(OTHER_PROJECT, "spider1")) == ["otherbot-v1-spider1"]
