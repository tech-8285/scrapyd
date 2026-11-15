"""Spiders that report the parameters received by the crawl process.

Both spiders emit items from ``Spider.start()`` (Scrapy >= 2.13), so they need
no downloader, and the item feed written by Scrapyd is the record of what the
process actually received.
"""

import asyncio
import json
import os
from pathlib import Path

import scrapy

from paramsbot.version_marker import MARKER

LEGACY_MESSAGE = "These integration tests require Scrapy >= 2.13 (async Spider.start() support)."


class EchoSpider(scrapy.Spider):
    """Emit the received spider arguments, settings, environment and version marker,
    followed by one item per keyword of the requested scope."""

    name = "echo"
    scope = "[]"
    tag = None

    async def start(self):
        scope = json.loads(self.scope)
        yield {
            "kind": "received",
            "job": getattr(self, "_job", None),
            "version_arg": getattr(self, "_version", None),
            "project": os.environ.get("SCRAPY_PROJECT"),
            "version_env": os.environ.get("SCRAPYD_EGG_VERSION"),
            "marker": MARKER,
            "scope": scope,
            "scope_raw": self.scope,
            "tag": self.tag,
            "settings": {key: value for key, value in self.settings.items() if key.startswith("PARAMS_")},
        }
        for index, keyword in enumerate(scope):
            yield {
                "kind": "record",
                "job": getattr(self, "_job", None),
                "index": index,
                "keyword": keyword,
            }

    def start_requests(self):
        raise RuntimeError(LEGACY_MESSAGE)


class GateSpider(scrapy.Spider):
    """Hold the only launcher slot until the test creates the release file."""

    name = "gate"
    wait_for = None

    async def start(self):
        if self.wait_for:
            # Poll the filesystem: the release is created by another process.
            while not Path(self.wait_for).exists():  # noqa: ASYNC110, ASYNC240
                await asyncio.sleep(0.05)
        yield {"kind": "gate", "job": getattr(self, "_job", None)}

    def start_requests(self):
        raise RuntimeError(LEGACY_MESSAGE)
