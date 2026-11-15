import asyncio
import time
from pathlib import Path

import scrapy


class CancelSpider(scrapy.Spider):
    """A controllable spider for cancellation race integration tests.

    Coordination happens through files, so that the test process (running the reactor) can
    deterministically pause the crawl at its start and release it again.

    Scrapy 2.13+ consumes the asynchronous ``start()`` method, while older versions call
    ``start_requests()``; both are provided. The barrier in ``start()`` awaits asynchronously,
    because the async iterator runs on the event loop and a blocking wait would stall the crawl.

    Spider arguments:
        ready_file: file touched once the crawl entered its start method.
        go_file: file whose appearance unblocks the crawl. When omitted, the crawl does not wait.
        item_id: value copied into every yielded item, to attribute items to a job.
        item_count: number of items to yield.
    """

    name = "cancel_spider"

    def __init__(self, ready_file="", go_file="", item_id="", item_count="2", **kwargs):
        super().__init__(**kwargs)
        self.ready_file = Path(ready_file) if ready_file else None
        self.go_file = Path(go_file) if go_file else None
        self.item_id = item_id
        self.item_count = int(item_count)

    def _initial_requests(self):
        for index in range(self.item_count):
            # data: URIs need no network access and exercise the full download/item pipeline.
            yield scrapy.Request(
                f"data:text/plain,record-{index}",
                callback=self.parse,
                dont_filter=True,
            )

    async def _wait_for_gate(self):
        if self.ready_file is not None:
            self.ready_file.touch()

        if self.go_file is not None:
            deadline = time.monotonic() + 60
            while not self.go_file.exists():
                if time.monotonic() > deadline:
                    raise RuntimeError(f"timed out waiting for {self.go_file}")
                await asyncio.sleep(0.05)

    async def start(self):
        await self._wait_for_gate()
        for request in self._initial_requests():
            yield request

    def start_requests(self):
        # Synchronous fallback for Scrapy versions older than 2.13, which do not call start().
        if self.ready_file is not None:
            self.ready_file.touch()
        if self.go_file is not None:
            deadline = time.monotonic() + 60
            while not self.go_file.exists():
                if time.monotonic() > deadline:
                    raise RuntimeError(f"timed out waiting for {self.go_file}")
                time.sleep(0.05)
        yield from self._initial_requests()

    def parse(self, response):
        yield {"item_id": self.item_id, "payload": response.text}
