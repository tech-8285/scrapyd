import asyncio
import time
from pathlib import Path

from scrapy import Spider


class RecordSpider(Spider):
    """Record the order in which crawls run.

    The spider accepts the ``record`` argument (a file path) and the ``sleep`` argument (the number of seconds to keep
    the crawl running). It appends the job ID to the file when the crawl starts.
    """

    name = "record"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sleep = float(getattr(self, "sleep", 0))

        record = getattr(self, "record", None)
        if record:
            with Path(record).open("a", encoding="utf-8") as f:
                f.write(f"{self._job}\n")

    async def start(self):
        # Keep the crawl running, so that it occupies the launcher's process slot.
        if self.sleep:
            await asyncio.sleep(self.sleep)

    def start_requests(self):
        """Support Scrapy versions older than 2.13, which use ``start_requests`` instead of ``start``."""
        if self.sleep:
            time.sleep(self.sleep)
        return []
