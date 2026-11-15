import asyncio
import time
from contextlib import suppress
from pathlib import Path

from scrapy import Request, Spider
from scrapy.http import TextResponse


class TraceSpider(Spider):
    """
    Record which job actually ran, so that the crawl records can be compared with the dispatch order.

    The spider fetches a ``data:`` URI, which Scrapy's built-in data URI download handler serves without any network
    access, and it yields one item per crawl. The ``payload`` argument lets a test tell apart a re-run of the same job.

    The crawls are optionally gated by files in the directory named by the ``barrier`` argument, so that a test can
    accept several jobs while they are pending, and then let them run:

    - A crawl that starts writes ``started-{job_id}``.
    - The crawl then waits for ``release`` to exist. The wait is bounded by ``wait`` seconds, so that a test failure
      can't leave a crawl hanging.
    """

    name = "trace"

    def __init__(self, payload="payload", barrier=None, wait="30", **kwargs):
        super().__init__(**kwargs)
        self.payload = payload
        self.barrier = Path(barrier) if barrier else None
        self.wait = float(wait)
        # The launcher passes the job ID as a spider argument.
        self.job_id = kwargs.get("_job", "")

    def _mark_started(self):
        if self.barrier is not None:
            (self.barrier / f"started-{self.job_id}").touch()

    def _released(self):
        return self.barrier is None or (self.barrier / "release").exists()

    async def start(self):
        self._mark_started()
        # Wait in one step, rather than polling, so that the crawl holds for exactly the released state.
        with suppress(TimeoutError):
            await asyncio.wait_for(self._wait_for_release(), self.wait)
        yield self._request()

    async def _wait_for_release(self):
        if self.barrier is None:
            return
        path = self.barrier / "release"
        # A file can't be waited on without polling, which is the whole point of the barrier.
        while not path.exists():  # noqa: ASYNC110
            await asyncio.sleep(0.02)

    def start_requests(self):
        """Kept for Scrapy versions before 2.13, which don't call the ``start`` method."""
        self._mark_started()
        deadline = time.time() + self.wait
        while self.barrier is not None and not (self.barrier / "release").exists() and time.time() < deadline:
            time.sleep(0.05)
        yield self._request()

    def _request(self):
        return Request("data:text/plain,ok", callback=self.parse, dont_filter=True)

    def parse(self, response: TextResponse):
        yield {"job_id": self.job_id, "payload": self.payload, "body": response.text}
