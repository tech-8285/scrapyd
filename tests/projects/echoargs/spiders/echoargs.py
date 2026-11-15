import json
import os
import time

from scrapy import Request, Spider


class EchoArgsSpider(Spider):
    """Report, in the job log and in a scraped item, what the crawl process received.

    Used by integration tests to check that the parameters accepted by the
    schedule webservice survive persistent queuing and reach the actual crawl
    process unchanged.
    """

    name = "echoargs"

    # Differs between the echoargs ("one") and echoargs2 ("two") eggs, so that
    # tests can tell which project version the crawl process actually ran.
    marker = "one"

    def start_requests(self):  # Scrapy < 2.13
        yield from self._start()

    async def start(self):  # Scrapy >= 2.13
        for request in self._start():
            yield request

    def _start(self):
        self.received = {
            "marker": self.marker,
            "scope": getattr(self, "scope", None),
            "job": getattr(self, "_job", None),
            "version_arg": getattr(self, "_version", None),
            "version_env": os.environ.get("SCRAPYD_EGG_VERSION"),
            "my_setting": self.settings.get("MY_SETTING"),
            "other_setting": self.settings.get("OTHER_SETTING"),
        }
        # Log before any delay, so that the parameters of a running job are
        # already visible in its log file.
        self.logger.info("echoargs received: %s", json.dumps(self.received, ensure_ascii=False, sort_keys=True))

        # "delay" keeps the job occupying a launcher slot, so that integration
        # tests can restart Scrapyd while this job is running.
        delay = float(getattr(self, "delay", 0) or 0)
        if delay:
            time.sleep(delay)

        yield Request("data:text/plain;charset=utf-8,", callback=self.parse)

    def parse(self, response):
        yield self.received
