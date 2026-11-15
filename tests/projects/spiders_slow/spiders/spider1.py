import asyncio

from scrapy.spiders import Spider


class Spider1(Spider):
    name = "slow"

    async def start(self):
        # Keep the crawl running until the test cancels it (abnormal termination).
        await asyncio.sleep(300)
        if False:
            yield None
