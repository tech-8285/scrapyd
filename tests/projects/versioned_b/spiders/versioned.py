import scrapy


class VersionedSpider(scrapy.Spider):
    name = "versioned"
    marker = "versioned-b"

    async def start(self):
        self.logger.info("version marker: %s", self.marker)
        yield {"marker": self.marker}

    def parse(self, response):
        yield {"marker": self.marker}
