import scrapy


class Spider4(scrapy.Spider):
    name = "spider4"

    async def start(self):
        yield scrapy.Request("data:text/plain,quotesbot-v2r-spider4")

    def parse(self, response):
        yield {"marker": response.text}
