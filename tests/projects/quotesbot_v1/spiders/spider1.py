import scrapy


class Spider1(scrapy.Spider):
    name = "spider1"

    async def start(self):
        yield scrapy.Request("data:text/plain,quotesbot-v1-spider1")

    def parse(self, response):
        yield {"marker": response.text}
