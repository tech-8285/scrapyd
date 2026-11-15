import scrapy


class Spider3(scrapy.Spider):
    name = "spider3"

    async def start(self):
        yield scrapy.Request("data:text/plain,quotesbot-v2-spider3")

    def parse(self, response):
        yield {"marker": response.text}
