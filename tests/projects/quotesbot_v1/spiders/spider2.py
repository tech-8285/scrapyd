import scrapy


class Spider2(scrapy.Spider):
    name = "spider2"

    async def start(self):
        yield scrapy.Request("data:text/plain,quotesbot-v1-spider2")

    def parse(self, response):
        yield {"marker": response.text}
