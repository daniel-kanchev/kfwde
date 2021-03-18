import scrapy
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst
from datetime import datetime
from kfwde.items import Article
import requests
import json


class KfwdeSpider(scrapy.Spider):
    name = 'kfwde'
    start_urls = ['https://www.kfw.de/KfW-Konzern/Newsroom/Aktuelles/newspress/do.haupia?query=*:*&page=1&rows=1&'
                  'sortBy=relevance&sortOrder=desc&facet.filter.language=de&dymFailover=true&groups=1']
    allowed_domains = ['kfw.de']

    def parse(self, response):
        json_res = json.loads(requests.get(
            "https://www.kfw.de/KfW-Konzern/Newsroom/Aktuelles/newspress/do.haupia?query=*:*&page=1&rows=10000&"
            "sortBy=relevance&sortOrder=desc&facet.filter.language=de&dymFailover=true&groups=1").text)

        docs = json_res['results']
        for article in docs:
            link = article['link']
            if 'pub' in article.keys():
                date = article['pub']
            else:
                date = ''
            yield response.follow(link, self.parse_article, cb_kwargs=dict(date=date))

    def parse_article(self, response, date):
        if 'pdf' in response.url:
            return

        item = ItemLoader(Article())
        item.default_output_processor = TakeFirst()

        title = response.xpath('//h1/text()').get()
        if title:
            title = title.strip()

        content = response.xpath('//div[@class="text-image-container "]//text()').getall()
        content = [text for text in content if text.strip()]
        content = "\n".join(content).strip()

        item.add_value('title', title)
        item.add_value('date', date)
        item.add_value('link', response.url)
        item.add_value('content', content)

        return item.load_item()
