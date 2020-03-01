# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class PttCrawlerItem(scrapy.Item):
    # define the fields for your item here like:
    author_id = scrapy.Field()
    author_name = scrapy.Field()
    title = scrapy.Field()
    published_time = scrapy.Field()
    content = scrapy.Field()
    canonical_url = scrapy.Field()
    
    pushes = scrapy.Field()
