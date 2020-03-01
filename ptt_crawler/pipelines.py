# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import pymysql
from ptt_crawler import settings

class PttCrawlerPipeline(object):
    def open_spider(self, spider):
        self._conn = pymysql.connect(host=settings.MYSQL_HOST,
            user=settings.MYSQL_USER,
            password=settings.MYSQL_PASSWORD,
            database=settings.MYSQL_DBNAME,
            charset='utf8',
            cursorclass=pymysql.cursors.DictCursor)
        self._cursor = self._conn.cursor()

    def process_item(self, item, spider):

        # 新增發文
        sql = """
            INSERT INTO article
            (authorId, title, publishedTime, content, canonicalUrl) VALUES
            (%s, %s, %s, %s, %s)
        """
        inputs = [
            item["author_id"], item["title"], item["published_time"], item["content"], item["canonical_url"]
        ]
        self._cursor.execute(sql, inputs)
        article_id = self._cursor.lastrowid

        # 新增推文資料
        for commentid in item["pushes"]:
            for ts in item["pushes"][commentid]:
                push_content = item["pushes"][commentid][ts][1]
                    
                sql = """
                    INSERT INTO response
                    (articleId, commentId, commentContent, commentTime) VALUES
                    (%s, %s, %s, %s)
                """
                inputs = [
                    article_id, commentid, push_content, ts
                ]
                self._cursor.execute(sql, inputs)
            
        self._conn.commit()

        return item
