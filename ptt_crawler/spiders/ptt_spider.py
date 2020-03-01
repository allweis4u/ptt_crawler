import re
import scrapy
import pymysql
from urllib.parse import urlparse
from datetime import datetime
from bs4 import BeautifulSoup
from ptt_crawler.items import PttCrawlerItem
from ptt_crawler import settings

class PttSpider(scrapy.Spider):
    name = "ptt"

    def __init__(self):
        self._dev = True # 開發模式

        self._page = 1
        #self._start_url = "https://www.ptt.cc/bbs/nCoV2019/index.html"
        self._start_url = "https://www.ptt.cc/bbs/hotboards.html"

        parsed_uri = urlparse(self._start_url)
        self._domain = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)

        self._conn = pymysql.connect(host=settings.MYSQL_HOST,
            user=settings.MYSQL_USER,
            password=settings.MYSQL_PASSWORD,
            database=settings.MYSQL_DBNAME,
            charset='utf8',
            cursorclass=pymysql.cursors.DictCursor)
        self._cursor = self._conn.cursor()
        self._seen_urls = self._get_urls()

    # 起始呼叫
    def start_requests(self):
        yield scrapy.FormRequest(url=self._start_url,
            method='GET',
            callback=self.parse_hotboard)

    # 擷取熱門看板
    def parse_hotboard(self, response):
        try:
            hots = response.css(".b-list-container .b-ent")
            for h in hots:
                url = self._domain + h.css('::attr("href")').get()

                yield scrapy.Request(url, cookies={'over18': '1'}, callback=self.parse_list)

                if self._dev:
                    break
        except Exception as e:
            print(str(e))

    # 擷取列表頁
    def parse_list(self, response):

        try:
            screens = response.css('.r-ent')

            sum = 0
            for scr in screens:
                # 擷取網址
                href = scr.css('.title a::attr("href")').get()
                if href is None:
                    continue

                link = self._domain + href

                if link in self._seen_urls:
                    continue
                self._seen_urls.append(link)

                # 擷取內頁內文
                yield scrapy.FormRequest(url=link,
                    method='GET',
                    callback=self.parse_detail)

                sum += 1

            # 有處理筆數繼續往下一頁處理
            if sum > 0 and self._dev == False:
                # 取得上一頁資訊
                btns = response.css(".btn-group-paging .btn")
                btn_href = btns[1].css('::attr("href")').get()

                # 有上一頁資訊
                if btn_href:
                    prev_link = self._domain + btn_href

                    yield scrapy.FormRequest(url=prev_link,
                        method='GET',
                        callback=self.parse_list)

        except Exception as e:
            print(str(e))

    # 處理內頁資訊
    def parse_detail(self, response):
        #print("@url =", response.url)

        soup = BeautifulSoup(response.text, features='lxml')

        # 取得上方訊息
        metas = soup.find("div", id="main-content").find_all("div", class_="article-metaline")
        
        if len(metas) == 0:
            return

        # 取得作者資訊
        author_info = metas[0].find("span", class_="article-meta-value").get_text()
        
        # 找出作者名稱和編號
        pattern = "(.+)\((.*)\)"
        matches = re.findall(pattern, author_info)
        author_id = matches[0][0]
        author_name = matches[0][1]

        # 取得標題
        title = metas[1].find("span", class_="article-meta-value").get_text()

        # 取得時間
        time_info = metas[2].find("span", class_="article-meta-value").get_text()
        # 將日期轉換成timestamp
        ts = int(datetime.strptime(time_info, "%a %b %d %H:%M:%S %Y").timestamp())

        # 取得推文資訊
        push_infos = soup.find("div", id="main-content").find_all("div", class_="push")
        pushes = {}
        for tag in push_infos:
            userid = tag.find("span", class_="push-userid").get_text()
            push_content = tag.find("span", class_="push-content").get_text()[2:] # 去除前兩個字元": "
            push_ipdatetime = tag.find("span", class_="push-ipdatetime").get_text().strip()
            
            # 取得標題年分
            year = time_info.split(" ")[-1]

            tmps = push_ipdatetime.split(" ")
            push_time = year + "/" + tmps[1] + " " + tmps[2]
            ts = int(datetime.strptime(push_time, "%Y/%m/%d %H:%M").timestamp())
            
            if userid not in pushes:
                pushes[userid] = {}

            # 同時間推文合在一起
            if ts not in pushes[userid]:
                pushes[userid][ts] = [userid, push_content, ts]
            else:
                pushes[userid][ts][1] += push_content


        # 發文內容最後取得，因為要移除tag
        _main_content = soup.find("div", id="main-content")

        # 移除上方作者的tag
        for tag in _main_content.find_all("div", class_="article-metaline"):
            tag.extract()
        # 移除上方的tag
        _main_content.find("div", class_="article-metaline-right").extract()
        # 移除下方推文的tag
        for tag in _main_content.find_all("div", class_="push"):
            tag.extract()
        # 移除下方額外訊息
        for tag in _main_content.find_all("span", class_="f2"):
            tag.extract()

        content = _main_content.get_text().strip()

        item = PttCrawlerItem()
        item["author_id"] = author_id
        item["author_name"] = author_name
        item["title"] = title
        item["published_time"] = ts
        item["content"] = content
        item["canonical_url"] = response.url
        item["pushes"] = pushes
        yield item

    # 取得資料庫內的網址資訊
    def _get_urls(self):
        sql = """
            SELECT canonicalUrl
            FROM article
        """
        
        self._cursor.execute(sql)
        data = self._cursor.fetchall()

        res = [d["canonicalUrl"] for d in data]
        return res
