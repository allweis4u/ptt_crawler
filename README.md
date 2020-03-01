
# 使用scrapy來做ptt爬蟲擷取

## 基本說明
* 從ptt熱門看板開始擷取網站資料，ptt_crawler/settings.py是scrapy的設定檔，可設定擷取網頁間隔時間、DB設定等等。
* requirements.py是本系統內python會安裝到的套件。

## 啟用爬蟲指令
* scrapy crawl ptt

## 主要功能
* 取得發文作者、標題、時間、發文內容
* 取得推文者編號、推文內容、推文時間
* 儲存資料至資料庫中

## 系統
* 使用MySQL 5.7資料庫
* 使用Python 3.7版
