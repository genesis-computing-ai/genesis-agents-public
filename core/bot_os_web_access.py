import http.client
import json
from spider import Spider



class WebAccess(object):
    def __init__(self, db_adapter):
        self.db_adapter = db_adapter
        self.serper_api_key = None
        self.spider_api_key = None
        self.spider_app = None

    def set_serper_api_key(self):
        if self.serper_api_key is None:
            query = f"""SELECT value FROM {self.db_adapter.schema}.EXT_SERVICE_CONFIG 
                      WHERE ext_service_name = 'serper' AND parameter = 'api_key';"""
            rows = self.db_adapter.run_query(query)
            if rows:
                self.serper_api_key = rows[0]['VALUE']
                return True
            return False

    def set_spider_api_key(self):
        if self.spider_api_key is None:
            query = f"""SELECT value FROM {self.db_adapter.schema}.EXT_SERVICE_CONFIG 
                      WHERE ext_service_name = 'spider' AND parameter = 'api_key';"""
            rows = self.db_adapter.run_query(query)
            if rows:
                self.spider_api_key = rows[0]['VALUE']
                self.spider_app = Spider(api_key=self.spider_api_key)
                return True
            return False

    def search_google(self, query):
        if self.serper_api_key is not None or self.set_serper_api_key():
            conn = http.client.HTTPSConnection("google.serper.dev")
            payload = json.dumps({"q": query})
            headers = {
                'X-API-KEY': self.serper_api_key,
                'Content-Type': 'application/json'
            }
            conn.request("POST", "/search", payload, headers)
            res = conn.getresponse()
            data = res.read()
            print(data.decode("utf-8"))
            return json.loads(data)
        return 'API key not set'
            

    def scrape_url(self, url): 
        if self.spider_api_key is not None or self.set_spider_api_key():
            scraped_data = self.spider_app.scrape_url(url)
            return scraped_data
        return 'API key not set'


    def crawl_url(self, url, **crawler_params):
        if self.spider_api_key is not None or self.set_spider_api_key():
            crawl_result = self.spider_app.crawl_url(url, params=crawler_params)
            return crawl_result
        return 'API key not set'
