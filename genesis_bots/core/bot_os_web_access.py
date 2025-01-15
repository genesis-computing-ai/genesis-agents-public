from genesis_bots.core.bot_os_tools2 import (BOT_ID_IMPLICIT_FROM_CONTEXT, THREAD_ID_IMPLICIT_FROM_CONTEXT,
                                            ToolFuncGroup, gc_tool)
import http.client
import json
from spider import Spider
from genesis_bots.connectors.database_tools import DatabaseConnector
from genesis_bots.connectors import get_global_db_connector

# Define tool group for web access functions
web_access_tools = ToolFuncGroup(
    name="web_access_tools",
    description="Tools for accessing and searching web content, including Google search and web scraping capabilities",
    lifetime="PERSISTENT",
)

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
            return {
                'success': True,
                'data': json.loads(data)
            }
        return {
            'success': False,
            'error': 'Serper API key not set. You can obtain a key at https://serper.dev and set it via the Genesis GUI on the "Setup Webaccess API Keys" page.'
        }

    def scrape_url(self, url): 
        if self.spider_api_key is not None or self.set_spider_api_key():
            scraped_data = self.spider_app.scrape_url(url)
            return {
                'success': True,
                'data': scraped_data
            }
        return {
            'success': False,
            'error': 'Spider API key not set. You can obtain a key at https://spiderapi.com and set it via the Genesis GUI on the "Setup Webaccess API Keys" page.'
        }

    def crawl_url(self, url, **crawler_params):
        if self.spider_api_key is not None or self.set_spider_api_key():
            crawl_result = self.spider_app.crawl_url(url, params=crawler_params)
            return {
                'success': True,
                'data': crawl_result
            }
        return {
            'success': False,
            'error': 'Spider API key not set. You can obtain a key at https://spiderapi.com and set it via the Genesis GUI on the "Setup Webaccess API Keys" page.'
        }

@gc_tool(
    query="Search query string to send to Google",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[web_access_tools]
)
def _search_google(
    query: str,
    bot_id: str = None,
    thread_id: str = None
) -> dict:
    """
    Perform a Google search using the Serper API
    
    Returns:
        dict: Google search results including organic results, knowledge graph, etc.
    """
    db_adapter = get_global_db_connector()
    web_access = WebAccess(db_adapter)
    return web_access.search_google(query)

@gc_tool(
    url="URL of the webpage to scrape",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[web_access_tools]
)
def _scrape_url(
    url: str,
    bot_id: str = None,
    thread_id: str = None
) -> dict:
    """
    Scrape content from a specific URL using Spider API
    
    Returns:
        dict: Scraped content from the webpage
    """
    db_adapter = get_global_db_connector()
    web_access = WebAccess(db_adapter)
    return web_access.scrape_url(url)

@gc_tool(
    url="URL to crawl",
    crawler_params="Optional parameters for the crawler",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[web_access_tools]
)
def _crawl_url(
    url: str,
    crawler_params: dict = None,
    bot_id: str = None,
    thread_id: str = None
) -> dict:
    """
    Crawl a URL and its linked pages using Spider API
    
    Returns:
        dict: Crawl results including content from multiple pages
    """
    db_adapter = get_global_db_connector()
    web_access = WebAccess(db_adapter)
    return web_access.crawl_url(url, **(crawler_params or {}))

# List of all web access tool functions
_all_web_access_functions = (
    _search_google,
    _scrape_url,
    _crawl_url,
)

def get_web_access_functions():
    """Return all registered web access tool functions"""
    return _all_web_access_functions
