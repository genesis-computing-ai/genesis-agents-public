from genesis_bots.core.bot_os_tools2 import (BOT_ID_IMPLICIT_FROM_CONTEXT, THREAD_ID_IMPLICIT_FROM_CONTEXT,
                                            ToolFuncGroup, gc_tool)
import json
from spider import Spider
from genesis_bots.connectors import get_global_db_connector
from genesis_bots.core.logging_config import logger
import os
import requests


# Define tool group for web access functions
web_access_tools = ToolFuncGroup(
    name="web_access_tools",
    description="Tools for accessing and searching web content, including Google search and web scraping capabilities",
    lifetime="PERSISTENT",
)

class WebAccess(object):
    _instance = None
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(WebAccess, cls).__new__(cls)
        return cls._instance

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
            if os.environ.get('SERPER_API_KEY', None):
                self.serper_api_key = os.environ.get('SERPER_API_KEY')
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
            if os.environ.get('SPIDER_API_KEY', None):
                self.serper_api_key = os.environ.get('SPIDER_API_KEY')
                return True
            return False

    def serper_search_api(self, query, search_type):
        if search_type == 'set_key':
            result = self.db_adapter.set_api_config_params('serper', json.dumps({"api_key": query}))
            if result["Success"]:
                self.serper_api_key = query
                return {'success': True, 'data': 'API key set successfully'}
            return {'success': False, 'error': f'Failed to set API key: {result.get("Error", "Unknown error")}'}
        if self.serper_api_key is not None or self.set_serper_api_key():
            url = f"https://google.serper.dev/{search_type}"
            payload = json.dumps({"q": query})
            headers = {
                'X-API-KEY': self.serper_api_key,
                'Content-Type': 'application/json'
            }
            response = requests.request("POST", url, headers=headers, data=payload)
            data = response.text
            logger.debug(data)
            return {'success': True, 'data': json.loads(data)}
        return {
            'success': False,
            'error': 'Serper API key not set. You can ask the user to obtain a free key at https://serper.dev and then give it to you, and then you can set the key programmatically by calling this function with search_type="set_key" and passing the API key as the query parameter if the user provides it to you.'
        }

    def serper_scrape_api(self, url):
        if self.serper_api_key is not None or self.set_serper_api_key():
            payload = json.dumps({"url": url})
            headers = {
                'X-API-KEY': self.serper_api_key,
                'Content-Type': 'application/json'
            }
            response = requests.request("POST", "https://scrape.serper.dev", headers=headers, data=payload)
            data = response.text
            return {'success': True, 'data': json.loads(data)}
        return {
            'success': False,
            'error': 'Serper API key not set. You can ask the user to obtain a free key at https://serper.dev and then give it to you, and then you can set the key programmatically by calling search_google function with search_type="set_key" and passing the API key as the query parameter if the user provides it to you.'
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

web_access = WebAccess(get_global_db_connector())

@gc_tool(
    query="Query string to search in Google",
    search_type="Search type (search, images, videos, places, maps, news, shopping, scholar, patent)",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[web_access_tools]
)
def _search_google(
    query: str,
    search_type: str,
    bot_id: str = None,
    thread_id: str = None
) -> dict:
    """
    Perform a Google search using the Serper API and depending on the search type:
        - search: returns organic results
        - images: returns Google image results
        - videos: returns Google video results
        - places: returns Google place results
        - maps: returns Google map results
        - news: returns Google news results
        - shopping: returns Google shopping results
        - scholar: returns Google scholar results
        - patent: returns Google patent results

    Returns:
        dict: Google search results including organic results, knowledge graph, etc.
    """
    return web_access.serper_search_api(query, search_type)

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
    Scrape content from a specific URL using Serper API

    Returns:
        dict: Scraped content from the webpage
    """
    return web_access.serper_scrape_api(url)

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
    return web_access.crawl_url(url, **(crawler_params or {}))

# List of all web access tool functions
_all_web_access_functions = [
    _search_google,
    _scrape_url,
    # _crawl_url,
    ]


def get_web_access_functions():
    """Return all registered web access tool functions"""
    return _all_web_access_functions
