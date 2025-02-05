# Main function to download webpage, extract links, and ensure each part is within the size limit
import json
import os
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from typing import Optional
from textwrap import dedent

from genesis_bots.core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

webpage_downloader = ToolFuncGroup(
    name="webpage_downloader",
    description=dedent("""
        Downloads a webpage and returns its HTML content and hyperlinks in chunks, ensuring each chunk does not
        exceed 512KB. Allows specifying a chunk index to download specific parts of the beautified content. This tool is particularly
        useful for large and complex webpages and utilizes BeautifulSoup for parsing. It might require multiple sequential chunk
        downloads to capture the complete content relevant to the user's request.
    """),
    lifetime="PERSISTENT",
)


@gc_tool(
    url=ToolFuncParamDescriptor(
        name="url",
        description="The URL of the webpage to download.",
        required=True,
        llm_type_desc=dict(type="string"),
    ),
    chunk_index=ToolFuncParamDescriptor(
        name="chunk_index",
        description="The specific chunk index to download, with each chunk being up to 512KB in size. Defaults to the first chunk (0) if not specified.",
        required=False,
        llm_type_desc=dict(type="integer"),
    ),
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[webpage_downloader],
)
def download_webpage(
    url: str,
    chunk_index: int = 0,
    bot_id: str = None,
    thread_id: str = None,
) -> None:
    """
    Downloads a webpage and returns its HTML content and hyperlinks in chunks, ensuring each chunk
    does not exceed 512KB. Allows specifying a chunk index to download specific parts of the beautified content.
    This tool is particularly useful for large and complex webpages and utilizes BeautifulSoup for parsing. It might
    require multiple sequential chunk downloads to capture the complete content relevant to the user's request.
    """
    try:
        content = get_webpage_content(url)
        chunks, total_chunks = _parse_and_chunk_content(content, url)
        if chunk_index >= total_chunks:
            return {"error": "Requested chunk index exceeds available chunks."}

        response = {
            "chunk": chunks[chunk_index],
            "next_chunk_index": (
                chunk_index + 1 if chunk_index + 1 < total_chunks else None
            ),
            "total_chunks": total_chunks,
        }
        return response
    except Exception as e:
        return {"error": str(e)}


# Function to make HTTP request and get the entire content
def get_webpage_content(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.content  # Return the entire content

    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import time

    chrome_options = Options()
    chrome_options.add_argument(
        "--headless"
    )  # Run in headless mode (no browser window)
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    )

    current_file_path = os.path.abspath(__file__)
    logger.info(current_file_path)

    service = Service("../../chromedriver")
    # driver = webdriver.Chrome(service=service, options=chrome_options)
    driver = webdriver.Chrome(options=chrome_options)

    driver.get(url)
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
    except Exception as e:
        logger.info("Error: ", e)
        driver.quit()

    data = driver.page_source  # find_element(By.XPATH, '//*[@id="data-id"]').text
    logger.info(f"Data scraped from {url}: \n{data}\n")
    return data

# Function for parsing HTML content, extracting links, and then chunking the beautified content
def _parse_and_chunk_content(content, base_url, chunk_size=256 * 1024):
    soup = BeautifulSoup(content, "html.parser")
    links = [urljoin(base_url, a["href"]) for a in soup.find_all("a", href=True)]
    pretty_content = soup.prettify()
    encoded_content = pretty_content.encode("utf-8")
    encoded_links = json.dumps(links).encode("utf-8")

    # Combine the content and links
    combined_content = encoded_content + encoded_links

    # Chunk the combined content
    chunks = []
    for i in range(0, len(combined_content), chunk_size):
        chunks.append({"content": combined_content[i : i + chunk_size]})

    if not chunks:
        raise ValueError("No content available within the size limit.")

    return chunks, len(chunks)  # Return chunks and total number of chunks

webpage_downloader_functions = [download_webpage,]

# Called from bot_os_tools.py to update the global list of functions
def get_webpage_downloader_functions():
    return webpage_downloader_functions
