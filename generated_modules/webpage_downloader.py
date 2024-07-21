import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json


# Function to make HTTP request and get the entire content
def get_webpage_content(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.content  # Return the entire content


# Function for parsing HTML content, extracting links, and then chunking the beautified content
def parse_and_chunk_content(content, base_url, chunk_size=256 * 1024):
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


# Main function to download webpage, extract links, and ensure each part is within the size limit
def download_webpage(url, chunk_index=0):
    try:
        content = get_webpage_content(url)
        chunks, total_chunks = parse_and_chunk_content(content, url)
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


# Start of Generated Description
TOOL_FUNCTION_DESCRIPTION_WEBPAGE_DOWNLOADER = {
    "type": "function",
    "function": {
        "name": "webpage_downloader--download_webpage",
        "description": "Downloads a webpage and returns its HTML content and hyperlinks in chunks, ensuring each chunk does not exceed 512KB. Allows specifying a chunk index to download specific parts of the beautified content. This tool is particularly useful for large and complex webpages and utilizes BeautifulSoup for parsing. It might require multiple sequential chunk downloads to capture the complete content relevant to the user's request.",
        "parameters": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "The URL of the webpage to download.",
                },
                "chunk_index": {
                    "type": "integer",
                    "default": 0,
                    "description": "The specific chunk index to download, with each chunk being up to 512KB in size. Defaults to the first chunk (0) if not specified.",
                },
            },
            "required": ["url"],
        },
    },
}

webpage_tools = {"webpage_downloader--download_webpage": download_webpage}
webpage_downloader_action_function_mapping = {
    "webpage_downloader--download_webpage": download_webpage
}
