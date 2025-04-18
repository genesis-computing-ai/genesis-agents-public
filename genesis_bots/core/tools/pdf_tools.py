import base64
import os
import requests
from genesis_bots.core.logging_config import logger
from genesis_bots.llm.llm_openai.openai_utils import get_openai_client
import json

from genesis_bots.core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)
import pymupdf

pdf_tools = ToolFuncGroup(
    name="pdf_tools",
    description="Tools to load and parse PDF files to simple text content for summarization and text processing. If the user provide a file with pdf extention or a URL to a PDF file, this function will parse the PDF file and return the text content.",
    lifetime="PERSISTENT",
)


@gc_tool(
    filepath=ToolFuncParamDescriptor(
        name="filepath",
        description="Path to the PDF file to parse",
        required=True,
        llm_type_desc=dict(type="string"),
    ),
    _group_tags_=[pdf_tools],
)
def pdf_parser(filepath: str) -> dict:
    """
    load and parse PDF files to simple text content for summarization and text processing.

    Args:
        filepath (str): Path to the PDF file to parse (local file or URL)
    """

    # check if the path is a local file or a URL
    if filepath.startswith("http"):
        # download the file from the URL
        response = requests.get(filepath)
        if response.status_code == 200:
            # save the file to a temporary location
            temp_file_path = os.path.join("tmp", os.path.basename(filepath))
            with open(temp_file_path, "wb") as f:
                f.write(response.content)
            filepath = temp_file_path
        else:
            logger.error(f"Failed to download file from URL: {filepath}")
            return {"Success": False, "Error": f"Failed to download file from URL: {filepath}"}

    try:
        # open the PDF file
        doc = pymupdf.open(filepath)
        # extract the text content
        text_data = []
        for page_num in range(doc.page_count):
            page = doc.load_page(page_num)
            text = page.get_text()
            text_data.append(text)
        doc.close()
        if len(text_data) == 0:
            return {"Success": False, "Error": "No text found in the PDF file."}
        text = "\n".join(text_data)
        return {"Success": True, "Content": text}
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return {"Success": False, "Error": f"An error occurred during parsing the pdf: {str(e)}"}

pdf_functions = [pdf_parser]

# Called from bot_os_tools.py to update the global list of functions
def get_pdf_functions():
    return pdf_functions
