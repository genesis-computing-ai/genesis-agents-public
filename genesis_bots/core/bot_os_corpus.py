import requests
import tempfile
import shutil
from genesis_bots.core.logging_config import logger

class FileCorpus:
    def process(self):
        # Base method to be overridden by subclasses
        raise NotImplementedError("Subclasses must implement this method")

class DatabaseStageFileCorpus(FileCorpus):
    def process(self):
        # Implementation for processing files from a database stage
        return ["processed_file_from_database_stage"]

class URLListFileCorpus(FileCorpus):
    def __init__(self, urls):
        self.urls = urls

    def download_file(self, url):
        # For file URLs, just open the file path directly
        if url.startswith('file://'):
            try:
                # Remove the 'file://' prefix and open the file directly
                from urllib.parse import unquote
                file_path = unquote(url[7:])
                return file_path #open(file_path, 'rb')
            except Exception as e:
                logger.info(f"Error opening {url}: {e}")
                return None
        else:
            # For non-file URLs, proceed with download
            try:
                with requests.get(url, stream=True) as r:
                    r.raise_for_status()
                    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                        shutil.copyfileobj(r.raw, tmp_file)
                        return tmp_file #open(tmp_file.name, 'rb')
            except requests.RequestException as e:
                logger.info(f"Error downloading {url}: {e}")
                return None

    def process(self):
        # Download and return the list of file pointers
        downloaded_files = []
        for url in self.urls:
            if url[:5] == 'file-':
                downloaded_files.append(url)
            else:
                file_pointer = self.download_file(url)
                if file_pointer is not None:
                    downloaded_files.append(file_pointer)
        return downloaded_files

class LocalListFileCorpus(FileCorpus):
    def __init__(self, file_locations):
        self.file_locations = file_locations

    def process(self):
        # Return the list of file locations
        return self.file_locations

#fc = URLListFileCorpus(["file:///Users/mglickman/Downloads/The%20Datapreneurs%20-%20Final%20v7.docx"])
#files = fc.process()
#logger.info(files)