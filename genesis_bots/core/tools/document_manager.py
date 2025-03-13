from genesis_bots.core.logging_config import logger
from llama_index.core import VectorStoreIndex, SimpleDirectoryReader
from llama_index.core import StorageContext, load_index_from_storage, load_indices_from_storage
from textwrap import dedent
import os

from genesis_bots.core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)


class DocumentManager(object):
    _instance = None
    _initialized = False  # Add initialization flag
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(DocumentManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if self._initialized:  # Skip if already initialized
            return
            
        self.storage_path = os.path.join(os.getcwd(), 'bot_git','storage')
        try:
            self.storage_context = StorageContext.from_defaults(persist_dir=self.storage_path)
        except Exception as e:
            self.storage_context = StorageContext.from_defaults()
            self.storage_context.persist(self.storage_path)
            
        self._index_cache = {}  # Add index cache
        self._initialized = True

    def list_of_indices(self):
        # First check if we have any cached indices
        if self._index_cache:
            return list(self._index_cache.keys())
        
        # If cache is empty, load from storage and populate cache
        indices = load_indices_from_storage(self.storage_context)
        for index in indices:
            self._index_cache[index.index_id] = index
        return [index.index_id for index in indices]
    
    def list_of_documents(self, index_id):
        index = self.load_index(index_id)
        docs = set()
        for doc_id, doc_val in index.ref_doc_info.items():
            docs.add(doc_val.metadata['file_path'])
        return docs
    
    def create_index(self, index_id=None):
        index = VectorStoreIndex([], storage_context=self.storage_context, index_id=index_id)
        self.storage_context.persist(self.storage_path)
        # Update cache with new index
        self._index_cache[index.index_id] = index
        return index.index_id
    
    def delete_index(self, index_id):
        try:
            # Get all node IDs associated with this index before deletion
            index = self.load_index(index_id)
            all_nodes = index.ref_doc_info.keys()
            
            # Delete from storage context
            if hasattr(self.storage_context, 'index_store'):
                self.storage_context.index_store.delete_index_struct(index_id)
            
            # Delete from vector store - both the index-specific and default store
            if hasattr(self.storage_context, 'vector_store'):
                # Delete from index-specific store
                self.storage_context.vector_store.delete(index_id)
                # Delete all nodes from default store
                for node_id in all_nodes:
                    self.storage_context.vector_store.delete(node_id)
            
            # Remove from cache
            if index_id in self._index_cache:
                del self._index_cache[index_id]
            
            # Persist the changes
            self.storage_context.persist(self.storage_path)
            
            return True
        except Exception as e:
            logger.error(f"Error deleting index {index_id}: {str(e)}")
            raise

    def load_index(self, index_id):
        # Cache the loaded index
        if index_id not in self._index_cache:
            self._index_cache[index_id] = load_index_from_storage(self.storage_context, index_id)
        return self._index_cache[index_id]

    def add_document(self, index_id, datapath):


        if datapath.startswith('BOT_GIT:'):
            repo_path = os.getenv('GIT_PATH', os.path.join(os.getcwd(), 'bot_git'))
            # Ensure repo_path ends with /
            if not repo_path.endswith('/'):
                repo_path = repo_path + '/'
            if datapath[len('BOT_GIT:'):].startswith('/'):
                datapath = 'BOT_GIT:' + datapath[len('BOT_GIT:')+1:]
            datapath = os.path.join(repo_path, datapath[len('BOT_GIT:'):])
        # Remove any double slashes that might occur from path joining
        datapath = os.path.normpath(datapath)

        if os.path.isfile(datapath):
            new_documents = SimpleDirectoryReader(input_files=[datapath]).load_data()
        elif os.path.isdir(datapath):
            new_documents = SimpleDirectoryReader(input_dir=datapath).load_data()
        else:
            raise Exception("Invalid path")
        index = self.load_index(index_id)
        for doc in new_documents:
            index.insert(doc)
        self.storage_context.persist(self.storage_path)

    def retrieve(self, query, index_id, top_n=3):
        index = self.load_index(index_id)
        retriever = index.as_retriever(similarity_top_k=top_n)
        return retriever.retrieve(query)

document_manager = DocumentManager()


document_manager_tools = ToolFuncGroup(
    name="document_index_tools",
    description="Tools to manage document indexes such as adding documents, creating indices, listing indices, deleting indices, listing documents, and searching documents.",
    lifetime="PERSISTENT"
)

@gc_tool(
    action=ToolFuncParamDescriptor(
        name="action",
        description="List of Actions can be done with document manager", 
        required=True,
        llm_type_desc=dict(
            type="string",
            enum=[
                "ADD_DOCUMENTS",
                "CREATE_INDEX",
                "LIST_INDICES",
                "DELETE_INDEX",
                "LIST_DOCUMENTS_IN_INDEX",
                "SEARCH",
            ],
        ),
    ),
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    top_n="Top N documents to retrieve",
    index_id="The unique identifier of the index",
    index_name="The name of the index",
    filepath="The file path on local server disk of the document to add, if from local git repo, prefix with BOT_GIT:",
    query="The query to retrieve the documents",
    _group_tags_=[document_manager_tools],
)
def _document_index(
    action: str,
    bot_id: str = '',
    thread_id: str = '',
    top_n : int = 10,
    index_id: str = '',
    index_name: str = '',
    filepath: str = '',
    query: str = '',

) -> dict:
    """
    Tool to manage document indicies such as adding documents, creating indices, listing indices, deleting indices, listing documents, and querying indicies for matching documents.
    """
    if (not index_id and not index_name) and action != 'CREATE_INDEX' and action != 'LIST_INDICES':
        return {"Success": False, "Error": "Either index_id or index_name must be provided"}
    datapath = filepath 
    if action == 'ADD_DOCUMENTS':
        try:
            document_manager.add_document(index_id, datapath)
            return {"Success": True, "Message": "Document added successfully"}
        except Exception as e:
            return {"Success": False, "Error": str(e)}
    elif action == 'CREATE_INDEX':
        try:
            index_id = document_manager.create_index(index_id if index_id else None)
            return {"Success": True, "index_id": index_id}
        except Exception as e:
            return {"Success": False, "Error": str(e)}
    elif action == 'LIST_INDICES':
        try:
            indices = document_manager.list_of_indices()
            return {"Success": True, "Indices": indices}
        except Exception as e:
            return {"Success": False, "Error": str(e)}
    elif action == 'DELETE_INDEX':
        try:
            document_manager.delete_index(index_id)
            return {"Success": True, "Message": "Index deleted successfully"}
        except Exception as e:
            return {"Success": False, "Error": str(e)}
    elif action == 'LIST_DOCUMENTS_IN_INDEX':
        try:
            docs = document_manager.list_of_documents(index_id)
            return {"Success": True, "Documents": docs}
        except Exception as e:
            return {"Success": False, "Error": str(e)}       
    elif action == 'SEARCH':
        try:
            results = document_manager.retrieve(query, index_id, top_n)
            return {"Success": True, "Results": results}
        except Exception as e:
            return {"Success": False, "Error": str(e)}
    else:
        return {"Success": False, "Error": "Invalid action"}



document_manager_functions = [ _document_index ]

# Called from bot_os_tools.py to update the global list of functions
def get_document_manager_functions():
    return document_manager_functions