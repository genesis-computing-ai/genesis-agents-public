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
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(DocumentManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self.storage_path = os.path.join(os.getcwd(), 'bot_git','storage')
        try:
            storage_context = StorageContext.from_defaults(persist_dir=self.storage_path)
        except Exception as e:
            storage_context = StorageContext.from_defaults()
            storage_context.persist(self.storage_path)
        self.storage_context = storage_context

    def list_of_indices(self):        
        indices = load_indices_from_storage(self.storage_context)
        return [index.index_id for index in indices]
    
    def list_of_documents(self, index_id):
        index = load_index_from_storage(self.storage_context, index_id)
        docs = set()
        for doc_id, doc_val in index.ref_doc_info.items():
            docs.add(doc_val.metadata['file_path'])
        return docs
    
    def create_index(self):
        index = VectorStoreIndex([], storage_context=self.storage_context)
        self.storage_context.persist(self.storage_path)
        return index.index_id
    
    def delete_index(self, index_id):
        index = load_index_from_storage(self.storage_context, index_id)
        # Delete an index is not implemented in llama_index

    def load_index(self, index_id):
        index = load_index_from_storage(self.storage_context, index_id)        
        return index
    
    def add_document(self, index_id, datapath):
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
    name="document_manager_tools",
    description="Tools to manage documents such as ",
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
                "ADD_DOCUMENT",
                "CREATE_INDEX",
                "LIST_INDICES",
                "DELETE_INDEX",
                "LIST_DOCUMENTS",
                "QUERY",
            ],
        ),
    ),
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    top_n="Top N documents to retrieve",
    index_id="The index id to perform the action on",
    filepath="The path of the document to add",
    query="The query to retrieve the documents",
    _group_tags_=[document_manager_tools],
)
def _document_manager(
    action: str,
    bot_id: str = '',
    thread_id: str = '',
    top_n : int = 10,
    index_id: str = '',
    datapath: str = '',
    query: str = '',

) -> dict:
    """
    Tools to manage documents such as adding documents, creating indices, listing indices, deleting indices, listing documents, and querying documents.

    Args:
        action (str): List of Actions can be done with document manager
        bot_id (str): The bot id to perform the action on
        thread_id (str): The thread id to perform the action on
        top_n (int): Top N documents to retrieve
        index_id (str): The index id to perform the action on
        datapath (str): The path of the document to add (either a file or a directory)
        query (str): The query to retrieve the documents

    Returns:
        dict: The result of the action
    """
    
    if action == 'ADD_DOCUMENT':
        try:
            document_manager.add_document(index_id, datapath)
            return {"Success": True, "Message": "Document added successfully"}
        except Exception as e:
            return {"Success": False, "Error": str(e)}
    elif action == 'CREATE_INDEX':
        try:
            index_id = document_manager.create_index()
            return {"Success": True, "Index ID": index_id}
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
    elif action == 'LIST_DOCUMENTS':
        try:
            docs = document_manager.list_of_documents(index_id)
            return {"Success": True, "Documents": docs}
        except Exception as e:
            return {"Success": False, "Error": str(e)}       
    elif action == 'QUERY':
        try:
            results = document_manager.retrieve(query, index_id, top_n)
            return {"Success": True, "Results": results}
        except Exception as e:
            return {"Success": False, "Error": str(e)}
    else:
        return {"Success": False, "Error": "Invalid action"}



document_manager_functions = [_document_manager]

# Called from bot_os_tools.py to update the global list of functions
def get_document_manager_functions():
    return document_manager_functions