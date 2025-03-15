from genesis_bots.core.logging_config import logger
from llama_index.core import VectorStoreIndex, SimpleDirectoryReader
from llama_index.core import StorageContext, load_index_from_storage, load_indices_from_storage, ComposableGraph
from textwrap import dedent
import os
from genesis_bots.connectors import get_global_db_connector


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

    def __init__(self, db_adapter):
        if self._initialized:  # Skip if already initialized
            return
        self.db_adapter = db_adapter
        self.storage_path = os.path.join(os.getcwd(), 'bot_git','storage')
        try:
            self.storage_context = StorageContext.from_defaults(persist_dir=self.storage_path)
        except Exception as e:
            self.storage_context = StorageContext.from_defaults()
            self.storage_context.persist(self.storage_path)
            
        self._index_cache = {}  # Add index cache
        self._initialized = True

    def get_index_id(self, index_name: str):
        query = f"SELECT * FROM {self.db_adapter.index_manager_table_name} WHERE index_name = '{index_name}'"
        result = self.db_adapter.run_query(query)
        if result:
            return result[0]['INDEX_ID']
        return ''
    
    def store_index_id(self, index_name: str, index_id: str, bot_id: str):
        # Format timestamp in YYYY-MM-DD HH:MI:SS format without timezone
        timestamp = self.db_adapter.get_current_time_with_timezone().split()[0:2]
        timestamp = ' '.join(timestamp)  # Join date and time parts without timezone
        self.db_adapter.run_insert(self.db_adapter.index_manager_table_name, 
                                   **{'index_name': index_name, 'index_id': index_id, 'bot_id': bot_id, 
                                    'timestamp': timestamp})

    def delete_index_id(self, index_id: str):
        query = f"DELETE FROM {self.db_adapter.index_manager_table_name} WHERE index_id = '{index_id}'"
        self.db_adapter.run_query(query)

    def list_of_indices(self):
        query = f'SELECT * FROM {self.db_adapter.index_manager_table_name}'
        result = self.db_adapter.run_query(query)
        return [item['INDEX_NAME'] for item in result]
    
    def list_of_documents(self, index_name):        
        index = self.load_index(index_name)
        docs = set()
        for doc_id, doc_val in index.ref_doc_info.items():
            docs.add(doc_val.metadata['file_path'])
        return docs
    
    def rename_index(self, index_name, new_index_name):
        if not self.get_index_id(index_name):
            raise Exception("Index does not exist")
        query = f"UPDATE {self.db_adapter.index_manager_table_name} SET index_name = '{new_index_name}' WHERE index_name = '{index_name}'"
        self.db_adapter.run_query(query)
    
    def create_index(self, index_name: str, bot_id: str):        
        if self.get_index_id(index_name):
            raise Exception("Index with the same name already exists")

        index = VectorStoreIndex([], storage_context=self.storage_context)
        self.storage_context.persist(self.storage_path)
        # Update cache with new index
        self._index_cache[index.index_id] = index
        self.store_index_id(index_name, index.index_id, bot_id)
        return index.index_id
    
    def delete_index(self, index_name):
        # Get all node IDs associated with this index before deletion
        index = self.load_index(index_name)
        index_id = index.index_id
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

        self.delete_index_id(index_id)
        
        # Persist the changes
        self.storage_context.persist(self.storage_path)            


    def load_index(self, index_name):
        index_id = self.get_index_id(index_name)
        if not index_id:
            raise Exception("Index does not exist")
        # Cache the loaded index
        if index_id not in self._index_cache:
            self._index_cache[index_id] = load_index_from_storage(self.storage_context, index_id)
        return self._index_cache[index_id]

    def add_document(self, index_name, datapath, immediate_write=True):
        index = self.load_index(index_name)

        if datapath is None and immediate_write:
            self.storage_context.persist(self.storage_path)
            return True

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
        
        for doc in new_documents:
            index.insert(doc)
        if immediate_write:
            self.storage_context.persist(self.storage_path)

    def retrieve(self, query, index_name=None, top_n=3):
        if index_name:
            index = self.load_index(index_name)
            retriever = index.as_retriever(similarity_top_k=top_n)
            return retriever.retrieve(query)
        else:
            # Search across all indices and combine results
            all_results = []
            for idx_name in self.list_of_indices():
                try:
                    index = self.load_index(idx_name)
                    retriever = index.as_retriever(similarity_top_k=top_n)
                    results = retriever.retrieve(query)
                    all_results.extend(results)
                except Exception as e:
                    print(f"Error retrieving from index {idx_name}: {e}")
                    continue
            return all_results
    
    def retrieve_all_indices(self, query, top_n=3):
        """
        Search across all indices using a composable graph to combine results.
        
        Args:
            query (str): The search query
            top_n (int): Number of top results to return per index
            
        Returns:
            Response from querying across all indices
        """
        try:
            # Get list of all indices
            indices = []
            summaries = []
            for index_name in self.list_of_indices():
                try:
                    index = self.load_index(index_name)
                    indices.append(index)
                    summaries.append(f"Index: {index_name}")
                except Exception as e:
                    print(f"Error loading index {index_name}: {e}")
                    continue

            if not indices:
                return []

            # Create composable graph from all indices
            graph = ComposableGraph.from_indices(
                VectorStoreIndex,
                indices,
                index_summaries=summaries
            )

            retriever = index.as_retriever(similarity_top_k=top_n)
            return retriever.retrieve(query)
            # Create query engine and execute query
            query_engine = graph.as_query_engine()
            response = query_engine.query(query)

            return response

        except Exception as e:
            print(f"Error querying across indices: {e}")
            return []

db_adapter = get_global_db_connector()
document_manager = DocumentManager(db_adapter)


document_index_tools = ToolFuncGroup(
    name="document_index_tools",
    description="Tools to manage document indexes such as adding documents, creating indices, listing indices, deleting indices, listing documents, and searching documents.",
    lifetime="PERSISTENT"
)

@gc_tool(
    action=ToolFuncParamDescriptor(
        name="action",
        description="List of Actions can be done with document manager. QUERY searches one or all indexes and returns results, ASK allows you to ask a question across all indices and returns an answer based on the context of all indices", 
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
                "RENAME_INDEX",
                "ASK"
            ],
        ),
    ),
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    top_n="Top N documents to retrieve",
    index_name="The name of the index. On search leave empty to search all indicies",
    new_index_name="The name of the index to be renamed to",
    filepath="The file path on local server disk of the document to add, if from local git repo, prefix with BOT_GIT:",
    query="The query to retrieve the documents",
    _group_tags_=[document_index_tools],
)
def _document_index(
    action: str,
    bot_id: str = '',
    thread_id: str = '',
    top_n : int = 10,
    index_name: str = '',
    new_index_name: str = '',
    filepath: str = '',
    query: str = '',
    immediate_write: bool = True,

) -> dict:
    """
    Tool to manage document indicies such as adding documents, creating indices, listing indices, deleting indices, listing documents, and querying indicies for matching documents.
    """
    datapath = filepath 
    if action == 'ADD_DOCUMENTS':
            try:
                document_manager.add_document(index_name, datapath, immediate_write=immediate_write)
                return {"Success": True, "Message": "Document added successfully"}
            except Exception as e:
                return {"Success": False, "Error": str(e)}
    elif action == 'SAVE_INDEX':
            try:
                document_manager.add_document(index_name, None, immediate_write=True)
                return {"Success": True, "Message": "Document store persisted to storage"}
            except Exception as e:
                return {"Success": False, "Error": str(e)}
    elif action == 'CREATE_INDEX':
        try:
            if not index_name or not bot_id:
                raise Exception("Index name is required")
            document_manager.create_index(index_name, bot_id)
            return {"Success": True, "Message": f"Index {index_name} created successfully"}
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
            document_manager.delete_index(index_name)
            return {"Success": True, "Message": "Index deleted successfully"}
        except Exception as e:
            return {"Success": False, "Error": str(e)}
    elif action == 'RENAME_INDEX':
        try:
            if not new_index_name:
                raise Exception("New index name is required")
            if not index_name:
                raise Exception("Index name is required")
            document_manager.rename_index(index_name, new_index_name)
            return {"Success": True, "Message": "Index renamed successfully"}
        except Exception as e:
            return {"Success": False, "Error": str(e)}
    elif action == 'LIST_DOCUMENTS_IN_INDEX':
        try:
            docs = document_manager.list_of_documents(index_name)
            return {"Success": True, "Documents": docs}
        except Exception as e:
            return {"Success": False, "Error": str(e)}       
    elif action == 'SEARCH':
        try:
            results = document_manager.retrieve(query, index_name, top_n)
            return {"Success": True, "Results": results}
        except Exception as e:
            return {"Success": False, "Error": str(e)}
    elif action == 'ASK':
        try:
            results = document_manager.retrieve_all_indices(query, top_n)
            return {"Success": True, "Results": results}
        except Exception as e:
            return {"Success": False, "Error": str(e)}
    else:
        return {"Success": False, "Error": "Invalid action"}



document_manager_functions = [ _document_index ]

# Called from bot_os_tools.py to update the global list of functions
def get_document_manager_functions():
    return document_manager_functions