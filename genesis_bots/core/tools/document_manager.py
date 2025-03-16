from genesis_bots.core.logging_config import logger
from llama_index.core import VectorStoreIndex, SimpleDirectoryReader
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.core import StorageContext, load_index_from_storage, load_indices_from_storage, ComposableGraph
from textwrap import dedent
import os
from genesis_bots.connectors import get_global_db_connector
from llama_index.core import Settings
from llama_index.vector_stores.chroma import ChromaVectorStore

import chromadb


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
        
        # Initialize ChromaDB client
        self.storage_path = os.path.join(os.getenv('GIT_PATH', os.path.join(os.getcwd(), 'bot_git')), 'storage')

        self.chroma_client = chromadb.PersistentClient(path=self.storage_path)
        
        # Create or get the collection for storing embeddings
        self.collection = self.chroma_client.get_or_create_collection(
            name="document_store",
            metadata={"hnsw:space": "cosine"}
        )
        
        # Create vector store
        self.vector_store = ChromaVectorStore(
            chroma_collection=self.collection
        )
        
        # Create storage context with ChromaDB
        self.storage_context = StorageContext.from_defaults(
            vector_store=self.vector_store
        )
        
        self._index_cache = {}  # Add index cache
        self._initialized = True

        # Set the default embedding model to large
        Settings.embed_model = OpenAIEmbedding(model="text-embedding-3-large")

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
    
    def list_of_documents(self, index_name=None, path_filter=None, query=None):        
        logger.info(f"Listing documents. Index name: {index_name}, Path filter: {path_filter}, Query: {query}")
        
        # If no index specified, get all indices first
        if not index_name:
            indices = self.list_of_indices()
            logger.info(f"No index specified, searching across all indices: {indices}")
        else:
            indices = [index_name]
        
        # Use a dictionary to deduplicate by path and track counts per index
        unique_docs = {}  # path -> {path, index}
        docs_per_index = {}  # index -> count
        
        for idx in indices:
            docs_per_index[idx] = 0
            # Query with index name
            where_clause = {"index_name": idx}
            results = self.collection.get(
                where=where_clause
            )
            
            # Only log the count of results, not the full results
            result_count = len(results.get('metadatas', [])) if results and isinstance(results, dict) else 0
            logger.info(f"Found {result_count} documents in index {idx}")
            
            if results and isinstance(results, dict) and results.get('metadatas'):
                for metadata in results['metadatas']:
                    if metadata:
                        # Prefer original_path if available, fall back to file_path
                        doc_path = metadata.get('original_path') or metadata.get('file_path')
                        if doc_path:
                            # Apply both path_filter and query filter if provided
                            path_match = path_filter is None or path_filter in doc_path
                            query_match = query is None or query.lower() in doc_path.lower()
                            if path_match and query_match:
                                unique_docs[doc_path] = {
                                    "path": doc_path,
                                    "index": idx
                                }
                                docs_per_index[idx] += 1
        
        # Convert dictionary values to list
        docs = list(unique_docs.values())
        total_docs = len(docs)
        logger.info(f"Found {total_docs} unique documents after filtering")
        
        # If we have more than 50 documents
        if total_docs > 50:
            remaining_docs = total_docs - 50
            message = f"Showing first 50 of {total_docs} documents. {remaining_docs} more documents exist."
            
            # Add index suggestions if no index specified and multiple indices exist
            if not index_name and len(indices) > 1:
                index_counts = [f"{idx} ({count} docs)" for idx, count in docs_per_index.items()]
                message += f"\nTry filtering by one of these indices: {', '.join(index_counts)}"
            
            if path_filter or query:
                message += "\nYou can also try refining your path_filter or query to narrow down results."
            else:
                message += "\nYou can also use path_filter or query to narrow down results."
            
            return {
                "documents": docs[:50],  # List of dicts with path and index
                "total_count": total_docs,
                "message": message,
                "available_indices": docs_per_index  # Include counts per index in response
            }
        
        return {
            "documents": docs,  # List of dicts with path and index
            "total_count": total_docs
        }
    
    def rename_index(self, index_name, new_index_name):
        if not self.get_index_id(index_name):
            raise Exception("Index does not exist")
        query = f"UPDATE {self.db_adapter.index_manager_table_name} SET index_name = '{new_index_name}' WHERE index_name = '{index_name}'"
        self.db_adapter.run_query(query)
    
    def create_index(self, index_name: str, bot_id: str):        
        if self.get_index_id(index_name):
            raise Exception("Index with the same name already exists")

        embed_model = OpenAIEmbedding(model="text-embedding-3-large")
        Settings.embed_model = embed_model
        
        # Create filtered vector store for this index
        filtered_vector_store = ChromaVectorStore(
            chroma_collection=self.collection,
            metadata_filters={"index_name": index_name}
        )
        
        storage_context = StorageContext.from_defaults(
            vector_store=filtered_vector_store
        )
        
        index = VectorStoreIndex(
            [], 
            storage_context=storage_context,
            embed_model=embed_model
        )
        
        # Update cache with new index
        self._index_cache[index.index_id] = index
        self.store_index_id(index_name, index.index_id, bot_id)
        return index.index_id
    
    def delete_index(self, index_name):
        index = self.load_index(index_name)
        index_id = index.index_id
        
        # Delete from vector store
        if index_id in self._index_cache:
            del self._index_cache[index_id]
        
        # Delete from ChromaDB collection using filter
        self.collection.delete(
            where={"index_id": index_id}
        )
        
        self.delete_index_id(index_id)

    def load_index(self, index_name):
        index_id = self.get_index_id(index_name)
        if not index_id:
            raise Exception("Index does not exist")
        # Cache the loaded index
        if index_id not in self._index_cache:
            filtered_vector_store = ChromaVectorStore(
                chroma_collection=self.collection,
                filter={"index_id": index_id}
            )
            storage_context = StorageContext.from_defaults(vector_store=filtered_vector_store)
            self._index_cache[index_id] = VectorStoreIndex(
                [], 
                storage_context=storage_context,
                embed_model=OpenAIEmbedding(model="text-embedding-3-large")
            )
        return self._index_cache[index_id]

    def add_document(self, index_name, datapath):
        """
        Add a document to an index
        
        Args:
            index_name (str): Name of the index to add document to
            datapath (str): Path to the document to add
            
        Returns:
            dict: Result containing success status and additional info
        """
        if not index_name:
            indices = self.list_of_indices()
            if not indices:
                return {
                    'success': False,
                    'error': 'No index specified and no indices exist. Create an index first.',
                    'available_indices': []
                }
            return {
                'success': False,
                'error': 'No index specified. Please specify one of the available indices.',
                'available_indices': indices
            }

        try:
            index = self.load_index(index_name)
        except Exception as e:
            indices = self.list_of_indices()
            return {
                'success': False,
                'error': f'Invalid index: {str(e)}',
                'available_indices': indices
            }

        if datapath is None:
            return {
                'success': False,
                'error': 'No document path specified'
            }

        original_path = datapath  # Store the original path with BOT_GIT: prefix if present
        
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

        try:
            if os.path.isfile(datapath):
                new_documents = SimpleDirectoryReader(input_files=[datapath]).load_data()
            elif os.path.isdir(datapath):
                new_documents = SimpleDirectoryReader(input_dir=datapath).load_data()
            else:
                return {
                    'success': False,
                    'error': 'Invalid path'
                }
            
            for doc in new_documents:
                # Add index_name and original path to the document metadata
                if not hasattr(doc, 'metadata'):
                    doc.metadata = {}
                doc.metadata['index_name'] = index_name
                doc.metadata['original_path'] = original_path
                index.insert(doc)
            
            return {
                'success': True,
                'message': f'Successfully added document to index {index_name}'
            }
        except Exception as e:
            return {
                'success': False,
                'error': f'Error adding document: {str(e)}'
            }

    def retrieve(self, query, index_name=None, top_n=3):
        if index_name:
            index = self.load_index(index_name)
            retriever = index.as_retriever(similarity_top_k=top_n)
            all_results = retriever.retrieve(query)
            simplified_results = []
            for result in all_results:
                simplified_results.append({
                    'score': result.score,
                    'text': result.text,
                    'metadata': result.metadata
                })
            return simplified_results
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
            # Sort all results by score and take top_n
            sorted_results = sorted(all_results, key=lambda x: x.score, reverse=True)
            all_results = sorted_results[:top_n]
            # Convert to simple array of score/text/metadata
            simplified_results = []
            for result in all_results:
                simplified_results.append({
                    'score': result.score,
                    'text': result.text,
                    'metadata': result.metadata
                })
            return simplified_results
    
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

      #      retriever = index.as_retriever(similarity_top_k=top_n)
       #     return retriever.retrieve(query)
            # Create query engine and execute query
            query_engine = graph.as_query_engine(
                response_mode="compact",
                llm_kwargs={"model": "o3-mini"}  # or any other OpenAI model
            )
            response = query_engine.query(query)

            return response

        except Exception as e:
            print(f"Error querying across indices: {e}")
            return []

    def delete_document(self, index_name: str, document_path: str):
        """
        Delete a single document from an index based on its path
        
        Args:
            index_name (str): Name of the index containing the document
            document_path (str): Path of the document to delete
        """
        logger.info(f"Attempting to delete document {document_path} from index {index_name}")
        
        # Get all documents that match the index name
        results = self.collection.get(
            where={"index_name": index_name}
        )
        
        # Find the IDs of documents that match either path
        ids_to_delete = []
        if results and isinstance(results, dict):
            for i, metadata in enumerate(results.get('metadatas', [])):
                if metadata:
                    doc_path = metadata.get('original_path') or metadata.get('file_path')
                    if doc_path == document_path:
                        ids_to_delete.append(results['ids'][i])
                        logger.info(f"Found matching document with ID: {results['ids'][i]}")
        
        # Delete by IDs if any found
        if ids_to_delete:
            logger.info(f"Deleting {len(ids_to_delete)} documents from collection")
            self.collection.delete(
                ids=ids_to_delete
            )
        else:
            logger.info("No matching documents found to delete")
        
        return True

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
                "LIST_DOCUMENTS",
                "SEARCH",
                "RENAME_INDEX",
                "ASK",
                "DELETE_DOCUMENT"
            ],
        ),
    ),
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    top_n="Top N documents to retrieve (default 10)",
    index_name="The name of the index. Leave empty for all indices",
    new_index_name="The name of the index to be renamed to",
    filepath="The file path on local server disk of the document to ADD or DELETE, if from local git repo, prefix with BOT_GIT:",
    query="The search query (SEARCH) or question to answer (ASK), or text to filter documents by (LIST_DOCUMENTS)",
    path_filter="Optional filter to only show documents containing this path string",
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
    path_filter: str = None
) -> dict:
    """
    Tool to manage document indicies such as adding documents, creating indices, listing indices, deleting indices, listing documents in indicies.
    There are two ways to search:
     SEARCH - returns more raw results based on a search term
     ASK - returns a synthesized answer to a question with footnotes
    """
    datapath = filepath 
    if action == 'ADD_DOCUMENTS':
        try:
            result = document_manager.add_document(index_name, filepath)
            if not result['success']:
                if 'available_indices' in result:
                    return {
                        "Success": False,
                        "Error": result['error'],
                        "AvailableIndices": result['available_indices'],
                        "Hint": "Please specify one of the available indices when adding documents"
                    }
                return {"Success": False, "Error": result['error']}
            return {"Success": True, "Message": result['message']}
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
    elif action == 'LIST_DOCUMENTS':
        try:
            result = document_manager.list_of_documents(index_name, path_filter, query)
            response = {
                "Success": True,
                "Documents": result["documents"],  # Contains list of dicts with path and index
                "TotalCount": result["total_count"]
            }
            if "message" in result:
                response["Message"] = result["message"]
            return response
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
    elif action == 'DELETE_DOCUMENT':
        try:
            if not index_name:
                raise Exception("Index name is required")
            if not filepath:
                raise Exception("Filepath is required")
            document_manager.delete_document(index_name, filepath)
            return {"Success": True, "Message": "Document deleted successfully"}
        except Exception as e:
            return {"Success": False, "Error": str(e)}
    else:
        return {"Success": False, "Error": "Invalid action"}



document_manager_functions = [ _document_index ]

# Called from bot_os_tools.py to update the global list of functions
def get_document_manager_functions():
    return document_manager_functions