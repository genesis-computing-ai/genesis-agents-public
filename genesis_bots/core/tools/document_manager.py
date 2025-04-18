from genesis_bots.core.logging_config import logger
from textwrap import dedent
import os
from genesis_bots.connectors import get_global_db_connector
import chromadb

from genesis_bots.core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

# Lazy imports for llama_index components
def get_llama_components():
    from llama_index.vector_stores.chroma import ChromaVectorStore
    from llama_index.core import Settings
    from llama_index.embeddings.openai import OpenAIEmbedding
    from llama_index.core.indices.vector_store import VectorStoreIndex
    from llama_index.core.storage.storage_context import StorageContext
    from llama_index.core.readers import SimpleDirectoryReader
    from llama_index.core import ComposableGraph
    return {
        'ChromaVectorStore': ChromaVectorStore,
        'Settings': Settings,
        'OpenAIEmbedding': OpenAIEmbedding,
        'VectorStoreIndex': VectorStoreIndex,
        'StorageContext': StorageContext,
        'SimpleDirectoryReader': SimpleDirectoryReader,
        'ComposableGraph': ComposableGraph
    }

class DocumentManager(object):
    _instance = None
    _initialized = False
    _llama_components = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(DocumentManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, db_adapter):
        if self._initialized:
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

        self._index_cache = {}
        self._initialized = True

    def _ensure_llama_initialized(self):
        if self._llama_components is None:
            self._llama_components = get_llama_components()

            # Initialize vector store
            self.vector_store = self._llama_components['ChromaVectorStore'](chroma_collection=self.collection)

            # Set up embedding model
            self.embed_model = self._llama_components['OpenAIEmbedding'](model="text-embedding-3-large")
            self._llama_components['Settings'].embed_model = self.embed_model

    def get_index_id(self, index_name: str):
        query = f"SELECT INDEX_ID FROM {self.db_adapter.index_manager_table_name} WHERE index_name = '{index_name}'"
        result = self.db_adapter.run_query(query)
        if result:
            return str(result[0]['INDEX_ID'])
        return ''

    def store_index_id(self, index_name: str, index_id: str, bot_id: str):
        # Format timestamp in YYYY-MM-DD HH:MI:SS format without timezone
        timestamp = self.db_adapter.get_current_time_with_timezone().split()[0:2]
        timestamp = ' '.join(timestamp)  # Join date and time parts without timezone
        self.db_adapter.run_insert(self.db_adapter.index_manager_table_name, 
                                   **{'index_name': index_name, 'index_id': index_id, 'bot_id': bot_id, 
                                    'timestamp': timestamp})
        return True

    def delete_index_from_table(self, index_name: str):
        query = f"DELETE FROM {self.db_adapter.index_manager_table_name} WHERE index_name = '{index_name}'"
        self.db_adapter.run_query(query)
        self.db_adapter.client.commit()
        return True

    def list_of_indices(self):
        query = f'SELECT * FROM {self.db_adapter.index_manager_table_name}'
        result = self.db_adapter.run_query(query)
        return [item['INDEX_NAME'] for item in result]

    def list_of_documents(self, index_name=None, path_filter=None, query=None, show_files_only=False):
        logger.info(f"Listing documents. Index name: {index_name}, Path filter: {path_filter}, Query: {query}, Files only: {show_files_only}")

        # If no index specified, get all indices first
        if not index_name:
            indices = self.list_of_indices()
            logger.info(f"No index specified, searching across all indices: {indices}")
        else:
            indices = [index_name]

        # Use dictionaries to track unique docs and subdirectories
        unique_docs = {}  # path -> {path, index, content}
        subdirs = {}  # subdir -> {path, index, is_subdir}
        docs_per_index = {}  # index -> count of unique documents

        for idx in indices:
            # Get unique document paths for accurate counting
            unique_paths = set()
            where_clause = {"index_name": idx}

            results = self.collection.get(
                where=where_clause,
                include=['metadatas']  # First just get metadata for counting
            )

            if results and isinstance(results, dict) and results.get('metadatas'):
                for metadata in results['metadatas']:
                    if metadata:
                        doc_path = metadata.get('original_path') or metadata.get('file_path')
                        if doc_path:
                            unique_paths.add(doc_path)

            docs_per_index[idx] = len(unique_paths)
            logger.info(f"Index {idx} has {len(unique_paths)} unique documents")

            # Now process the documents for display
            results = self.collection.get(
                where=where_clause,
                include=['documents', 'metadatas']
            )

            if not results or not isinstance(results, dict) or not results.get('metadatas'):
                continue

            processed_paths = set()  # Track which paths we've already processed

            for i, metadata in enumerate(results['metadatas']):
                if not metadata:
                    continue

                doc_path = metadata.get('original_path') or metadata.get('file_path')
                if not doc_path or doc_path in processed_paths:
                    continue

                processed_paths.add(doc_path)

                # Apply filters
                path_match = path_filter is None or path_filter in doc_path
                query_match = query is None or query.lower() in doc_path.lower()

                if not path_match or not query_match:
                    continue

                # Extract directory path
                if 'BOT_GIT:' in doc_path:
                    base_path = doc_path.split('BOT_GIT:')[1]
                else:
                    base_path = doc_path

                dir_parts = base_path.split('/')
                current_level = len(path_filter.split('/')) if path_filter else 1

                # Handle subdirectories
                if not show_files_only and len(dir_parts) > current_level:
                    subdir = dir_parts[0]
                    if 'BOT_GIT:' not in subdir:
                        subdir = f"BOT_GIT:{subdir}"
                    subdirs[subdir] = {
                        "path": subdir,
                        "index": idx,
                        "is_subdir": True
                    }

                # Add the document
                content = results['documents'][i] if results.get('documents') and i < len(results['documents']) else ""
                unique_docs[doc_path] = {
                    "path": doc_path,
                    "index": idx,
                    "file_name": metadata.get('file_name'),
                    "file_type": metadata.get('file_type'),
                    "file_size": metadata.get('file_size'),
                    "is_subdir": False,
                    "content": content
                }

        # Combine results
        if show_files_only:
            combined_results = list(unique_docs.values())
        else:
            combined_results = list(subdirs.values()) + list(unique_docs.values())

        total_docs = len(combined_results)

        # If we have more than 50 results
        if total_docs > 50:
            remaining = total_docs - 50
            message = f"Showing first 50 of {total_docs} items. {remaining} more items exist."
            if not index_name:
                index_counts = [f"{idx} ({count} docs)" for idx, count in docs_per_index.items()]
                message += f"\nAvailable indices: {', '.join(index_counts)}"
            return {
                "documents": combined_results[:50],
                "total_count": total_docs,
                "message": message,
                "available_indices": docs_per_index
            }

        return {
            "documents": combined_results,
            "total_count": total_docs,
            "available_indices": docs_per_index
        }

    def rename_index(self, index_name, new_index_name):
        if not self.get_index_id(index_name):
            raise Exception("Index does not exist")
        query = f"UPDATE {self.db_adapter.index_manager_table_name} SET index_name = '{new_index_name}' WHERE index_name = '{index_name}'"
        self.db_adapter.run_query(query)
        return True
    
    def create_index(self, index_name: str, bot_id: str):        
        if self.get_index_id(index_name):
            raise Exception("Index with the same name already exists")

        self._ensure_llama_initialized()

        # Create filtered vector store for this index
        filtered_vector_store = self._llama_components['ChromaVectorStore'](
            chroma_collection=self.collection,
            filter={"index_name": index_name}
        )

        storage_context = self._llama_components['StorageContext'].from_defaults(
            vector_store=filtered_vector_store
        )

        index = self._llama_components['VectorStoreIndex'](
            [],
            storage_context=storage_context,
            embed_model=self.embed_model
        )

        # Update cache with new index
        self._index_cache[index.index_id] = index
        self.store_index_id(index_name, index.index_id, bot_id)
        return f'Index {index_name} is created - index_id = {index.index_id}'

    def delete_index(self, index_name):
        index_id = self.get_index_id(index_name)

        # Delete from vector store
        if index_id in self._index_cache:
            del self._index_cache[index_id]

        # Delete from ChromaDB collection using filter
        res = self.collection.delete(
            where={"index_name": index_name}
        )

        self.delete_index_from_table(index_name)

        return True

    def load_index(self, index_name):
        index_id = self.get_index_id(index_name)
        if not index_id:
            raise Exception("Index does not exist")
        # Cache the loaded index
        if index_id not in self._index_cache:
            self._ensure_llama_initialized()
            filtered_vector_store = self._llama_components['ChromaVectorStore'](
                chroma_collection=self.collection,
                filter={"index_name": index_name}
            )
            storage_context = self._llama_components['StorageContext'].from_defaults(vector_store=filtered_vector_store)
            self._index_cache[index_id] = self._llama_components['VectorStoreIndex'](
                [],
                storage_context=storage_context,
                embed_model=self.embed_model
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
            raise Exception("Index name is required")

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
            self._ensure_llama_initialized()
            if os.path.isfile(datapath):
                new_documents = self._llama_components['SimpleDirectoryReader'](input_files=[datapath]).load_data()
            elif os.path.isdir(datapath):
                new_documents = self._llama_components['SimpleDirectoryReader'](input_dir=datapath).load_data()
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
            self._ensure_llama_initialized()
            graph = self._llama_components['ComposableGraph'].from_indices(
                self._llama_components['VectorStoreIndex'],
                indices,
                index_summaries=summaries
            )

            # Create query engine and execute query
            query_engine = graph.as_query_engine(
                response_mode="compact",
                llm_kwargs={"model": "o3-mini"}  # or any other OpenAI model
            )
            response = query_engine.query(query)

            references = ', '.join(list(set(item['file_name'] for item in response.metadata.values() if 'file_name' in item)))
            return {'answer': response.response, 'references': references}

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

    def get_document_content(self, index_name: str, document_path: str):
        """
        Retrieve the full content of a specific document from an index
        
        Args:
            index_name (str): Name of the index containing the document
            document_path (str): Path of the document to retrieve
            
        Returns:
            dict: Document content and metadata if found, None otherwise
        """
        results = self.collection.get(
            where={
                "index_name": index_name,
                "$or": [
                    {"original_path": document_path},
                    {"file_path": document_path}
                ]
            },
            include=['documents', 'metadatas']
        )
        
        if results and isinstance(results, dict) and results.get('documents'):
            return {
                'content': results['documents'][0],
                'metadata': results['metadatas'][0] if results.get('metadatas') else None
            }
        return None

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
        description="List of Actions can be done with document manager",
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
                "DELETE_DOCUMENT",
                "GET_DOCUMENT"
            ],
        ),
    ),
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    top_n="Top N documents to retrieve (default 10)",
    index_name="Optional name of the index. Leave empty to list documents from all indices",
    new_index_name="The name of the index to be renamed to",
    filepath="The file path on local server disk of the document to add or delete, if from local git repo, prefix with BOT_GIT:",
    query="Optional text to filter documents by in LIST_DOCUMENTS, or search query for SEARCH, or question for ASK",
    path_filter="Optional filter to only show documents containing this path string",
    show_files_only="If True, only show files in the current directory level, not subdirectories",
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
    path_filter: str = None,
    show_files_only: bool = False
) -> dict:
    """
    Tool to manage document indicies such as adding documents, creating indices, listing indices, deleting indices, listing documents in indicies.
    There are two ways to search:
     SEARCH - returns more raw results based on a search term
     ASK - returns a synthesized answer to a question with footnotes
    """
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
            result = document_manager.list_of_documents(index_name, path_filter, query, show_files_only)
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
    elif action == 'GET_DOCUMENT':
        try:
            if not index_name:
                raise Exception("Index name is required")
            if not filepath:
                raise Exception("Filepath is required")
            result = document_manager.get_document_content(index_name, filepath)
            if result:
                return {"Success": True, "Content": result['content'], "Metadata": result['metadata']}
            return {"Success": False, "Error": "Document not found"}
        except Exception as e:
            return {"Success": False, "Error": str(e)}
    else:
        return {"Success": False, "Error": "Invalid action"}



document_manager_functions = [ _document_index ]

# Called from bot_os_tools.py to update the global list of functions
def get_document_manager_functions():
    return document_manager_functions