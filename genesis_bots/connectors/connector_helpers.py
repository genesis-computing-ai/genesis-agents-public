class llm_keys_and_types_struct:
    def __init__(
        self,
        llm_type=None,
        llm_key=None,
        llm_endpoint=None,
        model_name=None,
        embedding_model_name=None,
    ):
        """
        Initializes the LLM configuration with various parameters for LLM type, key, endpoint, and model names.

        Args:
            llm_type (str, optional): The type of LLM (e.g., 'cortex', 'openai'). Defaults to None.
            llm_key (str, optional): The key required for accessing the LLM. Defaults to None.
            llm_endpoint (str, optional): The endpoint URL of the Azure OpenAI LLM. Defaults to an empty string if None.
            model_name (str, optional): The name of the specific model to use (e.g. gpt-4o). Defaults to an empty string if None.
            embedding_model_name (str, optional): The name of the specific embedding model (e.g. text-embedding-3-large). Defaults to an empty string if None.
        """
        self.llm_type = (
            llm_type  # a str like 'cortex', 'openai', etc. TODO: use BotLlmEngineEnum
        )
        self.llm_key = llm_key
        self.llm_endpoint = llm_endpoint if llm_endpoint is not None else ""
        self.model_name = model_name if model_name is not None else ""
        self.embedding_model_name = (
            embedding_model_name if embedding_model_name is not None else ""
        )
