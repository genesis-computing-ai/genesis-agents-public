from typing import Optional, Dict, Any

from core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

from core.tools.tool_helpers import chat_completion
from textwrap import dedent

from connectors import get_global_db_connector
db_adapter = get_global_db_connector()

from core.file_diff_handler import GitFileManager
git_manager = GitFileManager()

git_action = ToolFuncGroup(
    name="git_action",
    description="",
    lifetime="PERSISTENT",
)


@gc_tool(
    action=ToolFuncParamDescriptor(
        name="action",
        description="The git action to perform (list_files, read_file, write_file, etc.)",
        required=True,
        llm_type_desc=dict(
            type="string",
            enum=["list_files", "read_file", "write_file", "commit", "diff", "branch"],
        ),
    ),
    file_path=ToolFuncParamDescriptor(
        name="file_path",
        description="Path to the file within the repository",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    content=ToolFuncParamDescriptor(
        name="content",
        description="Content to write to the file",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    commit_message=ToolFuncParamDescriptor(
        name="commit_message",
        description="Message to use when committing changes",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    old_content=ToolFuncParamDescriptor(
        name="old_content",
        description="Original content for generating diff",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    new_content=ToolFuncParamDescriptor(
        name="new_content",
        description="New content for generating diff",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    diff_content=ToolFuncParamDescriptor(
        name="diff_content",
        description="Unified diff content to apply to a file",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    branch_name=ToolFuncParamDescriptor(
        name="branch_name",
        description="Name of the branch to create or switch to",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    path=ToolFuncParamDescriptor(
        name="path",
        description="Optional path filter for listing files",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    message=ToolFuncParamDescriptor(
        name="message",
        description="Message to use when committing changes",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    max_count=ToolFuncParamDescriptor(
        name="max_count",
        description="Maximum number of commits to return",
        required=False,
        llm_type_desc=dict(type="integer"),
    ),
    context_lines=ToolFuncParamDescriptor(
        name="context_lines",
        description="Number of context lines in generated diffs",
        required=False,
        llm_type_desc=dict(type="integer"),
    ),
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[git_action],
)
def git_action(
    action: str,
    file_path: str = None,
    content: str = None,
    commit_message: str = None,
    old_content: str = None,
    new_content: str = None,
    diff_content: str = None,
    branch_name: str = None,
    path: str = None,
    message: str = None,
    max_count: int = None,
    context_lines: int = None,
    bot_id: str = None,
    thread_id: str = None,
) -> Dict[str, Any]:
    """
    Wrapper for Git file management operations

    Args:
        action: The git action to perform (list_files, read_file, write_file, etc.)
        **kwargs: Additional arguments needed for the specific action

    Returns:
        Dict containing operation result and any relevant data
    """
    return git_manager.git_action(action, **kwargs)


git_action_functions = (git_action,)

# Called from bot_os_tools.py to update the global list of functions
def get_git_action_functions():
    return git_action_functions
