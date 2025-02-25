from typing import Optional, Dict, Any
from genesis_bots.core.logging_config import logger

from genesis_bots.core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

from genesis_bots.core.tools.tool_helpers import chat_completion
from textwrap import dedent

from genesis_bots.connectors import get_global_db_connector
db_adapter = get_global_db_connector()

def _check_git_available():
    """Check if git is available on the system."""
    try:
        import subprocess
        subprocess.run(['git', '--version'], check=True, capture_output=True)
        return True
    except (subprocess.SubprocessError, FileNotFoundError):
        return False

# Only import and initialize GitFileManager if git is available
git_manager = None
if _check_git_available():
    from genesis_bots.core.file_diff_handler import GitFileManager
    git_manager = GitFileManager()

git_action_grp = ToolFuncGroup(
    name="git_action",
    description="Git repository file management operations",
    lifetime="PERSISTENT",
)


@gc_tool(
    action=ToolFuncParamDescriptor(
        name="action",
        description="The git action to perform (list_files, read_file, write_file, etc.)",
        required=True,
        llm_type_desc=dict(
            type="string",
            enum=["list_files", "read_file", "write_file", "generate_diff", "apply_diff",
             "commit", "get_history", "create_branch", "switch_branch", "get_branch", "get_status"],
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
    kwargs=ToolFuncParamDescriptor(
        name="kwargs",
        description="Additional arguments needed for the specific action",
        required=True,
        llm_type_desc=dict(type="object"),
    ),
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[git_action_grp],
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
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Wrapper for Git file management operations

    Args:
        action: The git action to perform (list_files, read_file, write_file, etc.)
        file_path: Path to the file to operate on
        content: Content to write to file
        commit_message: Message for git commit
        old_content: Previous content for comparison
        new_content: New content for comparison
        diff_content: Content for diff operation
        branch_name: Name of git branch
        path: Alternative path specification
        message: Alternative message specification
        max_count: Maximum number of items to return
        context_lines: Number of context lines to include
        bot_id: Bot identifier
        thread_id: Thread identifier
        kwargs: Additional arguments needed for the specific action

    Returns:
        Dict containing operation result and any relevant data
    """
    params = {
            'file_path': file_path,
            'content': content,
            'commit_message': commit_message,
            'old_content': old_content,
            'new_content': new_content,
            'diff_content': diff_content,
            'branch_name': branch_name,
            'path': path,
            'message': message,
            'max_count': max_count,
            'context_lines': context_lines,
            'bot_id': bot_id,
            'thread_id': thread_id,
            **kwargs
        }
    # Remove None values
    params = {k: v for k, v in params.items() if v is not None}
    return git_manager.git_action(action, **params)

# Define as a list explicitly
git_action_functions = [git_action]

# And make the getter function explicitly return a list
def get_git_action_functions():
    if not _check_git_available():
        logger.warning("Git is not available on the system. Git action tools will not be registered.")
        return []
    if git_manager is None:
        logger.warning("GitFileManager failed to initialize. Git action tools will not be registered.")
        return []
    return list(git_action_functions)  # Explicitly convert to list before returning
