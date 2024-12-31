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
        description=dedent(
            """The action to perform:
            - list_files: List all tracked files (optional: path)
            - read_file: Read file contents (requires: file_path)
            - write_file: Write content to file (requires: file_path, content; optional: commit_message)
            - generate_diff: Generate diff between contents (requires: old_content, new_content; optional: context_lines)
            - apply_diff: Apply a unified diff to a file (requires: file_path, diff_content; optional: commit_message)
            - commit: Commit changes (requires: message)
            - get_history: Get commit history (optional: file_path, max_count)
            - create_branch: Create new branch (requires: branch_name)
            - switch_branch: Switch to branch (requires: branch_name)
            - get_branch: Get current branch name
            - get_status: Get file status (optional: file_path)"""
        ),
        required=True,
        llm_type_desc=dict(
            type="string",
            enum=[
                "list_files",
                "read_file",
                "write_file",
                "generate_diff",
                "apply_diff",
                "commit",
                "get_history",
                "create_branch",
                "switch_branch",
                "get_branch",
                "get_status",
            ],
        ),
    ),
    file_path="Path to the file within the repository",
    content="Content to write to the file",
    commit_message="Message to use when committing changes",
    old_content="Original content for generating diff",
    new_content="New content for generating diff",
    diff_content="Unified diff content to apply to a file",
    branch_name="Name of the branch to create or switch to",
    path="Optional path filter for listing files",
    message="Message to use when committing changes",
    max_count="Maximum number of commits to return",
    context_lines="Number of context lines in generated diffs",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[git_action],
)
def git_action(action, **kwargs):
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
def get_google_drive_tool_functions():
    return git_action_functions
