# Instantiates a ProjectManager object and creates tools to add to new method of adding tools (12/31/24)

from connectors import get_global_db_connector
db_adapter = get_global_db_connector()
from core.bot_os_project_manager import ProjectManager

from core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

todos = ProjectManager(db_adapter)

manage_todos_tools = ToolFuncGroup(
    name="manage_todos_tools",
    description="",
    lifetime="PERSISTENT",
)

@gc_tool(
    action="Action to perform (CREATE, UPDATE, CHANGE_STATUS, LIST)",
    todo_id="ID of the todo item (required for UPDATE and CHANGE_STATUS)",
    todo_details=ToolFuncParamDescriptor(
        name="todo_details",
        description="Details for the todo item. For CREATE: requires project_id, todo_name, what_to_do, depends_on. "
        "For CHANGE_STATUS: requires only new_status.",
        llm_type_desc=dict(
            type="object",
            properties=dict(
                project_id=dict(
                    type="string",
                    description="ID of the project the todo item belongs to",
                ),
                todo_name=dict(type="string", description="Name of the todo item"),
                what_to_do=dict(
                    type="string", description="What the todo item is about"
                ),
                assigned_to_bot_id=dict(
                    type="string",
                    description="The bot_id (not just the name) of the bot assigned to this todo. Omit to assign it to yourself.",
                ),
                depends_on=dict(
                    type=["string", "array", "null"],
                    description="ID or array of IDs of todos that this todo depends on",
                ),
                new_status=ToolFuncParamDescriptor(
                    name="new_status",
                    description="New status for the todo (required for CHANGE_STATUS)",
                    required=True,
                    llm_type_desc=dict(
                        type="string",
                        enum=[
                            "NEW",
                            "IN_PROGRESS",
                            "ON_HOLD",
                            "COMPLETED",
                            "CANCELLED",
                        ],
                    ),
                ),
            ),
        ),
        required=False,
    ),
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[manage_todos_tools],
)
def manage_todos(
    self, action, bot_id, todo_id=None, todo_details="", thread_id=None
):
    """
    Manage todo items with various actions.  When creating Todos try to include any dependencies on other todos 
    where they exist, it is important to track those to make sure todos are done in the correct order.
    """
    return todos.manage_todos(
        action=action,
        bot_id=bot_id,
        todo_id=todo_id,
        todo_details=todo_details,
        thread_id=thread_id,
    )


@gc_tool(
    action="Action to perform (CREATE, UPDATE, CHANGE_STATUS, LIST)",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    project_id="ID of the project (required for CREATE and UPDATE)",
    project_details=ToolFuncParamDescriptor(
        name="project_details",
        description="Details for the project. For CREATE: requires project_name, project_description. "
        "For UPDATE: requires only new_status.",
        llm_type_desc=dict(
            type="object",
            properties=dict(
                project_name=dict(type="string", description="Name of the project"),
                project_description=dict(
                    type="string", description="Description of the project"
                ),
                new_status=dict(
                    type="string", description="New status for the project"
                ),
            ),
        ),
        required=False,
    ),
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[manage_todos_tools],
)
def manage_projects(
    action, bot_id, project_id=None, project_details=None, thread_id=None
):
    """
    Manages projects through various actions (CREATE, UPDATE, CHANGE_STATUS, LIST)
    """
    return todos.manage_projects(
        action=action,
        bot_id=bot_id,
        project_id=project_id,
        project_details=project_details,
        thread_id=thread_id,
    )


@gc_tool(
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    todo_id="ID of the todo item to record work for",
    work_description="Detailed description of the work performed or progress made",
    work_results="Optional results, output, or findings from the work performed",
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[manage_todos_tools],
)
def record_todo_work(
    bot_id,
    todo_id,
    work_description,
    work_results=None,
    thread_id=None,
):
    """
    Record work progress on a todo item without changing its status. Use this to log incremental progress, intermediate results, 
    or work updates.
    """
    return todos.record_work(
        bot_id=bot_id,
        todo_id=todo_id,
        work_description=work_description,
        work_results=work_results,
        thread_id=thread_id,
    )


@gc_tool(
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    project_id="ID of the project to get todos for",
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[manage_todos_tools],
)
def get_project_todos(self, bot_id, project_id, thread_id=None):
    """
    Get all todos associated with a specific project
    """
    return todos.get_project_todos(bot_id=bot_id, project_id=project_id)


@gc_tool(
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    todo_id="ID of the todo to get dependencies for",
    include_reverse="If true, also include todos that depend on this todo",
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[manage_todos_tools],
)
def get_todo_dependencies(
    self, bot_id, todo_id, include_reverse=False, thread_id=None
):
    """
    Get all dependencies for a specific todo item
    """
    return todos._get_todo_dependencies(
        bot_id=bot_id, todo_id=todo_id, include_reverse=include_reverse
    )


@gc_tool(
    action="Action to perform (ADD, REMOVE)",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    todo_id="ID of the todo that has the dependency",
    depends_on_todo_id="ID of the todo that needs to be completed first",
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[manage_todos_tools],
)
def manage_todo_dependencies(
    self, action, bot_id, todo_id, depends_on_todo_id=None, thread_id=None
):
    """
    Manage dependencies between todo items, allowing you to specify that one todo must be completed before another can start
    """
    return todos.manage_todo_dependencies(
        action=action,
        bot_id=bot_id,
        todo_id=todo_id,
        depends_on_todo_id=depends_on_todo_id,
    )


@gc_tool(
    action="Action to perform (CREATE, UPDATE, CHANGE_STATUS, LIST)",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    project_id="ID of the project the asset belongs to",
    asset_id="ID of the asset (required for UPDATE and DELETE actions)",
    asset_details=ToolFuncParamDescriptor(
        name="asset_details",
        description="Details for the asset (required for CREATE and UPDATE actions)",
        llm_type_desc=dict(
            type="object",
            properties=dict(
                description=dict(
                    type="string", description="Description of what the asset is for"
                ),
                git_path=dict(
                    type="string",
                    description="Path to the asset's location in the git system",
                ),
            ),
        ),
        required=False,
    ),
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[manage_todos_tools],
)
def manage_project_assets(
    self,
    action,
    bot_id,
    project_id,
    asset_id=None,
    asset_details=None,
    thread_id=None,
):
    """
    Manage project assets including their descriptions and locations in the git system
    """
    return todos.manage_project_assets(
        action=action,
        bot_id=bot_id,
        project_id=project_id,
        asset_id=asset_id,
        asset_details=asset_details,
    )

manage_todos_functions = (
    manage_todos,
    manage_projects,
    record_todo_work,
    get_project_todos,
    get_todo_dependencies,
    manage_todo_dependencies,
    manage_project_assets,
)


# Called from bot_os_tools.py to update the global list of functions
def get_google_drive_tool_functions():
    return manage_todos_functions
