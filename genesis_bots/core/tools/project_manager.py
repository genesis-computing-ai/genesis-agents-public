# Instantiates a ProjectManager object and creates tools to add to new method of adding tools (12/31/24)

from genesis_bots.connectors import get_global_db_connector
db_adapter = get_global_db_connector()
from genesis_bots.core.bot_os_project_manager import ProjectManager

from typing import Dict, Optional, List, Callable, Any

from genesis_bots.core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

project_manager = ProjectManager(db_adapter)

project_manager_tools = ToolFuncGroup(
    name="project_manager_tools",
    description="Functions to manage Projects and TODOs",
    lifetime="PERSISTENT",
)


@gc_tool(
    action=ToolFuncParamDescriptor(
        name="action",
        description="Action to perform (CREATE, UPDATE, CHANGE_STATUS, LIST)",
        required=True,
        llm_type_desc=dict(
            type="string", enum=["CREATE", "UPDATE", "CHANGE_STATUS", "LIST"]
        ),
    ),
    todo_id=ToolFuncParamDescriptor(
        name="todo_id",
        description="ID of the todo item (required for UPDATE and CHANGE_STATUS)",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    todo_details=ToolFuncParamDescriptor(
        name="todo_details",
        description="Details for the todo item. For CREATE: requires project_id, todo_name, what_to_do, depends_on. For CHANGE_STATUS: requires only new_status.",
        required=False,
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
                    type=["string", "array"],
                    description="ID or array of IDs of todos that this todo depends on",
                    items=dict(
                        type="string"
                    )
                ),
                new_status=dict(
                    name="new_status",
                    description="New status for the todo (required for CHANGE_STATUS)",
               #     required=True,
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
    ),
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[project_manager_tools],
)
def manage_todos(
    action: str,
    bot_id: str,
    todo_id: str = None,
    todo_details: Dict = None,
    thread_id: str = None,
) -> None:
    """
    Manage todo items with various actions. When creating Todos try to include any dependencies on other todos
    where they exist, it is important to track those to make sure todos are done in the correct order.
    """
    return project_manager.manage_todos(
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
        description="Details for the project. For CREATE: requires project_name, description. "
        "For UPDATE: requires only new_status.",
        llm_type_desc=dict(
            type="object",
            properties=dict(
                project_name=dict(type="string", description="Name of the project"),
                description=dict(
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
    _group_tags_=[project_manager_tools],
)
def manage_projects(
    action: str,
    bot_id: str,
    project_id: str=None,
    project_details: Dict=None,
    thread_id: str=None,
    static_project_id: bool = False
):
    """
    Manages projects through various actions (CREATE, UPDATE, CHANGE_STATUS, LIST, DELETE)
    These tools allow you to list, create, update, and remove projects, and change the status of projects.
    """
    return project_manager.manage_projects(
        action=action,
        bot_id=bot_id,
        project_id=project_id,
        project_details=project_details,
        thread_id=thread_id,
        static_project_id=static_project_id
    )


@gc_tool(
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    todo_id="ID of the todo item to record work for",
    work_description="Detailed description of the work performed or progress made",
    work_results="Optional results, output, or findings from the work performed",
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[project_manager_tools],
)
def record_todo_work(
    bot_id: str,
    todo_id: str,
    work_description: str,
    work_results: str=None,
    thread_id: str=None,
):
    """
    Record work progress on a todo item without changing its status. Use this to log incremental progress, intermediate results,
    or work updates.
    """
    return project_manager.record_work(
        bot_id=bot_id,
        todo_id=todo_id,
        work_description=work_description,
        work_results=work_results,
        thread_id=thread_id,
    )

@gc_tool(
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    todo_id="ID of the todo item to get history for",
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[project_manager_tools],
)
def get_todo_history(
    bot_id: str,
    todo_id: str,
    thread_id: str=None
):
    """
    Get the complete history of a todo item, including status changes, work records, and other actions.
    Returns a chronological list of all actions and changes made to the todo.
    """
    return project_manager.get_todo_history(todo_id=todo_id)


@gc_tool(
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    project_id="ID of the project to get todos for",
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[project_manager_tools],
)
def get_project_todos(
    bot_id: str,
    project_id: str,
    thread_id: str=None):
    """
    Get all todos associated with a specific project
    """
    return project_manager.get_project_todos(bot_id=bot_id, project_id=project_id)


@gc_tool(
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    todo_id="ID of the todo to get dependencies for",
    include_reverse="If true, also include todos that depend on this todo",
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[project_manager_tools],
)
def get_todo_dependencies(
    bot_id: str,
    todo_id: str,
    include_reverse: bool=False,
    thread_id: str=None
):
    """
    Get all dependencies for a specific todo item
    """
    return project_manager._get_todo_dependencies(
        bot_id=bot_id, todo_id=todo_id, include_reverse=include_reverse
    )


@gc_tool(
    action="Action to perform (ADD, REMOVE)",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    todo_id="ID of the todo that has the dependency",
    depends_on_todo_id="ID of the todo that needs to be completed first",
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[project_manager_tools],
)
def manage_todo_dependencies(
    action: str,
    bot_id: str,
    todo_id: str,
    depends_on_todo_id: str=None,
    thread_id: str=None
):
    """
    Manage dependencies between todo items, allowing you to specify that one todo must be completed before another can start
    """
    return project_manager.manage_todo_dependencies(
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
    _group_tags_=[project_manager_tools],
)
def manage_project_assets(
    action: str,
    bot_id: str,
    project_id: str,
    asset_id: str=None,
    asset_details: Dict=None,
    thread_id: str=None,
):
    """
    Manage project assets including their descriptions and locations in the git system
    """
    return project_manager.manage_project_assets(
        action=action,
        bot_id=bot_id,
        project_id=project_id,
        asset_id=asset_id,
        asset_details=asset_details,
    )


@gc_tool(
    todo_ids=ToolFuncParamDescriptor(
        name="todo_ids",
        description="Array of todo IDs to delete",
        required=True,
        llm_type_desc=dict(
            type="array",
            items=dict(type="string"),
        ),
    ),
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[project_manager_tools],
)
def delete_todos_bulk(
    todo_ids: List[str],
    bot_id: str,
    thread_id: str = None,
) -> None:
    """
    Delete multiple todo items in bulk. This will permanently remove the specified todos and their work history.
    """
    results = []
    for todo_id in todo_ids:
        result = project_manager.manage_todos(
            action="DELETE",
            bot_id=bot_id,
            todo_id=todo_id,
            thread_id=thread_id,
        )
        results.append(result)
    return results


@gc_tool(
    project_id=ToolFuncParamDescriptor(
        name="project_id",
        description="ID of the project the todos belong to",
        required=True,
        llm_type_desc=dict(type="string"),
    ),
    todos=ToolFuncParamDescriptor(
        name="todos",
        description="Array of todo items to create. Each todo requires todo_name, what_to_do, and optional depends_on.",
        required=True,
        llm_type_desc=dict(
            type="array",
            items=dict(
                type="object",
                properties=dict(
                    todo_name=dict(type="string", description="Name of the todo item"),
                    what_to_do=dict(type="string", description="What the todo item is about"),
                    assigned_to_bot_id=dict(
                        type="string",
                        description="The bot_id of the bot assigned to this todo. Omit to assign it to yourself.",
                    ),
                    depends_on=dict(
                        type=["string", "array"],
                        description="ID or array of IDs of todos that this todo depends on",
                        items=dict(type="string")
                    ),
                ),
                required=["todo_name", "what_to_do"]
            )
        ),
    ),
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[project_manager_tools],
)
def create_todos_bulk(
    project_id: str,
    todos: List[Dict[str, Any]],
    bot_id: str,
    thread_id: str = None,
) -> None:
    """
    Create multiple todo items in bulk for a project. Each todo in the array can specify its name,
    description, assignments, and dependencies.
    """
    results = []
    for todo in todos:
        todo_details = {
            "project_id": project_id,
            "todo_name": todo["todo_name"],
            "what_to_do": todo["what_to_do"],
            "depends_on": todo.get("depends_on", []),
        }
        if "assigned_to_bot_id" in todo:
            todo_details["assigned_to_bot_id"] = todo["assigned_to_bot_id"]
            
        result = project_manager.manage_todos(
            action="CREATE",
            bot_id=bot_id,
            todo_details=todo_details,
            thread_id=thread_id,
        )
        results.append(result)
    return results


project_manager_functions: List[Callable[..., Any]] = [
    manage_todos,
    manage_projects,
    record_todo_work,
    get_project_todos,
    get_todo_dependencies,
    manage_todo_dependencies,
    manage_project_assets,
    create_todos_bulk,
    delete_todos_bulk,
]


# Called from bot_os_tools.py to update the global list of functions
def get_project_manager_functions() -> List[Callable[..., Any]]:
    return project_manager_functions
