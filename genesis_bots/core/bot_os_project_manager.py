import time
import random
from datetime import datetime

class ProjectManager:
    VALID_STATUSES = ["NEW", "IN_PROGRESS", "ON_HOLD", "COMPLETED", "CANCELLED"]

    def __init__(self, db_adapter):
        self.db_adapter = db_adapter
        self._ensure_tables_exist()

    def _ensure_tables_exist(self):
        """Create the necessary tables if they don't exist"""
        cursor = self.db_adapter.client.cursor()
        try:
            # Create PROJECTS table
            create_projects_query = f"""
            CREATE TABLE IF NOT EXISTS {self.db_adapter.schema}.PROJECTS (
                project_id VARCHAR(255) PRIMARY KEY,
                project_name VARCHAR(255) NOT NULL,
                description TEXT,
                project_manager_bot_id VARCHAR(255) NOT NULL,
                requested_by_user VARCHAR(255),
                current_status VARCHAR(50) NOT NULL,
                target_completion_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            

            # Create PROJECT_HISTORY table
            create_project_history_query = f"""
            CREATE TABLE IF NOT EXISTS {self.db_adapter.schema}.PROJECT_HISTORY (
                history_id VARCHAR(255) PRIMARY KEY,
                project_id VARCHAR(255) NOT NULL,
                action_taken VARCHAR(255) NOT NULL,
                action_by_bot_id VARCHAR(255) NOT NULL,
                action_details TEXT,
                action_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (project_id) REFERENCES {self.db_adapter.schema}.PROJECTS(project_id)
            )
            """

            # Modify TODO_ITEMS table to include project_id
            create_todos_query = f"""
            CREATE TABLE IF NOT EXISTS {self.db_adapter.schema}.TODO_ITEMS (
                todo_id VARCHAR(255) PRIMARY KEY,
                project_id VARCHAR(255) NOT NULL,
                todo_name VARCHAR(255) NOT NULL,
                current_status VARCHAR(50) NOT NULL,
                assigned_to_bot_id VARCHAR(255) NOT NULL,
                requested_by_user VARCHAR(255),
                what_to_do TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (project_id) REFERENCES {self.db_adapter.schema}.PROJECTS(project_id)
            )
            """

            # Create TODO_HISTORY table for tracking actions
            create_history_query = f"""
            CREATE TABLE IF NOT EXISTS {self.db_adapter.schema}.TODO_HISTORY (
                history_id VARCHAR(255) PRIMARY KEY,
                todo_id VARCHAR(255) NOT NULL,
                action_taken VARCHAR(255) NOT NULL,
                action_by_bot_id VARCHAR(255) NOT NULL,
                previous_status VARCHAR(50),
                current_status VARCHAR(50),
                status_changed_flag CHAR(1) DEFAULT 'N',
                work_description TEXT,
                work_results TEXT,
                action_details TEXT,
                action_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (todo_id) REFERENCES {self.db_adapter.schema}.TODO_ITEMS(todo_id)
            )
            """
            
            # Add new TODO_DEPENDENCIES table
            create_dependencies_query = f"""
            CREATE TABLE IF NOT EXISTS {self.db_adapter.schema}.TODO_DEPENDENCIES (
                dependency_id VARCHAR(255) PRIMARY KEY,
                todo_id VARCHAR(255) NOT NULL,
                depends_on_todo_id VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (todo_id) REFERENCES {self.db_adapter.schema}.TODO_ITEMS(todo_id),
                FOREIGN KEY (depends_on_todo_id) REFERENCES {self.db_adapter.schema}.TODO_ITEMS(todo_id),
                CONSTRAINT unique_dependency UNIQUE (todo_id, depends_on_todo_id)
            )
            """

            # Add new PROJECT_ASSETS table
            create_assets_query = f"""
            CREATE TABLE IF NOT EXISTS {self.db_adapter.schema}.PROJECT_ASSETS (
                asset_id VARCHAR(255) PRIMARY KEY,
                project_id VARCHAR(255) NOT NULL,
                description TEXT NOT NULL,
                git_path VARCHAR(1024) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (project_id) REFERENCES {self.db_adapter.schema}.PROJECTS(project_id)
            )
            """

            cursor.execute(create_projects_query)
            cursor.execute(create_project_history_query)
            cursor.execute(create_todos_query)
            cursor.execute(create_history_query)
            cursor.execute(create_dependencies_query)
            cursor.execute(create_assets_query)
            self.db_adapter.client.commit()
        finally:
            cursor.close()

    def manage_todos(self, action, bot_id, todo_id=None, todo_details=None, thread_id = None, requested_by_user=None ):
        """
        Manages todo items with various actions.
        
        Args:
            action (str): The action to perform (CREATE, UPDATE, CHANGE_STATUS, LIST, DELETE)
            bot_id (str): The ID of the bot performing the action
            todo_id (str, optional): The ID of the todo item
            todo_details (dict, optional): Details for creating/updating a todo item
        
        Returns:
            dict: Result of the operation
        """
        action = action.upper()
        cursor = self.db_adapter.client.cursor()

        try:
            if action == "LIST":
                # Get todos
                todos_query = f"""
                SELECT todo_id, todo_name, current_status, assigned_to_bot_id, 
                       what_to_do, created_at, updated_at
                FROM {self.db_adapter.schema}.TODO_ITEMS t
                WHERE assigned_to_bot_id = %s
                """
                cursor.execute(todos_query, (bot_id,))
                todos = cursor.fetchall()
                
                # For each todo, get its history separately
                result_todos = []
                for todo in todos:
                    history_query = f"""
                    SELECT action_taken, action_by_bot_id, action_details, action_timestamp
                    FROM {self.db_adapter.schema}.TODO_HISTORY
                    WHERE todo_id = %s
                    ORDER BY action_timestamp DESC
                    """
                    cursor.execute(history_query, (todo[0],))
                    history = cursor.fetchall()
                    
                    result_todos.append({
                        "todo_id": todo[0],
                        "todo_name": todo[1],
                        "current_status": todo[2],
                        "assigned_to_bot_id": todo[3],
                        "what_to_do": todo[4],
                        "created_at": todo[5],
                        "updated_at": todo[6],
                        "dependencies": self._get_todo_dependencies(cursor, todo[0]),
                        "history": [
                            {
                                "action_taken": h[0],
                                "action_by_bot_id": h[1],
                                "action_details": h[2],
                                "action_timestamp": h[3]
                            } for h in history
                        ]
                    })
                
                return {
                    "success": True,
                    "todos": result_todos
                }

            elif action == "CREATE":
                if not todo_details or "project_id" not in todo_details:
                    # Check if there's at least one project
                    cursor.execute(f"SELECT project_id FROM {self.db_adapter.schema}.PROJECTS LIMIT 1")
                    if not cursor.fetchone():
                        return {
                            "success": False,
                            "error": "No projects exist. Please create a project first. Suggestion: Create a 'General' project for one-off todos."
                        }
                    return {
                        "success": False,
                        "error": "Todo details and project_id are required for creation"
                    }

                # Verify project exists
                cursor.execute(
                    f"SELECT project_id FROM {self.db_adapter.schema}.PROJECTS WHERE project_id = %s",
                    (todo_details["project_id"],)
                )
                if not cursor.fetchone():
                    return {
                        "success": False,
                        "error": f"Project with ID {todo_details['project_id']} does not exist"
                    }

                # If assigned_to_bot_id is not specified, assign to the creating bot
                if not todo_details.get("assigned_to_bot_id"):
                    todo_details["assigned_to_bot_id"] = bot_id
                required_fields = ["todo_name", "what_to_do", "assigned_to_bot_id"]
                missing_fields = [f for f in required_fields if f not in todo_details]
                if missing_fields:
                    return {
                        "success": False,
                        "error": f"Missing required fields: {', '.join(missing_fields)}"
                    }

                # Generate unique todo_id
                todo_id = f"todo_{bot_id}_{int(time.time())}_{random.randint(1000, 9999)}"
                
                insert_query = f"""
                INSERT INTO {self.db_adapter.schema}.TODO_ITEMS 
                (todo_id, project_id, todo_name, current_status, assigned_to_bot_id, requested_by_user, what_to_do)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(
                    insert_query,
                    (
                        todo_id,
                        todo_details["project_id"],
                        todo_details["todo_name"],
                        "NEW",
                        todo_details["assigned_to_bot_id"],
                        requested_by_user,
                        todo_details["what_to_do"]
                    )
                )

                # Record creation in history
                self._add_history(cursor, todo_id, "CREATED", bot_id, "Todo item created")
                
                # Handle dependencies if specified
                if "depends_on" in todo_details and todo_details["depends_on"]:
                    dependencies = todo_details["depends_on"]
                    if not isinstance(dependencies, list):
                        dependencies = [dependencies]
                        
                    for depends_on_todo_id in dependencies:
                        dependency_id = f"dep_{todo_id}_{depends_on_todo_id}_{int(time.time())}"
                        try:
                            cursor.execute(
                                f"""
                                INSERT INTO {self.db_adapter.schema}.TODO_DEPENDENCIES
                                (dependency_id, todo_id, depends_on_todo_id)
                                VALUES (%s, %s, %s)
                                """,
                                (dependency_id, todo_id, depends_on_todo_id)
                            )
                            self._add_history(
                                cursor,
                                todo_id,
                                "DEPENDENCY_ADDED",
                                bot_id,
                                f"Added initial dependency on todo {depends_on_todo_id}"
                            )
                        except Exception as e:
                            # Log the error but continue with creation
                            print(f"Failed to add dependency {depends_on_todo_id}: {str(e)}")
                
                self.db_adapter.client.commit()
                return {
                    "success": True,
                    "message": "Todo created successfully",
                    "todo_id": todo_id
                }

            elif action == "CHANGE_STATUS":
                if not todo_id or "new_status" not in todo_details:
                    return {
                        "success": False,
                        "error": "Todo ID and new_status are required in todo_details"
                    }

                new_status = todo_details["new_status"].upper()
                if new_status not in self.VALID_STATUSES:
                    return {
                        "success": False, 
                        "error": f"Invalid status. Must be one of: {', '.join(self.VALID_STATUSES)}"
                    }

                # Verify todo exists
                cursor.execute(
                    f"""
                    SELECT current_status FROM {self.db_adapter.schema}.TODO_ITEMS 
                    WHERE todo_id = %s 
                    """,
                    (todo_id)
                )
                result = cursor.fetchone()
                if not result:
                    return {
                        "success": False,
                        "error": "Todo not found"
                    }

                old_status = result[0]
                if old_status == new_status:
                    return {
                        "success": True,
                        "message": f"Todo already in {new_status} status"
                    }

                # Update todo status
                update_query = f"""
                UPDATE {self.db_adapter.schema}.TODO_ITEMS 
                SET current_status = %s, updated_at = CURRENT_TIMESTAMP
                WHERE todo_id = %s 
                """
                cursor.execute(update_query, (new_status, todo_id))

                # Add history entry with status tracking
                work_description = todo_details.get('work_description')
                work_results = todo_details.get('work_results')
                self._add_history(
                    cursor,
                    todo_id,
                    "STATUS_CHANGED",
                    bot_id,
                    f"Status changed from {old_status} to {new_status}",
                    previous_status=old_status,
                    current_status=new_status,
                    work_description=work_description,
                    work_results=work_results
                )

                self.db_adapter.client.commit()
                return {"success": True, "message": f"Todo status changed to {new_status}"}

            elif action == "UPDATE":
                if not todo_id or not todo_details:
                    return {
                        "success": False,
                        "error": "Todo ID and update details are required"
                    }

                update_fields = []
                update_values = []
                for field in ["todo_name", "what_to_do", "assigned_to_bot_id"]:
                    if field in todo_details:
                        update_fields.append(f"{field} = %s")
                        update_values.append(todo_details[field])

                if not update_fields and "depends_on" not in todo_details:
                    return {
                        "success": False,
                        "error": "No valid fields to update"
                    }

                if update_fields:
                    update_values.append(todo_id)
                    update_query = f"""
                    UPDATE {self.db_adapter.schema}.TODO_ITEMS
                    SET {", ".join(update_fields)}, updated_at = CURRENT_TIMESTAMP
                    WHERE todo_id = %s
                    """
                    cursor.execute(update_query, update_values)
                
                # Handle dependency updates if specified
                if "depends_on" in todo_details:
                    # Get current dependencies
                    cursor.execute(
                        f"""
                        SELECT depends_on_todo_id 
                        FROM {self.db_adapter.schema}.TODO_DEPENDENCIES
                        WHERE todo_id = %s
                        """,
                        (todo_id,)
                    )
                    current_deps = {row[0] for row in cursor.fetchall()}
                    
                    # Convert new dependencies to set
                    new_deps = todo_details["depends_on"]
                    if new_deps is None:
                        new_deps = set()
                    elif isinstance(new_deps, str):
                        new_deps = {new_deps}
                    else:
                        new_deps = set(new_deps)
                    
                    # Remove dependencies that are no longer needed
                    deps_to_remove = current_deps - new_deps
                    if deps_to_remove:
                        cursor.execute(
                            f"""
                            DELETE FROM {self.db_adapter.schema}.TODO_DEPENDENCIES
                            WHERE todo_id = %s AND depends_on_todo_id = ANY(%s)
                            """,
                            (todo_id, list(deps_to_remove))
                        )
                        for dep_id in deps_to_remove:
                            self._add_history(
                                cursor,
                                todo_id,
                                "DEPENDENCY_REMOVED",
                                bot_id,
                                f"Removed dependency on todo {dep_id}"
                            )
                    
                    # Add new dependencies
                    deps_to_add = new_deps - current_deps
                    for dep_id in deps_to_add:
                        dependency_id = f"dep_{todo_id}_{dep_id}_{int(time.time())}"
                        try:
                            cursor.execute(
                                f"""
                                INSERT INTO {self.db_adapter.schema}.TODO_DEPENDENCIES
                                (dependency_id, todo_id, depends_on_todo_id)
                                VALUES (%s, %s, %s)
                                """,
                                (dependency_id, todo_id, dep_id)
                            )
                            self._add_history(
                                cursor,
                                todo_id,
                                "DEPENDENCY_ADDED",
                                bot_id,
                                f"Added dependency on todo {dep_id}"
                            )
                        except Exception as e:
                            print(f"Failed to add dependency {dep_id}: {str(e)}")
                
                # Record update in history
                self._add_history(
                    cursor,
                    todo_id,
                    "UPDATED",
                    bot_id,
                    f"Todo details updated: {', '.join(todo_details.keys())}"
                )
                
                self.db_adapter.client.commit()
                return {
                    "success": True,
                    "message": "Todo updated successfully"
                }

            elif action == "DELETE":
                if not todo_id:
                    return {
                        "success": False,
                        "error": "Todo ID is required for deletion"
                    }

                # Verify todo exists and bot has permission
                cursor.execute(
                    f"""
                    SELECT todo_id FROM {self.db_adapter.schema}.TODO_ITEMS 
                    WHERE todo_id = %s AND assigned_to_bot_id = %s
                    """,
                    (todo_id, bot_id)
                )
                if not cursor.fetchone():
                    return {
                        "success": False,
                        "error": "Todo not found or you don't have permission to delete it"
                    }

                # Delete dependencies first (both where this todo depends on others and where others depend on this)
                cursor.execute(
                    f"""
                    DELETE FROM {self.db_adapter.schema}.TODO_DEPENDENCIES
                    WHERE todo_id = %s OR depends_on_todo_id = %s
                    """,
                    (todo_id, todo_id)
                )

                # Delete todo history
                cursor.execute(
                    f"""
                    DELETE FROM {self.db_adapter.schema}.TODO_HISTORY
                    WHERE todo_id = %s
                    """,
                    (todo_id,)
                )

                # Delete the todo
                cursor.execute(
                    f"""
                    DELETE FROM {self.db_adapter.schema}.TODO_ITEMS
                    WHERE todo_id = %s AND assigned_to_bot_id = %s
                    """,
                    (todo_id, bot_id)
                )

                self.db_adapter.client.commit()
                return {
                    "success": True,
                    "message": "Todo and all related records deleted successfully",
                    "todo_id": todo_id
                }

            else:
                return {
                    "success": False,
                    "error": f"Invalid action: {action}"
                }

        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
        finally:
            cursor.close()

    def _add_history(self, cursor, todo_id, action_taken, action_by_bot_id, action_details, 
                    previous_status=None, current_status=None, work_description=None, work_results=None):
        """Helper method to add an entry to the todo history"""
        history_id = f"hist_{todo_id}_{int(time.time())}_{random.randint(1000, 9999)}"
        status_changed_flag = 'Y' if (previous_status and current_status and previous_status != current_status) else 'N'
        
        insert_query = f"""
        INSERT INTO {self.db_adapter.schema}.TODO_HISTORY 
        (history_id, todo_id, action_taken, action_by_bot_id, action_details,
         previous_status, current_status, status_changed_flag, work_description, work_results)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(
            insert_query,
            (history_id, todo_id, action_taken, action_by_bot_id, action_details,
             previous_status, current_status, status_changed_flag, work_description, work_results)
        )

    def manage_projects(self, action, bot_id, project_id=None, project_details=None, thread_id = None, requested_by_user=None, static_project_id = False):
        """Manages projects with various actions."""
        action = action.upper()
        cursor = self.db_adapter.client.cursor()

        try:
            if action == "CREATE":
                if not project_details:
                    return {"success": False, "error": "Project details are required"}

                required_fields = ["project_name", "description"]
                missing_fields = [f for f in required_fields if f not in project_details]
                if missing_fields:
                    return {"success": False, "error": f"Missing required fields: {', '.join(missing_fields)}"}

                if static_project_id:
                    project_id = project_id
                else:
                    project_id = f"proj_{bot_id}_{int(time.time())}_{random.randint(1000, 9999)}"
                
                insert_query = f"""
                INSERT INTO {self.db_adapter.schema}.PROJECTS 
                (project_id, project_name, description, project_manager_bot_id, requested_by_user, current_status, target_completion_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(
                    insert_query,
                    (
                        project_id,
                        project_details["project_name"],
                        project_details.get("description", ""),
                        bot_id,
                        requested_by_user,
                        project_details.get("current_status", "NEW"),
                        project_details.get("target_completion_date")
                    )
                )

                self._add_project_history(cursor, project_id, "CREATED", bot_id, "Project created")
                self.db_adapter.client.commit()
                
                return {"success": True, "message": "Project created successfully", "project_id": project_id}

            elif action == "LIST":
                projects_query = f"""
                SELECT p.project_id, p.project_name, p.description, p.project_manager_bot_id,
                       p.current_status, p.target_completion_date, p.created_at, p.updated_at,
                       COUNT(t.todo_id) as todo_count
                FROM {self.db_adapter.schema}.PROJECTS p
                LEFT JOIN {self.db_adapter.schema}.TODO_ITEMS t ON p.project_id = t.project_id
                WHERE p.project_manager_bot_id = %s
                GROUP BY p.project_id, p.project_name, p.description, p.project_manager_bot_id,
                         p.current_status, p.target_completion_date, p.created_at, p.updated_at
                """
                cursor.execute(projects_query, (bot_id,))
                projects = cursor.fetchall()
                
                result_projects = []
                for project in projects:
                    history_query = f"""
                    SELECT action_taken, action_by_bot_id, action_details, action_timestamp
                    FROM {self.db_adapter.schema}.PROJECT_HISTORY
                    WHERE project_id = %s
                    ORDER BY action_timestamp DESC
                    """
                    cursor.execute(history_query, (project[0],))
                    history = cursor.fetchall()
                    
                    result_projects.append({
                        "project_id": project[0],
                        "project_name": project[1],
                        "description": project[2],
                        "project_manager_bot_id": project[3],
                        "current_status": project[4],
                        "target_completion_date": project[5],
                        "created_at": str(project[6]),
                        "updated_at": str(project[7]),
                        "todo_count": project[8],
                        "history": [
                            {
                                "action_taken": h[0],
                                "action_by_bot_id": h[1],
                                "action_details": h[2],
                                "action_timestamp": str(h[3])
                            } for h in history
                        ]
                    })
                
                return {"success": True, "projects": result_projects}

            elif action == "UPDATE":
                if not project_id or not project_details:
                    return {"success": False, "error": "Project ID and update details are required"}

                # Verify project exists and bot has permission
                cursor.execute(
                    f"""
                    SELECT project_id FROM {self.db_adapter.schema}.PROJECTS 
                    WHERE project_id = %s AND project_manager_bot_id = %s
                    """,
                    (project_id, bot_id)
                )
                if not cursor.fetchone():
                    return {
                        "success": False,
                        "error": "Project not found or you don't have permission to modify it"
                    }

                # Build update query dynamically based on provided fields
                allowed_fields = {
                    "project_name": "Project name updated",
                    "description": "Description updated",
                    "bot_id": "Project manager changed",
                    "target_completion_date": "Target completion date updated"
                }

                update_fields = []
                update_values = []
                history_notes = []

                for field, value in project_details.items():
                    if field in allowed_fields:
                        update_fields.append(f"{field} = %s")
                        update_values.append(value)
                        history_notes.append(allowed_fields[field])

                if not update_fields:
                    return {"success": False, "error": "No valid fields to update"}

                # Add updated_at to the update
                update_fields.append("updated_at = CURRENT_TIMESTAMP")
                
                update_query = f"""
                UPDATE {self.db_adapter.schema}.PROJECTS 
                SET {', '.join(update_fields)}
                WHERE project_id = %s AND project_manager_bot_id = %s
                """
                update_values.extend([project_id, bot_id])
                
                cursor.execute(update_query, tuple(update_values))
                
                # Add history entry
                self._add_project_history(
                    cursor, 
                    project_id, 
                    "UPDATED", 
                    bot_id, 
                    f"Project updated: {'; '.join(history_notes)}"
                )
                
                self.db_adapter.client.commit()
                return {"success": True, "message": "Project updated successfully"}

            elif action == "CHANGE_STATUS":
                if not project_id or "new_status" not in project_details:
                    return {"success": False, "error": "Project ID and new status are required"}

                new_status = project_details["new_status"].upper()
                if new_status not in self.VALID_STATUSES:
                    return {
                        "success": False, 
                        "error": f"Invalid status. Must be one of: {', '.join(self.VALID_STATUSES)}"
                    }

                # Verify project exists and bot has permission
                cursor.execute(
                    f"""
                    SELECT current_status FROM {self.db_adapter.schema}.PROJECTS 
                    WHERE project_id = %s AND project_manager_bot_id = %s
                    """,
                    (project_id, bot_id)
                )
                result = cursor.fetchone()
                if not result:
                    return {
                        "success": False,
                        "error": "Project not found or you don't have permission to modify it"
                    }

                old_status = result[0]
                if old_status == new_status:
                    return {
                        "success": True,
                        "message": f"Project already in {new_status} status"
                    }

                # Update project status
                update_query = f"""
                UPDATE {self.db_adapter.schema}.PROJECTS 
                SET current_status = %s, updated_at = CURRENT_TIMESTAMP
                WHERE project_id = %s AND project_manager_bot_id = %s
                """
                cursor.execute(update_query, (new_status, project_id, bot_id))

                # Add history entry
                self._add_project_history(
                    cursor,
                    project_id,
                    "STATUS_CHANGED",
                    bot_id,
                    f"Status changed from {old_status} to {new_status}"
                )

                self.db_adapter.client.commit()
                return {"success": True, "message": f"Project status changed to {new_status}"}

            elif action == "REMOVE" or action == "DELETE":
                if not project_id:
                    return {"success": False, "error": "Project ID is required for deletion"}

                # Verify project exists and bot has permission
                cursor.execute(
                    f"""
                    SELECT project_id FROM {self.db_adapter.schema}.PROJECTS 
                    WHERE project_id = %s AND project_manager_bot_id = %s
                    """,
                    (project_id, bot_id)
                )
                if not cursor.fetchone():
                    return {
                        "success": False,
                        "error": "Project not found or you don't have permission to delete it"
                    }

                # Delete project assets
                cursor.execute(
                    f"""
                    DELETE FROM {self.db_adapter.schema}.PROJECT_ASSETS
                    WHERE project_id = %s
                    """,
                    (project_id,)
                )

                # Delete todo dependencies for all todos in the project
                cursor.execute(
                    f"""
                    DELETE FROM {self.db_adapter.schema}.TODO_DEPENDENCIES
                    WHERE todo_id IN (
                        SELECT todo_id FROM {self.db_adapter.schema}.TODO_ITEMS
                        WHERE project_id = %s
                    ) OR depends_on_todo_id IN (
                        SELECT todo_id FROM {self.db_adapter.schema}.TODO_ITEMS
                        WHERE project_id = %s
                    )
                    """,
                    (project_id, project_id)
                )

                # Delete todo history for all todos in the project
                cursor.execute(
                    f"""
                    DELETE FROM {self.db_adapter.schema}.TODO_HISTORY
                    WHERE todo_id IN (
                        SELECT todo_id FROM {self.db_adapter.schema}.TODO_ITEMS
                        WHERE project_id = %s
                    )
                    """,
                    (project_id,)
                )

                # Delete all todos in the project
                cursor.execute(
                    f"""
                    DELETE FROM {self.db_adapter.schema}.TODO_ITEMS
                    WHERE project_id = %s
                    """,
                    (project_id,)
                )

                # Delete project history
                cursor.execute(
                    f"""
                    DELETE FROM {self.db_adapter.schema}.PROJECT_HISTORY
                    WHERE project_id = %s
                    """,
                    (project_id,)
                )

                # Finally, delete the project
                cursor.execute(
                    f"""
                    DELETE FROM {self.db_adapter.schema}.PROJECTS
                    WHERE project_id = %s AND project_manager_bot_id = %s
                    """,
                    (project_id, bot_id)
                )

                self.db_adapter.client.commit()
                return {
                    "success": True,
                    "message": "Project and all related records deleted successfully"
                }

            else:
                return {"success": False, "error": f"Unknown action: {action}"}

        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            cursor.close()

    def _add_project_history(self, cursor, project_id, action_taken, action_by_bot_id, action_details):
        """Helper method to add an entry to the project history"""
        history_id = f"proj_hist_{project_id}_{int(time.time())}_{random.randint(1000, 9999)}"
        insert_query = f"""
        INSERT INTO {self.db_adapter.schema}.PROJECT_HISTORY 
        (history_id, project_id, action_taken, action_by_bot_id, action_details)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(
            insert_query,
            (history_id, project_id, action_taken, action_by_bot_id, action_details)
        )

    def record_work(self, bot_id, todo_id, work_description=None, work_results=None, thread_id=None, work_details=None):
        """
        Records work progress on a todo item without changing its status.
        
        Args:
            bot_id (str): The ID of the bot recording the work
            todo_id (str): The ID of the todo item
            work_description (str, optional): Description of work performed
            work_results (str, optional): Results or output of the work
            thread_id (str, optional): Associated thread ID
            work_details (dict, optional): Additional work details
        
        Returns:
            dict: Result of the operation
        """
        cursor = self.db_adapter.client.cursor()
        
        try:
            # If work_details is provided, extract description and results
            if work_details:
                work_description = work_details.get('description', work_description)
                work_results = work_details.get('results', work_results)

            if not work_description:
                return {
                    "success": False,
                    "error": "Work description is required"
                }

            # Verify todo exists and bot has permission
            cursor.execute(
                f"""
                SELECT current_status FROM {self.db_adapter.schema}.TODO_ITEMS 
                WHERE todo_id = %s AND assigned_to_bot_id = %s
                """,
                (todo_id, bot_id)
            )
            result = cursor.fetchone()
            if not result:
                return {
                    "success": False,
                    "error": "Todo not found or you don't have permission to record work on it"
                }
            
            current_status = result[0]
            
            # Add history entry for work progress
            self._add_history(
                cursor,
                todo_id,
                "WORK_RECORDED",
                bot_id,
                "Work progress recorded",
                previous_status=current_status,
                current_status=current_status,
                work_description=work_description,
                work_results=work_results
            )
            
            # Update the todo's updated_at timestamp
            update_query = f"""
            UPDATE {self.db_adapter.schema}.TODO_ITEMS 
            SET updated_at = CURRENT_TIMESTAMP
            WHERE todo_id = %s
            """
            cursor.execute(update_query, (todo_id,))
            
            self.db_adapter.client.commit()
            return {
                "success": True,
                "message": "Work progress recorded successfully"
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
        finally:
            cursor.close()

    def get_todo_history(self, todo_id):
        """
        Gets the history of a todo item.
        
        Args:
            todo_id (str): The ID of the todo to get history for
            
        Returns:
            dict: Contains success status and either history entries or error message
        """
        cursor = self.db_adapter.client.cursor()
        
        try:
            # Get all history entries for this todo
            cursor.execute(
                f"""
                SELECT history_id, action_taken, action_by_bot_id, action_details,
                       previous_status, current_status, status_changed_flag,
                       work_description, work_results, created_at
                FROM {self.db_adapter.schema}.TODO_HISTORY
                WHERE todo_id = %s
                ORDER BY created_at DESC
                """,
                (todo_id,)
            )
            
            history_entries = []
            for row in cursor.fetchall():
                history_entries.append({
                    "history_id": row[0],
                    "action_taken": row[1],
                    "action_by_bot_id": row[2],
                    "action_details": row[3],
                    "previous_status": row[4],
                    "current_status": row[5],
                    "status_changed": row[6] == 'Y',
                    "work_description": row[7],
                    "work_results": row[8],
                    "created_at": str(row[9])
                })
                
            return {
                "success": True,
                "history": history_entries
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
        finally:
            cursor.close()


    def manage_todo_dependencies(self, action, bot_id, todo_id, depends_on_todo_id=None):
        """
        Manages todo dependencies.
        
        Args:
            action (str): ADD or REMOVE dependency
            bot_id (str): The ID of the bot performing the action
            todo_id (str): The ID of the todo that has the dependency
            depends_on_todo_id (str): The ID of the todo that needs to be completed first
        """
        action = action.upper()
        cursor = self.db_adapter.client.cursor()
        
        try:
            # Verify bot has permission to modify the todo
            cursor.execute(
                f"""
                SELECT assigned_to_bot_id FROM {self.db_adapter.schema}.TODO_ITEMS 
                WHERE todo_id = %s AND assigned_to_bot_id = %s
                """,
                (todo_id, bot_id)
            )
            if not cursor.fetchone():
                return {
                    "success": False,
                    "error": "Todo not found or you don't have permission to modify it"
                }

            if action == "ADD":
                if not depends_on_todo_id:
                    return {
                        "success": False,
                        "error": "depends_on_todo_id is required for adding dependency"
                    }

                # Check if the dependency already exists
                cursor.execute(
                    f"""
                    SELECT dependency_id FROM {self.db_adapter.schema}.TODO_DEPENDENCIES
                    WHERE todo_id = %s AND depends_on_todo_id = %s
                    """,
                    (todo_id, depends_on_todo_id)
                )
                if cursor.fetchone():
                    return {
                        "success": False,
                        "error": "Dependency already exists"
                    }

                # Add the dependency
                dependency_id = f"dep_{todo_id}_{depends_on_todo_id}_{int(time.time())}"
                cursor.execute(
                    f"""
                    INSERT INTO {self.db_adapter.schema}.TODO_DEPENDENCIES
                    (dependency_id, todo_id, depends_on_todo_id)
                    VALUES (%s, %s, %s)
                    """,
                    (dependency_id, todo_id, depends_on_todo_id)
                )

                # Record in history
                self._add_history(
                    cursor,
                    todo_id,
                    "DEPENDENCY_ADDED",
                    bot_id,
                    f"Added dependency on todo {depends_on_todo_id}"
                )

            elif action == "REMOVE":
                if not depends_on_todo_id:
                    return {
                        "success": False,
                        "error": "depends_on_todo_id is required for removing dependency"
                    }

                cursor.execute(
                    f"""
                    DELETE FROM {self.db_adapter.schema}.TODO_DEPENDENCIES
                    WHERE todo_id = %s AND depends_on_todo_id = %s
                    """,
                    (todo_id, depends_on_todo_id)
                )

                # Record in history
                self._add_history(
                    cursor,
                    todo_id,
                    "DEPENDENCY_REMOVED",
                    bot_id,
                    f"Removed dependency on todo {depends_on_todo_id}"
                )

            self.db_adapter.client.commit()
            return {"success": True, "message": f"Dependency {action.lower()}ed successfully"}

        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            cursor.close()

    def _get_todo_dependencies(self, cursor, todo_id):
        """Helper method to get dependencies for a todo"""
        cursor.execute(
            f"""
            SELECT d.depends_on_todo_id, t.todo_name, t.current_status
            FROM {self.db_adapter.schema}.TODO_DEPENDENCIES d
            JOIN {self.db_adapter.schema}.TODO_ITEMS t ON d.depends_on_todo_id = t.todo_id
            WHERE d.todo_id = %s
            """,
            (todo_id,)
        )
        return [{
            "todo_id": row[0],
            "todo_name": row[1],
            "status": row[2]
        } for row in cursor.fetchall()]

    def get_project_todos(self, bot_id, project_id):
        """
        Get all todos for a specific project.
        
        Args:
            bot_id (str): The ID of the bot requesting the todos
            project_id (str): The ID of the project
        
        Returns:
            dict: Result containing todos or error message
        """
        cursor = self.db_adapter.client.cursor()
        
        try:
            # Verify project exists and bot has permission
            cursor.execute(
                f"""
                SELECT project_id FROM {self.db_adapter.schema}.PROJECTS 
                WHERE project_id = %s 
                """,
                (project_id)
            )

            project = cursor.fetchone()
            if not project:
                # Project not found, get list of valid projects
                if bot_id is not None:
                    projects_result = self.manage_projects(
                        action="LIST", 
                        bot_id=bot_id
                    )
                else:
                    return {
                        "success": False,
                        "error": "Project not found"
                    }
                if not projects_result.get("projects"):
                    return {
                        "success": False,
                        "error": "You don't currently have any projects"
                    }
                    
                return {
                    "success": False,
                    "error": "Project not found. Your valid projects are: " + 
                            ", ".join([p["project_id"] for p in projects_result["projects"]])
                }

            # Get todos for the project
            todos_query = f"""
            SELECT todo_id, todo_name, current_status, assigned_to_bot_id, 
                   what_to_do, created_at, updated_at
            FROM {self.db_adapter.schema}.TODO_ITEMS
            WHERE project_id = %s
            """
            cursor.execute(todos_query, (project_id,))
            todos = cursor.fetchall()
            
            # Format results similar to LIST action
            result_todos = []
            for todo in todos:
                history_query = f"""
                SELECT action_taken, action_by_bot_id, action_details, action_timestamp, work_description, work_results, previous_status, current_status, status_changed_flag
                FROM {self.db_adapter.schema}.TODO_HISTORY
                WHERE todo_id = %s
                ORDER BY action_timestamp DESC
                """
                cursor.execute(history_query, (todo[0],))
                history = cursor.fetchall()
                
                result_todos.append({
                    "todo_id": todo[0],
                    "todo_name": todo[1],
                    "current_status": todo[2],
                    "assigned_to_bot_id": todo[3],
                    "what_to_do": todo[4],
                    "created_at": str(todo[5]),
                    "updated_at": str(todo[6]),
                    "dependencies": self._get_todo_dependencies(cursor, todo[0]),
                    "history": [
                        {
                            "action_taken": h[0],
                            "action_by_bot_id": h[1],
                            "action_details": h[2],
                            "action_timestamp": str(h[3]),
                            "work_description": h[4],
                            "work_results": h[5],
                            "previous_status": h[6],
                            "current_status": h[7],
                            "status_changed_flag": h[8]
                        } for h in history
                    ]
                })
            
            return {
                "success": True,
                "project_id": project_id,
                "todos": result_todos
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            cursor.close()

    def get_todo_dependencies(self, bot_id, todo_id, include_reverse=False):
        """
        Get dependencies for a specific todo.
        
        Args:
            bot_id (str): The ID of the bot requesting the dependencies
            todo_id (str): The ID of the todo
            include_reverse (bool): If True, also include todos that depend on this todo
        
        Returns:
            dict: Result containing dependencies or error message
        """
        cursor = self.db_adapter.client.cursor()
        
        try:
            # Verify todo exists and bot has permission
            cursor.execute(
                f"""
                SELECT todo_id FROM {self.db_adapter.schema}.TODO_ITEMS 
                WHERE todo_id = %s AND assigned_to_bot_id = %s
                """,
                (todo_id, bot_id)
            )
            if not cursor.fetchone():
                return {
                    "success": False,
                    "error": "Todo not found or you don't have permission to view it"
                }

            # Get direct dependencies (todos this todo depends on)
            dependencies = self._get_todo_dependencies(cursor, todo_id)
            
            result = {
                "success": True,
                "todo_id": todo_id,
                "depends_on": dependencies
            }
            
            # Optionally get reverse dependencies (todos that depend on this todo)
            if include_reverse:
                cursor.execute(
                    f"""
                    SELECT d.todo_id, t.todo_name, t.current_status
                    FROM {self.db_adapter.schema}.TODO_DEPENDENCIES d
                    JOIN {self.db_adapter.schema}.TODO_ITEMS t ON d.todo_id = t.todo_id
                    WHERE d.depends_on_todo_id = %s
                    """,
                    (todo_id,)
                )
                reverse_deps = [{
                    "todo_id": row[0],
                    "todo_name": row[1],
                    "status": row[2]
                } for row in cursor.fetchall()]
                
                result["depended_on_by"] = reverse_deps
            
            return result
            
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            cursor.close()

    def manage_project_assets(self, action, bot_id, project_id, asset_id=None, asset_details=None):
        """
        Manages project assets.
        
        Args:
            action (str): CREATE, UPDATE, DELETE, or LIST
            bot_id (str): The ID of the bot performing the action
            project_id (str): The ID of the project
            asset_id (str, optional): The ID of the asset for updates/deletes
            asset_details (dict, optional): Details for creating/updating an asset
                {
                    "description": str,
                    "git_path": str
                }
        
        Returns:
            dict: Result of the operation
        """
        action = action.upper()
        cursor = self.db_adapter.client.cursor()
        
        try:
            # Verify project exists and bot has permission
            cursor.execute(
                f"""
                SELECT project_id FROM {self.db_adapter.schema}.PROJECTS 
                WHERE project_id = %s AND project_manager_bot_id = %s
                """,
                (project_id, bot_id)
            )
            project = cursor.fetchone()
            if not project:
                # Project not found, get list of valid projects
                projects_result = self.manage_projects(
                    action="LIST", 
                    bot_id=bot_id
                )
                    
                if not projects_result.get("projects"):
                    return {
                        "success": False,
                        "error": "You don't currently have any projects"
                    }
                    
                return {
                    "success": False,
                    "error": "Project not found. Your valid projects are: " + 
                            ", ".join([p["project_id"] for p in projects_result["projects"]])
                }

            if action == "CREATE":
                if not asset_details or "description" not in asset_details or "git_path" not in asset_details:
                    return {
                        "success": False,
                        "error": "Asset details must include description and git_path"
                    }

                asset_id = f"asset_{project_id}_{int(time.time())}_{random.randint(1000, 9999)}"
                
                cursor.execute(
                    f"""
                    INSERT INTO {self.db_adapter.schema}.PROJECT_ASSETS
                    (asset_id, project_id, description, git_path)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (asset_id, project_id, asset_details["description"], asset_details["git_path"])
                )
                
                self._add_project_history(
                    cursor,
                    project_id,
                    "ASSET_CREATED",
                    bot_id,
                    f"Added asset: {asset_details['git_path']}"
                )
                
                self.db_adapter.client.commit()
                return {
                    "success": True,
                    "message": "Asset created successfully",
                    "asset_id": asset_id
                }

            elif action == "UPDATE":
                if not asset_id or not asset_details:
                    return {
                        "success": False,
                        "error": "Asset ID and update details are required"
                    }

                update_fields = []
                update_values = []
                
                if "description" in asset_details:
                    update_fields.append("description = %s")
                    update_values.append(asset_details["description"])
                
                if "git_path" in asset_details:
                    update_fields.append("git_path = %s")
                    update_values.append(asset_details["git_path"])
                
                if not update_fields:
                    return {
                        "success": False,
                        "error": "No valid fields to update"
                    }
                
                update_fields.append("updated_at = CURRENT_TIMESTAMP")
                update_values.extend([asset_id, project_id])
                
                cursor.execute(
                    f"""
                    UPDATE {self.db_adapter.schema}.PROJECT_ASSETS
                    SET {", ".join(update_fields)}
                    WHERE asset_id = %s AND project_id = %s
                    """,
                    tuple(update_values)
                )
                
                self._add_project_history(
                    cursor,
                    project_id,
                    "ASSET_UPDATED",
                    bot_id,
                    f"Updated asset: {asset_id}"
                )
                
                self.db_adapter.client.commit()
                return {
                    "success": True,
                    "message": "Asset updated successfully"
                }

            elif action == "DELETE":
                if not asset_id:
                    return {
                        "success": False,
                        "error": "Asset ID is required for deletion"
                    }

                cursor.execute(
                    f"""
                    DELETE FROM {self.db_adapter.schema}.PROJECT_ASSETS
                    WHERE asset_id = %s AND project_id = %s
                    """,
                    (asset_id, project_id)
                )
                
                self._add_project_history(
                    cursor,
                    project_id,
                    "ASSET_DELETED",
                    bot_id,
                    f"Deleted asset: {asset_id}"
                )
                
                self.db_adapter.client.commit()
                return {
                    "success": True,
                    "message": "Asset deleted successfully"
                }

            elif action == "LIST":
                cursor.execute(
                    f"""
                    SELECT asset_id, description, git_path, created_at, updated_at
                    FROM {self.db_adapter.schema}.PROJECT_ASSETS
                    WHERE project_id = %s
                    ORDER BY created_at DESC
                    """,
                    (project_id,)
                )
                
                assets = [{
                    "asset_id": row[0],
                    "description": row[1],
                    "git_path": row[2],
                    "created_at": row[3],
                    "updated_at": row[4]
                } for row in cursor.fetchall()]
                
                return {
                    "success": True,
                    "project_id": project_id,
                    "assets": assets
                }

            else:
                return {
                    "success": False,
                    "error": f"Invalid action: {action}"
                }

        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            cursor.close()
