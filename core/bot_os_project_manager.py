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
            
            cursor.execute(create_projects_query)
            cursor.execute(create_project_history_query)
            cursor.execute(create_todos_query)
            cursor.execute(create_history_query)
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
                        "error": "Todo ID and new status are required"
                    }

                new_status = todo_details["new_status"].upper()
                if new_status not in self.VALID_STATUSES:
                    return {
                        "success": False, 
                        "error": f"Invalid status. Must be one of: {', '.join(self.VALID_STATUSES)}"
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
                        "error": "Todo not found or you don't have permission to modify it"
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
                WHERE todo_id = %s AND assigned_to_bot_id = %s
                """
                cursor.execute(update_query, (new_status, todo_id, bot_id))

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

                if not update_fields:
                    return {
                        "success": False,
                        "error": "No valid fields to update"
                    }

                update_values.append(todo_id)
                update_query = f"""
                UPDATE {self.db_adapter.schema}.TODO_ITEMS
                SET {", ".join(update_fields)}, updated_at = CURRENT_TIMESTAMP
                WHERE todo_id = %s
                """
                cursor.execute(update_query, update_values)
                
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

    def manage_projects(self, action, bot_id, project_id=None, project_details=None, thread_id = None, requested_by_user=None):
        """Manages projects with various actions."""
        action = action.upper()
        cursor = self.db_adapter.client.cursor()

        try:
            if action == "CREATE":
                if not project_details:
                    return {"success": False, "error": "Project details are required"}

                required_fields = ["project_name", "project_manager_bot_id"]
                missing_fields = [f for f in required_fields if f not in project_details]
                if missing_fields:
                    return {"success": False, "error": f"Missing required fields: {', '.join(missing_fields)}"}

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
                        project_details["project_manager_bot_id"],
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
                        "created_at": project[6],
                        "updated_at": project[7],
                        "todo_count": project[8],
                        "history": [
                            {
                                "action_taken": h[0],
                                "action_by_bot_id": h[1],
                                "action_details": h[2],
                                "action_timestamp": h[3]
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
                    "project_manager_bot_id": "Project manager changed",
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
