/** @jsxImportSource @emotion/react */
// import { jsx } from "@emotion/react";
import React, { useEffect, useState } from "react";

interface Project {
  project_id: string;
  project_name: string;
}

interface Todo {
  todo_id: string;
  todo_name: string;
  current_status: string;
  what_to_do: string;
}

interface ApiResponse<T> {
  data: [[number, { [key: string]: T[] }]];
}

const Projects: React.FC = () => {
  const [selectedBot, setSelectedBot] = useState<string>("");
  const [selectedProject, setSelectedProject] = useState<string>("");
  const [projects, setProjects] = useState<Project[]>([]);
  const [todos, setTodos] = useState<Todo[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string>("");

  useEffect(() => {
    if (selectedBot) {
      populateProjectDD(selectedBot);
    }
  }, [selectedBot]);

  useEffect(() => {
    if (selectedProject) {
      populateTodos(selectedProject);
    }
  }, [selectedProject]);

  const handleError = (error: Error, context: string) => {
    console.error(`Error ${context}:`, error);
    setError(`Failed to ${context}. Please try again.`);
    setLoading(false);
  };

  const populateProjectDD = async (botId: string) => {
    try {
      const response = await fetch(
        "http://localhost:8080/udf_proxy/get_metadata",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            data: [[0, `list_projects ${botId}`]],
          }),
        }
      );

      const data: ApiResponse<Project> = await response.json();
      setProjects(data.data[0][1].projects);
      setError("");
    } catch (err) {
      handleError(err as Error, "fetch projects");
    }
  };

  const populateTodos = async (projectId: string) => {
    setLoading(true);
    try {
      const response = await fetch(
        "http://localhost:8080/udf_proxy/get_metadata",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            data: [[0, `list_todos ${projectId}`]],
          }),
        }
      );

      const data: ApiResponse<Todo> = await response.json();
      setTodos(data.data[0][1].todos);
      setError("");
      setLoading(false);
    } catch (err) {
      handleError(err as Error, "fetch todos");
    }
  };

  const handleDeleteTodo = (todoId: string) => {
    // TODO: Implement delete functionality
    alert(`Delete todo with id: ${todoId}`);
  };

  return (
    <div className="container">
        <div className="left-column">
            <div id='logo'>
                <img src="http://localhost:8501/media/4d465ae1619cad8ef5279ea3b8d118de15d7edfd4a011bce87b32e3c.png" alt="Image" />
            </div>
            <p id='new-chat-button'>⚡  New Chat</p>
            <select id="dropdown">
            </select>
        </div>
        <div className="right-column">
            <h3 id="back_to_chat">← Back to Chat</h3>
            <div id="selector">
                <div className="select-wrapper">
                    <div className='dropdown-label'>
                    Select a bot:
                    </div>
                    <select
                      id="select-bot"
                      value={selectedBot}
                      onChange={(e) => setSelectedBot(e.target.value)}
                    >
                      <option value="">Select a bot</option>
                      {projects.map((project) => (
                    <select
                      id="select-project"
                      value={selectedProject}
                      onChange={(e) => setSelectedProject(e.target.value)}
                    >
                      <option value="">Select a project</option>
                      {todos.map((todo) => (
                        <option key={todo.todo_id} value={todo.todo_id}>
                          {todo.todo_name}
                        </option>
                      ))}
                    </select>
                      ))}
                    </select>
                </div>
                <div className="select-wrapper">
                    <div className='dropdown-label'>
                    Filter by project:
                    </div>
                    <select id = 'select-project'>
                        <option value="Select a bot first"></option>
                    </select>
                </div>
            </div>

        <div>
            <div id='spinner' className='spinner'></div>
            <div id='todo-table-wrapper'>
                <h2>Todo List:</h2>
                <table className="todo-table">
                    <thead>
                        <tr>
                            <th></th>
                            <th>Todo Id</th>
                            <th>Todo Name</th>
                            <th>Todo Status</th>
                            <th>What To Do</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
        </div>
    </div>
  );
}

export default Projects;