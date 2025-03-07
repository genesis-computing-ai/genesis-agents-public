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
    <div>
      <select
        id="select-bot"
        value={selectedBot}
        onChange={(e: React.ChangeEvent<HTMLSelectElement>) =>
          setSelectedBot(e.target.value)
        }
      >
        <option value="">Select a bot...</option>
        <option value="bot1">Bot 1</option>
        <option value="bot2">Bot 2</option>
      </select>

      <select
        id="select-project"
        value={selectedProject}
        onChange={(e: React.ChangeEvent<HTMLSelectElement>) =>
          setSelectedProject(e.target.value)
        }
      >
        <option value="">Select a project...</option>
        {projects.map((project) => (
          <option key={project.project_id} value={project.project_id}>
            {project.project_name}
          </option>
        ))}
      </select>

      {error && <div className="error-message">{error}</div>}

      {loading ? (
        <div id="spinner" className="spinner">
          Loading...
        </div>
      ) : (
        <div id="todo-table-wrapper" className="visible">
          <table className="todo-table">
            <thead>
              <tr>
                <th>Actions</th>
                <th>ID</th>
                <th>Name</th>
                <th>Status</th>
                <th>Description</th>
              </tr>
            </thead>
            <tbody>
              {todos.length === 0 ? (
                <tr>
                  <td colSpan={5}>No todos found</td>
                </tr>
              ) : (
                todos.map((todo) => (
                  <tr key={todo.todo_id}>
                    <td
                      className="delete-todo"
                      onClick={() => handleDeleteTodo(todo.todo_id)}
                    >
                      🗑️
                    </td>
                    <td>{todo.todo_id}</td>
                    <td>{todo.todo_name}</td>
                    <td>{todo.current_status}</td>
                    <td>{todo.what_to_do}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
};

export default Projects;
