document.addEventListener("DOMContentLoaded", function () {
    document
      .getElementById("select-bot")
      .addEventListener("change", function (event) {
        const selectedBot = event.target.value;
        filterProjectsByBot(selectedBot);
      });

    function filterProjectsByBot(botId) {
        const botSelectElement = document.getElementById("select-bot");
        const botSelectValue = botSelectElement ? botSelectElement.value : null;
        const body = JSON.stringify({
          data: [[0, "list_projects " + botSelectValue]],
        });
        console.log("Selected bot value:", botSelectValue);
        fetch("http://localhost:8080/udf_proxy/get_metadata", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: body,
        })
            .then((response) => {
                console.log("Response:", response);
                return response.json()
            })
            .then((data) => {
                // Clear existing options
                data = data["data"][0][1]["projects"];
                console.log("Data:", data);
                const selectElement = document.getElementById("select-project");
                selectElement.innerHTML = "";

                // Add new options from filtered projects
                if (data.length === 0) {
                    console.log("No projects found");
                    const option = document.createElement("option");
                    option.value = "No projects found";
                    option.textContent = "No projects found";
                    selectElement.appendChild(option);
                    selectElement.value = option.textContent;

                }
                else {
                    console.log("Projects found:", data.length);
                    data.forEach((project) => {
                        const option = document.createElement("option");
                        option.value = project.project_id;
                        option.textContent = project.project_name;
                        selectElement.appendChild(option);
                    });
                }

                console.log('Updated project options:', selectElement.value);

                if (selectElement.value === "") {
                    selectElement.value = "Select a project";
                }
                else {
                    const body = JSON.stringify({
                      data: [[0, "list_todos " + selectElement.value]],
                    });
                    fetch("http://localhost:8080/udf_proxy/get_metadata", {
                        method: "POST",
                        headers: {
                            "Content-Type": "application/json",
                        },
                        body: body,
                    })
                    .then((response) => response.json())
                    .then((data) => {
                        console.log("Data:", data);
                        console.log("Selected project value:", selectElement.value);
                        const todos = data["data"][0][1][selectElement.value];
                        console.log("Todos:", todos);

                        const tableWrapper = document.getElementById("todo-table-wrapper");
                        const tableBody = document.getElementById("project-table").getElementsByTagName("tbody")[0];

                        // Clear existing table rows
                        tableBody.innerHTML = "";

                        // Populate table with new data
                        todos.forEach((todo) => {
                            const row = document.createElement("tr");

                            const projectIdCell = document.createElement("td");
                            projectIdCell.textContent = todo.project_id;
                            row.appendChild(projectIdCell);

                            const projectNameCell = document.createElement("td");
                            projectNameCell.textContent = todo.project_name;
                            row.appendChild(projectNameCell);

                            const todoCountCell = document.createElement("td");
                            todoCountCell.textContent = todo.todo_count;
                            row.appendChild(todoCountCell);

                            const createdAtCell = document.createElement("td");
                            createdAtCell.textContent = todo.created_at;
                            row.appendChild(createdAtCell);

                            tableBody.appendChild(row);
                        });

                        // Display the table wrapper
                        tableWrapper.style.display = "block";
                    })
                    .catch((error) => {
                        console.error("Error fetching todos:", error);
                    });
                }
            })
            .catch((error) => {
                console.error("Error filtering projects:", error);
            });
    }
});
