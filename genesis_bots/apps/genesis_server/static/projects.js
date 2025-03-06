document.addEventListener("DOMContentLoaded", function () {
        const selectedBot = event.target.value;
        populateProjectDD(selectedBot);
});

document.addEventListener("DOMContentLoaded", function () {
    document
      .getElementById("select-bot")
      .addEventListener("change", function (event) {
        const selectedBot = event.target.value;
        const spinner = document.getElementById("spinner");
        const tableWrapper = document.getElementById("todo-table-wrapper");
        spinner.classList.add("visible");
        // tableWrapper.classList.remove("visible");
        populateProjectDD(selectedBot);
        spinner.classList.remove("visible");
        tableWrapper.classList.add("visible");

      });
});

document.addEventListener("DOMContentLoaded", function () {
    document
      .getElementById("select-project")
      .addEventListener("change", function (event) {
        const selectedProject = event.target.value;
        populateTodos(selectedProject);
      });
});

function populateTodos(projectId) {
    const projectSelectElement = document.getElementById("select-project");
    const projectSelectValue = projectSelectElement ? projectSelectElement.value : null;
    const body = JSON.stringify({
            data: [[0, "list_todos " + projectSelectValue]],
        });
    const spinner= document.getElementById("spinner");
    const tableWrapper = document.getElementById("todo-table-wrapper");
    spinner.classList.add("visible");
    // tableWrapper.classList.remove("visible");
    fetch("http://localhost:8080/udf_proxy/get_metadata", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: body,
    })
    .then((response) => response.json())
    .then((data) => {
        console.log("Selected project value:", projectSelectValue);
        console.log("Data:", data["data"][0][1]);
        const todos = data["data"][0][1]['todos'];
        console.log("Todos:", todos);
        // const tableBody = document.getElementById("todo-table").getElementsByTagName("tbody");
        const tableBody = document.querySelector(".todo-table tbody");

        if (!todos || todos.length === 0) {
            console.log("No todos found");
            const row = document.createElement("tr");
            const cell = document.createElement("td");
            cell.textContent = "No todos found";
            row.appendChild(cell);
            tableBody.appendChild(row);
        }
        // Clear existing table rows
        tableBody.innerHTML = "";

        // Populate table with new data
        todos.forEach((todo) => {
            const row = document.createElement("tr");

            const todoDeleteCell = document.createElement("td");
            todoDeleteCell.classList.add("delete-todo");
            todoDeleteCell.textContent = "🗑️";
            todoDeleteCell.onclick = function() {
                alert('click');
            };
            row.appendChild(todoDeleteCell);

            const todoIdCell = document.createElement("td");
            todoIdCell.textContent = todo.todo_id;
            row.appendChild(todoIdCell);

            const todoNameCell = document.createElement("td");
            todoNameCell.textContent = todo.todo_name;
            row.appendChild(todoNameCell);

            const todoStatusCell = document.createElement("td");
            todoStatusCell.textContent = todo.current_status;
            row.appendChild(todoStatusCell);

            const whatToDoCell = document.createElement("td");
            whatToDoCell.textContent = todo.what_to_do;
            row.appendChild(whatToDoCell);

            tableBody.appendChild(row);
        });

        // Display the table wrapper
        spinner.classList.remove("visible");
        tableWrapper.classList.add("visible");
    })
    .catch((error) => {
        spinner.classList.remove("visible");
        console.error("Error fetching todos:", error);
    });
}


function populateProjectDD(botId){
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
            populateTodos(selectElement.value);

        }
    })
    .catch((error) => {
        console.error("Error filtering projects:", error);
    });
}

