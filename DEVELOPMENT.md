# Development Environment Setup with Docker Compose and VSCode

This document outlines how to set up and use the Docker-based development environment for this Airflow project.

## Prerequisites

- Docker Desktop installed and running.
- VSCode (Visual Studio Code) installed.
- VSCode "Remote - Containers" extension installed (ID: `ms-vscode-remote.remote-containers`).

## Setup & Launch

1.  **Clone the Repository:**
    ```bash
    git clone <your-repository-url>
    cd <your-repository-name>
    ```

2.  **Open in VSCode:**
    - Open the cloned repository folder in VSCode.
    - VSCode should automatically detect the `.devcontainer/devcontainer.json` configuration and prompt you: "Folder contains a Dev Container configuration file. Reopen in Container?"
    - Click "Reopen in Container".

3.  **First-time Build:**
    - The first time you open the project in the dev container, VSCode will build the Docker images and start the services defined in `docker-compose.yml`. This might take a few minutes.
    - You can follow the progress in the VSCode terminal (usually shows logs from the "Dev Containers" extension).
    - The `airflow-init` service will initialize the Airflow database and create a default admin user (admin/admin).

4.  **Access Airflow UI:**
    - Once the containers are up and running, the Airflow webserver will be accessible at [http://localhost:8080](http://localhost:8080).
    - Log in with username `admin` and password `admin`.

5.  **Access ClickHouse:**
    - The ClickHouse HTTP interface is available at [http://localhost:8123](http://localhost:8123).
    - You can use a ClickHouse client to connect to `localhost:9000` with user `clickhouse_user` and password `clickhouse_password` (as defined in `docker-compose.yml`).

## Development Workflow

-   **DAG Development:**
    -   The `dags/` folder in your local project directory is mounted into the Airflow containers at `/opt/airflow/dags`.
    -   Any changes you make to DAG files locally will be reflected in the Airflow UI automatically (Airflow polls for DAG changes).
-   **Debugging DAGs:**
    -   You can use VSCode's Python debugger. Ensure your VSCode is attached to the `airflow-webserver` (or `airflow-scheduler`) service.
    -   Set breakpoints in your DAG files.
    -   To debug a DAG run, you can trigger it from the Airflow UI. For tasks that run as separate processes, direct debugging with breakpoints in task instances might require more advanced setups (e.g., `debugpy` and attaching to the forked process, or using `LocalExecutor` which runs tasks in the scheduler process).
    -   For `LocalExecutor`, breakpoints in the DAG parsing logic and in `PythonOperator` callables should generally work when the scheduler parses the DAG or executes the task.
-   **VSCode Terminal:**
    -   You can open a terminal in VSCode (`Terminal > New Terminal`). This terminal will be inside the `airflow-webserver` (or `airflow-scheduler`) container.
    -   You can use Airflow CLI commands here (e.g., `airflow dags list`, `airflow tasks test <dag_id> <task_id> <execution_date>`).

## Managing the Environment

-   **Stopping the Environment:**
    -   Close the VSCode window or click the green "><" icon in the bottom-left corner and select "Close Remote Connection".
    -   To stop the Docker Compose services manually, navigate to your project directory in an external terminal and run:
        ```bash
        docker-compose down
        ```
-   **Rebuilding the Container:**
    -   If you change the `Dockerfile` or need a clean environment, open the command palette (`Ctrl+Shift+P` or `Cmd+Shift+P`) and search for "Remote-Containers: Rebuild Container" or "Remote-Containers: Rebuild and Reopen Container".
-   **Viewing Logs:**
    -   You can view logs for all services using Docker Desktop's interface or via the command line:
        ```bash
        docker-compose logs -f # Shows logs for all services
        docker-compose logs -f airflow-webserver # Shows logs for a specific service
        ```

## Troubleshooting

-   **Port Conflicts:** If `8080`, `5432`, `8123`, or `9000` are already in use on your machine, you might need to change the host port mappings in `docker-compose.yml`. For example, change `ports: - "8080:8080"` to `ports: - "8081:8080"`.
-   **Ensure Docker Desktop has enough resources** allocated (CPU, Memory).
-   **Check `airflow-init` logs:** If Airflow isn't starting correctly, the `airflow-init` service logs might show errors related to database initialization or user creation: `docker-compose logs airflow-init`.

This guide should help you get started. Happy DAG developing!
