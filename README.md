# Databricks MCP Server

A Model Context Protocol (MCP) server that connects to Databricks API, allowing LLMs to run SQL queries, list jobs, and get job status.

## Features

- Run SQL queries on Databricks SQL warehouses
- List all Databricks jobs 
- Get status of specific Databricks jobs
- Get detailed information about Databricks jobs

## Prerequisites

- Python 3.7+
- Databricks workspace with:
  - Personal access token
  - SQL warehouse endpoint
  - Permissions to run queries and access jobs

## Setup

1. Clone this repository
2. Create and activate a virtual environment (recommended):
   ```
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
4. Create a `.env` file in the root directory with the following variables:
   ```
   DATABRICKS_HOST=your-databricks-instance.cloud.databricks.com
   DATABRICKS_TOKEN=your-personal-access-token
   DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
   ```
5. Test your connection (optional but recommended):
   ```
   python test_connection.py
   ```

### Obtaining Databricks Credentials

1. **Host**: Your Databricks instance URL (e.g., `your-instance.cloud.databricks.com`)
2. **Token**: Create a personal access token in Databricks:
   - Go to User Settings (click your username in the top right)
   - Select "Developer" tab
   - Click "Manage" under "Access tokens"
   - Generate a new token, and save it immediately
3. **HTTP Path**: For your SQL warehouse:
   - Go to SQL Warehouses in Databricks
   - Select your warehouse
   - Find the connection details and copy the HTTP Path

## Running the Server

Start the MCP server:
```
python main.py
```

You can test the MCP server using the inspector by running 

```
npx @modelcontextprotocol/inspector python3 main.py
```

## Configuring with Docker (for MCP Clients like Cursor)

If you are integrating this server with an MCP client (like Cursor), you might configure it using Docker. The client will typically manage running the Docker container based on a configuration file (e.g., `mcp.json`).

The configuration passes environment variables directly to the Docker container. Here's an example structure, replacing placeholders with your actual credentials:

```json
{
  "mcpServers": {
    "databricks-docker": {
      "command": "docker",
      "args": [
        "run", 
        "--rm", 
        "-i", 
        "-e",
        "DATABRICKS_HOST=<your-databricks-host>",
        "-e",
        "DATABRICKS_TOKEN=<your-databricks-token>",
        "-e",
        "DATABRICKS_HTTP_PATH=<your-databricks-http-path>",
        "<your-docker-image-name>" 
      ]
    }
    // ... other servers ...
  }
}
```

- Replace `<your-databricks-host>` with your Databricks host (e.g., `dbc-xyz.cloud.databricks.com`).
- Replace `<your-databricks-token>` with your personal access token.
- Replace `<your-databricks-http-path>` with the HTTP path for your SQL warehouse.
- Replace `<your-docker-image-name>` with the name of the Docker image built from the `Dockerfile` (e.g., `jordineil/databricks-mcp`).

This method avoids storing secrets directly in a `.env` file within the project, as the MCP client injects them at runtime.

## Available MCP Tools

The following MCP tools are available:

1. **run_sql_query(sql: str)** - Execute SQL queries on your Databricks SQL warehouse
2. **list_jobs()** - List all Databricks jobs in your workspace
3. **get_job_status(job_id: int)** - Get the status of a specific Databricks job by ID
4. **get_job_details(job_id: int)** - Get detailed information about a specific Databricks job

## Example Usage with LLMs

When used with LLMs that support the MCP protocol, this server enables natural language interaction with your Databricks environment:

- "Show me all tables in the database"
- "Run a query to count records in the customer table"
- "List all my Databricks jobs"
- "Check the status of job #123"
- "Show me details about job #456"

## Troubleshooting

### Connection Issues

- Ensure your Databricks host is correct and doesn't include `https://` prefix
- Check that your SQL warehouse is running and accessible
- Verify your personal access token has the necessary permissions
- Run the included test script: `python test_connection.py`

## Security Considerations

- Your Databricks personal access token provides direct access to your workspace
- Secure your `.env` file and never commit it to version control
- Consider using Databricks token with appropriate permission scopes only
- Run this server in a secure environment
