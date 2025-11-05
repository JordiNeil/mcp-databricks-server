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

## Available MCP Tools

The following MCP tools are available:

1. **run_sql_query(sql: str)** - Execute SQL queries on your Databricks SQL warehouse
2. **list_jobs()** - List all Databricks jobs in your workspace
3. **get_job_status(job_id: int)** - Get the status of a specific Databricks job by ID
4. **get_job_details(job_id: int)** - Get detailed information about a specific Databricks job
5. **preview_table(table_name: str, limit: int = 10)** - Preview rows from a Delta table
6. **search_workspace(path: str = "/")** - List objects in a Databricks workspace path
7. **list_pipelines()** - List Delta Live Tables pipelines

## Example Prompts for New Functions

You can ask your LLM to use these tools in various ways. Here are some example prompts for each available function:

### 1. `run_sql_query(sql: str)`
- "Run the SQL: `SELECT count(*) FROM users`"
- "What is the total revenue in the `sales` table?"
- "Give me the earliest and latest order dates in the orders table."

### 2. `list_jobs()`
- "List all Databricks jobs in my workspace."
- "What jobs are currently available to run?"

### 3. `get_job_status(job_id: int)`
- "Show me the run status for job 9876."
- "Did job 123 finish successfully?"
- "How many times has job 222 failed?"

### 4. `get_job_details(job_id: int)`
- "What tasks are included in job 1001?"
- "Give me the details for job 345."

### 5. `preview_table(table_name: str, limit: int = 10)`
- "Preview the top 5 rows in the `customers` table."
- "Show 10 records from table `events`."
- "What data is in the `users_activity` table?"

### 6. `search_workspace(path: str = "/")`
- "List all folders in the workspace root."
- "What notebooks are in `/Shared/Analytics`?"
- "Show me all files in `/Users/janedoe`."

### 7. `list_pipelines()`
- "List all Delta Live Tables pipelines."
- "Which DLT pipelines are available in my environment?"

### 8. `list_clusters()`
- "List all Databricks clusters."
- "Show me every cluster in my workspace."
- "What clusters are currently available?"

---

Feel free to adapt these prompts to your needs or combine them for more complex workflows!



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
