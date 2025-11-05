import os
from typing import Dict
from dotenv import load_dotenv
from databricks.sql import connect
from databricks.sql.client import Connection
from mcp.server.fastmcp import FastMCP
import requests

# Load environment variables
load_dotenv()

# Get Databricks credentials from environment variables
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

# Set up the MCP server
mcp = FastMCP("Databricks API Explorer")


# Helper function to get a Databricks SQL connection
def get_databricks_connection() -> Connection:
    """Create and return a Databricks SQL connection"""
    if not all([DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH]):
        raise ValueError("Missing required Databricks connection details in .env file")

    return connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )

# Helper function for Databricks REST API requests
def databricks_api_request(endpoint: str, method: str = "GET", data: Dict = None) -> Dict:
    """Make a request to the Databricks REST API"""
    if not all([DATABRICKS_HOST, DATABRICKS_TOKEN]):
        raise ValueError("Missing required Databricks API credentials in .env file")
    
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    url = f"https://{DATABRICKS_HOST}/api/2.0/{endpoint}"
    
    if method.upper() == "GET":
        response = requests.get(url, headers=headers)
    elif method.upper() == "POST":
        response = requests.post(url, headers=headers, json=data)
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")
    
    response.raise_for_status()
    return response.json()

@mcp.resource("schema://tables")
def get_schema() -> str:
    """Provide the list of tables in the Databricks SQL warehouse as a resource"""
    conn = get_databricks_connection()
    try:
        cursor = conn.cursor()
        tables = cursor.tables().fetchall()
        
        table_info = []
        for table in tables:
            table_info.append(f"Database: {table.TABLE_CAT}, Schema: {table.TABLE_SCHEM}, Table: {table.TABLE_NAME}")
        
        return "\n".join(table_info)
    except Exception as e:
        return f"Error retrieving tables: {str(e)}"
    finally:
        if 'conn' in locals():
            conn.close()

@mcp.tool()
def run_sql_query(sql: str) -> str:
    """Execute SQL queries on Databricks SQL warehouse"""
    conn = get_databricks_connection()

    try:
        cursor = conn.cursor()
        result = cursor.execute(sql)
        
        if result.description:
            # Get column names
            columns = [col[0] for col in result.description]
            
            # Format the result as a table
            rows = result.fetchall()
            if not rows:
                return "Query executed successfully. No results returned."
            
            # Format as markdown table
            table = "| " + " | ".join(columns) + " |\n"
            table += "| " + " | ".join(["---" for _ in columns]) + " |\n"
            
            for row in rows:
                table += "| " + " | ".join([str(cell) for cell in row]) + " |\n"
                
            return table
        else:
            return "Query executed successfully. No results returned."
    except Exception as e:
        return f"Error executing query: {str(e)}"
    finally:
        if 'conn' in locals():
            conn.close()

@mcp.tool()
def list_jobs() -> str:
    """List all Databricks jobs"""
    try:
        response = databricks_api_request("jobs/list")
        
        if not response.get("jobs"):
            return "No jobs found."
        
        jobs = response.get("jobs", [])
        
        # Format as markdown table
        table = "| Job ID | Job Name | Created By |\n"
        table += "| ------ | -------- | ---------- |\n"
        
        for job in jobs:
            job_id = job.get("job_id", "N/A")
            job_name = job.get("settings", {}).get("name", "N/A")
            created_by = job.get("created_by", "N/A")
            
            table += f"| {job_id} | {job_name} | {created_by} |\n"
        
        return table
    except Exception as e:
        return f"Error listing jobs: {str(e)}"

@mcp.tool()
def get_job_status(job_id: int) -> str:
    """Get the status of a specific Databricks job"""
    try:
        response = databricks_api_request("jobs/runs/list", data={"job_id": job_id})
        
        if not response.get("runs"):
            return f"No runs found for job ID {job_id}."
        
        runs = response.get("runs", [])
        
        # Format as markdown table
        table = "| Run ID | State | Start Time | End Time | Duration |\n"
        table += "| ------ | ----- | ---------- | -------- | -------- |\n"
        
        for run in runs:
            run_id = run.get("run_id", "N/A")
            state = run.get("state", {}).get("result_state", "N/A")
            
            # Convert timestamps to readable format if they exist
            start_time = run.get("start_time", 0)
            end_time = run.get("end_time", 0)
            
            if start_time and end_time:
                duration = f"{(end_time - start_time) / 1000:.2f}s"
            else:
                duration = "N/A"
            
            # Format timestamps
            import datetime
            start_time_str = datetime.datetime.fromtimestamp(start_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if start_time else "N/A"
            end_time_str = datetime.datetime.fromtimestamp(end_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if end_time else "N/A"
            
            table += f"| {run_id} | {state} | {start_time_str} | {end_time_str} | {duration} |\n"
        
        return table
    except Exception as e:
        return f"Error getting job status: {str(e)}"

@mcp.tool()
def get_job_details(job_id: int) -> str:
    """Get detailed information about a specific Databricks job"""
    try:
        response = databricks_api_request(f"jobs/get?job_id={job_id}", method="GET")
        
        # Format the job details
        job_name = response.get("settings", {}).get("name", "N/A")
        created_time = response.get("created_time", 0)
        
        # Convert timestamp to readable format
        import datetime
        created_time_str = datetime.datetime.fromtimestamp(created_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if created_time else "N/A"
        
        # Get job tasks
        tasks = response.get("settings", {}).get("tasks", [])
        
        result = f"## Job Details: {job_name}\n\n"
        result += f"- **Job ID:** {job_id}\n"
        result += f"- **Created:** {created_time_str}\n"
        result += f"- **Creator:** {response.get('creator_user_name', 'N/A')}\n\n"
        
        if tasks:
            result += "### Tasks:\n\n"
            result += "| Task Key | Task Type | Description |\n"
            result += "| -------- | --------- | ----------- |\n"
            
            for task in tasks:
                task_key = task.get("task_key", "N/A")
                task_type = next(iter([k for k in task.keys() if k.endswith("_task")]), "N/A")
                description = task.get("description", "N/A")
                
                result += f"| {task_key} | {task_type} | {description} |\n"
        
        return result
    except Exception as e:
        return f"Error getting job details: {str(e)}"

@mcp.tool()
def preview_table(table_name: str, limit: int = 10) -> str:
    """Preview rows from a Delta table"""
    sql = f"SELECT * FROM {table_name} LIMIT {limit}"
    return run_sql_query(sql)

@mcp.tool()
def search_workspace(path: str = "/") -> str:
    """List objects in a Databricks workspace path"""
    try:
        response = databricks_api_request(f"workspace/list?path={path}")
        objects = response.get("objects", [])
        if not objects:
            return f"No objects found at path: {path}"
        table = "| Path | Type |\n| ---- | ---- |\n"
        for obj in objects:
            table += f"| {obj.get('path')} | {obj.get('object_type')} |\n"
        return table
    except Exception as e:
        return f"Error listing workspace objects: {str(e)}"

@mcp.tool()
def list_pipelines() -> str:
    """List Delta Live Tables pipelines"""
    try:
        response = databricks_api_request("pipelines")
        pipelines = response.get("statuses", [])
        if not pipelines:
            return "No DLT pipelines found."
        table = "| Pipeline ID | Name | State |\n| ------------ | ---- | ------ |\n"
        for p in pipelines:
            table += f"| {p.get('pipeline_id')} | {p.get('name')} | {p.get('state')} |\n"
        return table
    except Exception as e:
        return f"Error listing DLT pipelines: {str(e)}"

if __name__ == "__main__":
    mcp.run()