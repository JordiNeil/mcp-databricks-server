import os
import datetime
from typing import Optional
from dotenv import load_dotenv
from databricks.sql import connect
from databricks.sql.client import Connection
from databricks.sdk import WorkspaceClient
from mcp.server.fastmcp import FastMCP

# Load environment variables
load_dotenv()

# Get Databricks credentials from environment variables
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

# Set up the MCP server
mcp = FastMCP("Databricks API Explorer")

# Initialize Databricks SDK client
def get_workspace_client() -> WorkspaceClient:
    """Create and return a Databricks WorkspaceClient"""
    if not all([DATABRICKS_HOST, DATABRICKS_TOKEN]):
        raise ValueError("Missing required Databricks credentials in .env file")
    
    return WorkspaceClient(
        host=f"https://{DATABRICKS_HOST}",
        token=DATABRICKS_TOKEN
    )


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
        w = get_workspace_client()
        jobs = list(w.jobs.list())
        
        if not jobs:
            return "No jobs found."
        
        # Format as markdown table
        table = "| Job ID | Job Name | Created By |\n"
        table += "| ------ | -------- | ---------- |\n"
        
        for job in jobs:
            job_id = job.job_id or "N/A"
            job_name = job.settings.name if job.settings else "N/A"
            created_by = job.creator_user_name or "N/A"
            
            table += f"| {job_id} | {job_name} | {created_by} |\n"
        
        return table
    except Exception as e:
        return f"Error listing jobs: {str(e)}"

@mcp.tool()
def get_job_status(job_id: int) -> str:
    """Get the status of a specific Databricks job"""
    try:
        w = get_workspace_client()
        runs = list(w.jobs.list_runs(job_id=job_id))
        
        if not runs:
            return f"No runs found for job ID {job_id}."
        
        # Format as markdown table
        table = "| Run ID | State | Start Time | End Time | Duration |\n"
        table += "| ------ | ----- | ---------- | -------- | -------- |\n"
        
        for run in runs:
            run_id = run.run_id or "N/A"
            state = run.state.result_state.value if run.state and run.state.result_state else "N/A"
            
            # Convert timestamps to readable format if they exist
            start_time = run.start_time or 0
            end_time = run.end_time or 0
            
            if start_time and end_time:
                duration = f"{(end_time - start_time) / 1000:.2f}s"
            else:
                duration = "N/A"
            
            # Format timestamps
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
        w = get_workspace_client()
        job = w.jobs.get(job_id=job_id)
        
        # Format the job details
        job_name = job.settings.name if job.settings else "N/A"
        created_time = job.created_time or 0
        
        # Convert timestamp to readable format
        created_time_str = datetime.datetime.fromtimestamp(created_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if created_time else "N/A"
        
        # Get job tasks
        tasks = job.settings.tasks if job.settings and job.settings.tasks else []
        
        result = f"## Job Details: {job_name}\n\n"
        result += f"- **Job ID:** {job_id}\n"
        result += f"- **Created:** {created_time_str}\n"
        result += f"- **Creator:** {job.creator_user_name or 'N/A'}\n\n"
        
        if tasks:
            result += "### Tasks:\n\n"
            result += "| Task Key | Task Type | Description |\n"
            result += "| -------- | --------- | ----------- |\n"
            
            for task in tasks:
                task_key = task.task_key or "N/A"
                task_type = "N/A"
                if task.notebook_task:
                    task_type = "notebook_task"
                elif task.spark_python_task:
                    task_type = "spark_python_task"
                elif task.spark_jar_task:
                    task_type = "spark_jar_task"
                elif task.python_wheel_task:
                    task_type = "python_wheel_task"
                elif task.sql_task:
                    task_type = "sql_task"
                
                description = task.description or "N/A"
                
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
        w = get_workspace_client()
        objects = list(w.workspace.list(path=path))
        
        if not objects:
            return f"No objects found at path: {path}"
        
        table = "| Path | Type |\n| ---- | ---- |\n"
        for obj in objects:
            obj_path = obj.path or "N/A"
            obj_type = obj.object_type.value if obj.object_type else "N/A"
            table += f"| {obj_path} | {obj_type} |\n"
        
        return table
    except Exception as e:
        return f"Error listing workspace objects: {str(e)}"

@mcp.tool()
def list_pipelines() -> str:
    """List Delta Live Tables pipelines"""
    try:
        w = get_workspace_client()
        pipelines = list(w.pipelines.list_pipelines())
        
        if not pipelines:
            return "No DLT pipelines found."
        
        table = "| Pipeline ID | Name | State |\n| ------------ | ---- | ------ |\n"
        for p in pipelines:
            pipeline_id = p.pipeline_id or "N/A"
            name = p.name or "N/A"
            state = p.state.value if p.state else "N/A"
            table += f"| {pipeline_id} | {name} | {state} |\n"
        
        return table
    except Exception as e:
        return f"Error listing DLT pipelines: {str(e)}"

@mcp.tool()
def list_clusters() -> str:
    """List all Databricks clusters"""
    try:
        w = get_workspace_client()
        clusters = list(w.clusters.list())
        
        if not clusters:
            return "No clusters found."
        
        table = "| Cluster ID | Cluster Name | State | Spark Version |\n"
        table += "| ---------- | ------------ | ----- | ------------- |\n"
        
        for cluster in clusters:
            cluster_id = cluster.cluster_id or "N/A"
            cluster_name = cluster.cluster_name or "N/A"
            state = cluster.state.value if cluster.state else "N/A"
            spark_version = cluster.spark_version or "N/A"
            
            table += f"| {cluster_id} | {cluster_name} | {state} | {spark_version} |\n"
        
        return table
    except Exception as e:
        return f"Error listing clusters: {str(e)}"

if __name__ == "__main__":
    mcp.run()