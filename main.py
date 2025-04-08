import os
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from databricks.sql import connect
from databricks.sql.client import Connection
from mcp.server.fastmcp import FastMCP
import requests
import json
import urllib.parse
import subprocess

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
        response = requests.get(url, headers=headers, params=data)
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
    print(sql)
    conn = get_databricks_connection()
    print("connected")

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
def create_job(
    name: str,
    cluster_id: str = None,
    new_cluster: Dict = None,
    notebook_path: str = None,
    spark_jar_task: Dict = None,
    spark_python_task: Dict = None,
    schedule: Dict = None,
    timeout_seconds: int = None,
    max_concurrent_runs: int = None,
    max_retries: int = None
) -> str:
    """Create a new Databricks job
    
    Args:
        name: The name of the job
        cluster_id: ID of existing cluster to run the job on (specify either cluster_id or new_cluster)
        new_cluster: Specification for a new cluster (specify either cluster_id or new_cluster)
        notebook_path: Path to the notebook to run (specify a task type)
        spark_jar_task: Specification for a JAR task (specify a task type)
        spark_python_task: Specification for a Python task (specify a task type)
        schedule: Job schedule configuration
        timeout_seconds: Timeout in seconds for each run
        max_concurrent_runs: Maximum number of concurrent runs
        max_retries: Maximum number of retries per run
    """
    try:
        # Prepare the job settings
        settings = {
            "name": name
        }
        
        # Set either existing cluster or new cluster
        if cluster_id:
            settings["existing_cluster_id"] = cluster_id
        elif new_cluster:
            settings["new_cluster"] = new_cluster
        else:
            return "Error: Either cluster_id or new_cluster must be specified."
        
        # Set task definition - only one type of task can be set
        task_set = False
        if notebook_path:
            settings["notebook_task"] = {"notebook_path": notebook_path}
            task_set = True
        if spark_jar_task and not task_set:
            settings["spark_jar_task"] = spark_jar_task
            task_set = True
        if spark_python_task and not task_set:
            settings["spark_python_task"] = spark_python_task
            task_set = True
            
        if not task_set:
            return "Error: You must specify a task type (notebook_path, spark_jar_task, or spark_python_task)."
        
        # Add optional parameters
        if schedule:
            settings["schedule"] = schedule
        if timeout_seconds:
            settings["timeout_seconds"] = timeout_seconds
        if max_concurrent_runs:
            settings["max_concurrent_runs"] = max_concurrent_runs
        if max_retries:
            settings["max_retries"] = max_retries
        
        # Make the API request to create the job
        response = databricks_api_request("jobs/create", method="POST", data={"name": name, "settings": settings})
        
        # The response contains the job_id of the newly created job
        job_id = response.get("job_id")
        
        if job_id:
            return f"Job created successfully with job ID: {job_id}\n\nYou can view job details with: get_job_details({job_id})"
        else:
            return "Job creation failed. No job ID returned."
    except Exception as e:
        return f"Error creating job: {str(e)}"

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
def list_job_runs(
    job_id: int = None, 
    active_only: bool = None, 
    completed_only: bool = None,
    limit: int = None,
    run_type: str = None,
    expand_tasks: bool = None,
    start_time_from: int = None,
    start_time_to: int = None,
    page_token: str = None
) -> str:
    """List job runs with optional filtering parameters
    
    Args:
        job_id: Optional job ID to filter runs for a specific job
        active_only: If true, only active runs are included
        completed_only: If true, only completed runs are included
        limit: Number of runs to return (1-25, default 20)
        run_type: Type of runs (JOB_RUN, WORKFLOW_RUN, SUBMIT_RUN)
        expand_tasks: Whether to include task details
        start_time_from: Show runs that started at or after this UTC timestamp in milliseconds
        start_time_to: Show runs that started at or before this UTC timestamp in milliseconds
        page_token: Token for pagination
    """
    try:
        # Build query parameters
        params = {}
        
        if job_id is not None:
            params["job_id"] = job_id
        if active_only is not None:
            params["active_only"] = active_only
        if completed_only is not None:
            params["completed_only"] = completed_only
        if limit is not None:
            params["limit"] = limit
        if run_type is not None:
            params["run_type"] = run_type
        if expand_tasks is not None:
            params["expand_tasks"] = expand_tasks
        if start_time_from is not None:
            params["start_time_from"] = start_time_from
        if start_time_to is not None:
            params["start_time_to"] = start_time_to
        if page_token is not None:
            params["page_token"] = page_token
        
        # Make API request
        response = databricks_api_request("jobs/runs/list", method="GET", data=params)
        
        if not response.get("runs"):
            if job_id:
                return f"No runs found for job ID {job_id}."
            else:
                return "No job runs found."
        
        runs = response.get("runs", [])
        
        # Format as markdown table
        table = "| Run ID | Job ID | State | Creator | Start Time | End Time | Duration |\n"
        table += "| ------ | ------ | ----- | ------- | ---------- | -------- | -------- |\n"
        
        import datetime
        
        for run in runs:
            run_id = run.get("run_id", "N/A")
            run_job_id = run.get("job_id", "N/A")
            run_creator = run.get("creator_user_name", "N/A")
            
            # Get state information
            life_cycle_state = run.get("state", {}).get("life_cycle_state", "N/A")
            result_state = run.get("state", {}).get("result_state", "")
            state = f"{life_cycle_state}" if not result_state else f"{life_cycle_state} ({result_state})"
            
            # Process timestamps
            start_time = run.get("start_time", 0)
            end_time = run.get("end_time", 0)
            
            start_time_str = datetime.datetime.fromtimestamp(start_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if start_time else "N/A"
            end_time_str = datetime.datetime.fromtimestamp(end_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if end_time else "N/A"
            
            # Calculate duration
            if start_time and end_time:
                duration = f"{(end_time - start_time) / 1000:.2f}s"
            else:
                duration = "N/A"
            
            table += f"| {run_id} | {run_job_id} | {state} | {run_creator} | {start_time_str} | {end_time_str} | {duration} |\n"
        
        # Add pagination information
        result = table
        
        next_page = response.get("next_page_token")
        prev_page = response.get("prev_page_token")
        
        if next_page or prev_page:
            result += "\n### Pagination\n"
            if next_page:
                result += f"More results available. Use page_token='{next_page}' to view the next page.\n"
            if prev_page:
                result += f"Use page_token='{prev_page}' to view the previous page.\n"
        
        return result
        
    except Exception as e:
        return f"Error listing job runs: {str(e)}"

# Clusters API tools
@mcp.tool()
def list_clusters() -> str:
    """List all Databricks clusters"""
    try:
        response = databricks_api_request("clusters/list")
        
        if not response.get("clusters"):
            return "No clusters found."
        
        clusters = response.get("clusters", [])
        
        # Format as markdown table
        table = "| Cluster ID | Cluster Name | State | Autoscale | Workers |\n"
        table += "| ---------- | ------------ | ----- | --------- | ------- |\n"
        
        for cluster in clusters:
            cluster_id = cluster.get("cluster_id", "N/A")
            cluster_name = cluster.get("cluster_name", "N/A")
            state = cluster.get("state", "N/A")
            
            autoscale = "Yes" if cluster.get("autoscale", {}).get("min_workers") else "No"
            workers = cluster.get("num_workers", "N/A")
            
            table += f"| {cluster_id} | {cluster_name} | {state} | {autoscale} | {workers} |\n"
        
        return table
    except Exception as e:
        return f"Error listing clusters: {str(e)}"

@mcp.tool()
def get_cluster_details(cluster_id: str) -> str:
    """Get detailed information about a specific Databricks cluster"""
    try:
        response = databricks_api_request(f"clusters/get?cluster_id={cluster_id}")
        
        result = f"## Cluster Details\n\n"
        result += f"- **Cluster ID:** {cluster_id}\n"
        result += f"- **Cluster Name:** {response.get('cluster_name', 'N/A')}\n"
        result += f"- **State:** {response.get('state', 'N/A')}\n"
        result += f"- **Spark Version:** {response.get('spark_version', 'N/A')}\n"
        
        # Add node type info
        node_type_id = response.get('node_type_id', 'N/A')
        result += f"- **Node Type ID:** {node_type_id}\n"
        
        # Add autoscaling info if applicable
        if 'autoscale' in response:
            min_workers = response['autoscale'].get('min_workers', 'N/A')
            max_workers = response['autoscale'].get('max_workers', 'N/A')
            result += f"- **Autoscale:** {min_workers} to {max_workers} workers\n"
        else:
            result += f"- **Num Workers:** {response.get('num_workers', 'N/A')}\n"
        
        return result
    except Exception as e:
        return f"Error getting cluster details: {str(e)}"

# Instance Pools API tools
@mcp.tool()
def list_instance_pools() -> str:
    """List all instance pools in the workspace"""
    try:
        response = databricks_api_request("instance-pools/list")
        
        if not response.get("instance_pools"):
            return "No instance pools found."
        
        pools = response.get("instance_pools", [])
        
        # Format as markdown table
        table = "| Pool ID | Pool Name | Instance Type | Min Idle | Max Capacity | State |\n"
        table += "| ------- | --------- | ------------- | -------- | ------------ | ----- |\n"
        
        for pool in pools:
            pool_id = pool.get("instance_pool_id", "N/A")
            pool_name = pool.get("instance_pool_name", "N/A")
            instance_type = pool.get("node_type_id", "N/A")
            min_idle = pool.get("min_idle_instances", 0)
            max_capacity = pool.get("max_capacity", 0)
            state = pool.get("state", "N/A")
            
            table += f"| {pool_id} | {pool_name} | {instance_type} | {min_idle} | {max_capacity} | {state} |\n"
        
        return table
    except Exception as e:
        return f"Error listing instance pools: {str(e)}"

@mcp.tool()
def create_instance_pool(
    name: str, 
    node_type_id: str, 
    min_idle_instances: int = 0, 
    max_capacity: int = 10,
    idle_instance_autotermination_minutes: int = 60,
    preload_spark_versions: List[str] = None
) -> str:
    """Create a new instance pool
    
    Args:
        name: The name of the instance pool
        node_type_id: The node type for the instances in the pool (e.g., "Standard_DS3_v2")
        min_idle_instances: Minimum number of idle instances to keep in the pool
        max_capacity: Maximum number of instances the pool can contain
        idle_instance_autotermination_minutes: Number of minutes that idle instances are maintained
        preload_spark_versions: Spark versions to preload on the instances (optional)
    """
    try:
        # Build the request data
        data = {
            "instance_pool_name": name,
            "node_type_id": node_type_id,
            "min_idle_instances": min_idle_instances,
            "max_capacity": max_capacity,
            "idle_instance_autotermination_minutes": idle_instance_autotermination_minutes
        }
        
        # Add preloaded Spark versions if specified
        if preload_spark_versions:
            data["preloaded_spark_versions"] = preload_spark_versions
        
        # Make the API request
        response = databricks_api_request("instance-pools/create", method="POST", data=data)
        
        # The response contains the instance_pool_id of the newly created pool
        pool_id = response.get("instance_pool_id")
        
        if pool_id:
            return f"Instance pool created successfully with ID: {pool_id}"
        else:
            return "Instance pool creation failed. No pool ID returned."
    except Exception as e:
        return f"Error creating instance pool: {str(e)}"

# DBFS API tools
@mcp.tool()
def list_dbfs_files(path: str = "/") -> str:
    """List files in a DBFS directory"""
    try:
        if not all([DATABRICKS_HOST, DATABRICKS_TOKEN]):
            raise ValueError("Missing required Databricks API credentials in .env file")
        
        # Direct API call to match the exact curl command that works
        import subprocess
        import json
        
        # Execute the curl command that we know works
        cmd = [
            "curl", "-s",
            "-H", f"Authorization: Bearer {DATABRICKS_TOKEN}",
            "-H", "Content-Type: application/json",
            f"https://{DATABRICKS_HOST}/api/2.0/dbfs/list?path={path}"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise ValueError(f"Command failed with exit code {result.returncode}: {result.stderr}")
        
        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError:
            raise ValueError(f"Failed to parse response as JSON: {result.stdout}")
        
        if not data.get("files"):
            return f"No files found in {path}."
        
        files = data.get("files", [])
        
        # Format as markdown table
        table = "| File Path | Size | Is Directory |\n"
        table += "| --------- | ---- | ------------ |\n"
        
        for file in files:
            file_path = file.get("path", "N/A")
            size = file.get("file_size", 0)
            is_dir = "Yes" if file.get("is_dir", False) else "No"
            
            table += f"| {file_path} | {size} | {is_dir} |\n"
        
        return table
    except Exception as e:
        print(f"Error details: {str(e)}")
        return f"Error listing DBFS files: {str(e)}"

@mcp.tool()
def read_dbfs_file(path: str, length: int = 1000) -> str:
    """Read the contents of a file from DBFS (limited to 1MB)"""
    try:
        if not all([DATABRICKS_HOST, DATABRICKS_TOKEN]):
            raise ValueError("Missing required Databricks API credentials in .env file")
        
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json"
        }
        
        # Build URL with query parameters directly
        encoded_path = urllib.parse.quote(path)
        url = f"https://{DATABRICKS_HOST}/api/2.0/dbfs/read?path={encoded_path}&length={length}"
        
        print(f"Requesting URL: {url}")
        response = requests.get(url, headers=headers)
        print(f"Status code: {response.status_code}")
        print(f"Response: {response.text[:100]}...")  # Print just the beginning to avoid flooding logs
        
        response.raise_for_status()
        data = response.json()
        
        if "data" not in data:
            return f"No data found in file {path}."
        
        import base64
        file_content = base64.b64decode(data["data"]).decode("utf-8")
        
        return f"## File Contents: {path}\n\n```\n{file_content}\n```"
    except Exception as e:
        print(f"Error details: {str(e)}")
        return f"Error reading DBFS file: {str(e)}"

@mcp.tool()
def upload_to_dbfs(file_content: str, dbfs_path: str, overwrite: bool = False) -> str:
    """Upload content to a file in DBFS
    
    Args:
        file_content: The text content to upload to DBFS
        dbfs_path: The destination path in DBFS
        overwrite: Whether to overwrite an existing file
    """
    try:
        if not all([DATABRICKS_HOST, DATABRICKS_TOKEN]):
            raise ValueError("Missing required Databricks API credentials in .env file")
        
        import base64
        
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json"
        }
        
        # DBFS file upload requires three steps:
        # 1. Create a handle
        create_url = f"https://{DATABRICKS_HOST}/api/2.0/dbfs/create"
        create_data = {
            "path": dbfs_path,
            "overwrite": overwrite
        }
        
        create_response = requests.post(create_url, headers=headers, json=create_data)
        create_response.raise_for_status()
        
        handle = create_response.json().get("handle")
        if not handle:
            return f"Error: Failed to create a handle for the file upload."
        
        # 2. Add blocks of data
        # Convert string content to bytes and then encode as base64
        content_bytes = file_content.encode('utf-8')
        encoded_content = base64.b64encode(content_bytes).decode('utf-8')
        
        add_block_url = f"https://{DATABRICKS_HOST}/api/2.0/dbfs/add-block"
        add_block_data = {
            "handle": handle,
            "data": encoded_content
        }
        
        add_block_response = requests.post(add_block_url, headers=headers, json=add_block_data)
        add_block_response.raise_for_status()
        
        # 3. Close the handle
        close_url = f"https://{DATABRICKS_HOST}/api/2.0/dbfs/close"
        close_data = {
            "handle": handle
        }
        
        close_response = requests.post(close_url, headers=headers, json=close_data)
        close_response.raise_for_status()
        
        return f"Successfully uploaded content to {dbfs_path}.\n\nYou can view the file with: read_dbfs_file('{dbfs_path}')"
    except Exception as e:
        print(f"Error details: {str(e)}")
        return f"Error uploading to DBFS: {str(e)}"

# Workspace API tools
@mcp.tool()
def list_workspace(path: str = "/") -> str:
    """List notebooks and directories in a workspace"""
    try:
        if not all([DATABRICKS_HOST, DATABRICKS_TOKEN]):
            raise ValueError("Missing required Databricks API credentials in .env file")
        
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json"
        }
        
        # Build URL with query parameters directly
        encoded_path = urllib.parse.quote(path)
        url = f"https://{DATABRICKS_HOST}/api/2.0/workspace/list?path={encoded_path}"
        
        print(f"Requesting URL: {url}")
        response = requests.get(url, headers=headers)
        print(f"Status code: {response.status_code}")
        print(f"Response: {response.text}")
        
        response.raise_for_status()
        data = response.json()
        
        if not data.get("objects"):
            return f"No objects found in {path}."
        
        objects = data.get("objects", [])
        
        # Format as markdown table
        table = "| Path | Object Type | Language |\n"
        table += "| ---- | ----------- | -------- |\n"
        
        for obj in objects:
            obj_path = obj.get("path", "N/A")
            obj_type = obj.get("object_type", "N/A")
            language = obj.get("language", "N/A") if obj_type == "NOTEBOOK" else "N/A"
            
            table += f"| {obj_path} | {obj_type} | {language} |\n"
        
        return table
    except Exception as e:
        print(f"Error details: {str(e)}")
        return f"Error listing workspace: {str(e)}"

@mcp.tool()
def export_notebook(path: str, format: str = "SOURCE") -> str:
    """Export a notebook from the workspace
    
    Args:
        path: Path to the notebook in the workspace
        format: Export format (SOURCE, HTML, JUPYTER, DBC, R_MARKDOWN)
    """
    try:
        if not all([DATABRICKS_HOST, DATABRICKS_TOKEN]):
            raise ValueError("Missing required Databricks API credentials in .env file")
        
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json"
        }
        
        # Build request
        export_url = f"https://{DATABRICKS_HOST}/api/2.0/workspace/export"
        export_data = {
            "path": path,
            "format": format
        }
        
        response = requests.get(export_url, headers=headers, params=export_data)
        response.raise_for_status()
        data = response.json()
        
        if "content" not in data:
            return f"No content found for notebook at {path}."
        
        import base64
        notebook_content = base64.b64decode(data["content"]).decode("utf-8")
        
        # Determine file extension based on format
        extension = ""
        if format == "SOURCE":
            # Try to determine language
            language_url = f"https://{DATABRICKS_HOST}/api/2.0/workspace/get-status"
            language_response = requests.get(language_url, headers=headers, params={"path": path})
            
            if language_response.status_code == 200:
                language_data = language_response.json()
                language = language_data.get("language", "").lower()
                
                if language == "python":
                    extension = ".py"
                elif language == "scala":
                    extension = ".scala"
                elif language == "r":
                    extension = ".r"
                elif language == "sql":
                    extension = ".sql"
            
        elif format == "HTML":
            extension = ".html"
        elif format == "JUPYTER":
            extension = ".ipynb"
        elif format == "DBC":
            extension = ".dbc"
        elif format == "R_MARKDOWN":
            extension = ".Rmd"
        
        # Return the notebook content
        filename = path.split("/")[-1] + extension
        return f"## Exported Notebook: {filename}\n\n```\n{notebook_content}\n```"
    except Exception as e:
        print(f"Error details: {str(e)}")
        return f"Error exporting notebook: {str(e)}"

@mcp.tool()
def import_notebook(content: str, path: str, language: str = "PYTHON", format: str = "SOURCE", overwrite: bool = False) -> str:
    """Import a notebook into the workspace
    
    Args:
        content: The notebook content
        path: Destination path in the workspace
        language: Notebook language (PYTHON, SCALA, SQL, R)
        format: Import format (SOURCE, HTML, JUPYTER, DBC, R_MARKDOWN)
        overwrite: Whether to overwrite existing notebook
    """
    try:
        if not all([DATABRICKS_HOST, DATABRICKS_TOKEN]):
            raise ValueError("Missing required Databricks API credentials in .env file")
        
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json"
        }
        
        # Encode the content as base64
        import base64
        content_bytes = content.encode("utf-8")
        encoded_content = base64.b64encode(content_bytes).decode("utf-8")
        
        # Build request
        import_url = f"https://{DATABRICKS_HOST}/api/2.0/workspace/import"
        import_data = {
            "path": path,
            "content": encoded_content,
            "language": language,
            "format": format,
            "overwrite": overwrite
        }
        
        response = requests.post(import_url, headers=headers, json=import_data)
        response.raise_for_status()
        
        return f"Notebook successfully imported to {path}"
    except Exception as e:
        print(f"Error details: {str(e)}")
        return f"Error importing notebook: {str(e)}"

# Unity Catalog API tools
@mcp.tool()
def list_catalogs() -> str:
    """List all catalogs in Unity Catalog"""
    try:
        response = databricks_api_request("unity-catalog/catalogs")
        
        if not response.get("catalogs"):
            return "No catalogs found."
        
        catalogs = response.get("catalogs", [])
        
        # Format as markdown table
        table = "| Catalog Name | Comment | Provider | Properties |\n"
        table += "| ------------ | ------- | -------- | ---------- |\n"
        
        for catalog in catalogs:
            name = catalog.get("name", "N/A")
            comment = catalog.get("comment", "")
            provider = catalog.get("provider", {}).get("name", "N/A")
            properties = ", ".join([f"{k}: {v}" for k, v in catalog.get("properties", {}).items()]) if catalog.get("properties") else "None"
            
            table += f"| {name} | {comment} | {provider} | {properties} |\n"
        
        return table
    except Exception as e:
        return f"Error listing catalogs: {str(e)}"

@mcp.tool()
def create_catalog(name: str, comment: str = None, properties: Dict = None) -> str:
    """Create a new catalog in Unity Catalog
    
    Args:
        name: Name of the catalog
        comment: Description of the catalog (optional)
        properties: Key-value properties for the catalog (optional)
    """
    try:
        # Prepare the request data
        data = {
            "name": name
        }
        
        if comment:
            data["comment"] = comment
            
        if properties:
            data["properties"] = properties
        
        # Make the API request
        response = databricks_api_request("unity-catalog/catalogs", method="POST", data=data)
        
        # Check if the catalog was created successfully
        if response.get("name") == name:
            return f"Catalog '{name}' was created successfully."
        else:
            return f"Catalog creation might have failed. Please check with list_catalogs()."
    except Exception as e:
        return f"Error creating catalog: {str(e)}"

@mcp.tool()
def create_schema(catalog_name: str, schema_name: str, comment: str = None, properties: Dict = None) -> str:
    """Create a new schema in Unity Catalog
    
    Args:
        catalog_name: Name of the parent catalog
        schema_name: Name of the schema
        comment: Description of the schema (optional)
        properties: Key-value properties for the schema (optional)
    """
    try:
        # Prepare the request data
        data = {
            "name": schema_name,
            "catalog_name": catalog_name
        }
        
        if comment:
            data["comment"] = comment
            
        if properties:
            data["properties"] = properties
        
        # Make the API request
        response = databricks_api_request("unity-catalog/schemas", method="POST", data=data)
        
        # Check if the schema was created successfully
        if response.get("name") == schema_name and response.get("catalog_name") == catalog_name:
            return f"Schema '{catalog_name}.{schema_name}' was created successfully."
        else:
            return f"Schema creation might have failed. Please check using another Unity Catalog API call."
    except Exception as e:
        return f"Error creating schema: {str(e)}"

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True) 

if __name__ == "__main__":
    #run_sql_query("SELECT * FROM dev.dev_test.income_survey_dataset LIMIT 10;")
    mcp.run()