import os
from typing import Dict
from dotenv import load_dotenv
from databricks.sql import connect, exc as db_exc # Import Databricks SQL exceptions
from databricks.sql.client import Connection
from mcp.server.fastmcp import FastMCP
import requests
from requests import exceptions as req_exc # Import requests exceptions
import logging

# Load environment variables
load_dotenv()

# Get Databricks credentials from environment variables
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

# Set up the MCP server
mcp = FastMCP("Databricks API Explorer")

# Global variable to hold the Databricks SQL connection
_db_connection: Connection | None = None

# Helper function to get a reusable Databricks SQL connection
def get_databricks_connection() -> Connection:
    """Create and return a reusable Databricks SQL connection."""
    global _db_connection
    
    # Check if connection exists and is open
    if _db_connection is not None:
        try:
            # A simple way to check if the connection is still valid
            # This might depend on the driver's implementation; adjust if needed
            cursor = _db_connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            logging.info("Reusing existing Databricks SQL connection.")
            return _db_connection
        except db_exc.Error as db_error: # Catch specific DBAPI errors for liveness check
            logging.warning(f"Existing connection seems invalid (DB Error: {db_error}), creating a new one.")
            try:
                if _db_connection: # Ensure _db_connection is not None before closing
                    _db_connection.close()
            except db_exc.Error: # Ignore DB errors during close if connection was already broken
                pass 
            except Exception as close_exc: # Catch other potential close errors
                 logging.error(f"Unexpected error closing potentially broken DB connection: {close_exc}")
            _db_connection = None # Ensure we create a new one
        except Exception as e: # Catch other unexpected errors during liveness check
            logging.warning(f"Unexpected error checking connection liveness ({e}), creating a new one.")
            _db_connection = None

    if not all([DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH]):
        # This case is critical configuration error, raising is appropriate
        raise ValueError("Missing required Databricks connection details in .env file")

    try:
        logging.info("Creating new Databricks SQL connection.")
        _db_connection = connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        )
        return _db_connection
    except db_exc.Error as db_connect_error: # Catch DB connection errors
        logging.error(f"Failed to connect to Databricks SQL Warehouse: {db_connect_error}")
        raise # Re-raise after logging, as connection is essential
    except Exception as connect_exc:
        logging.error(f"Unexpected error creating Databricks SQL connection: {connect_exc}")
        raise # Re-raise unexpected connection errors

# Helper function for Databricks REST API requests
def databricks_api_request(endpoint: str, method: str = "GET", data: Dict = None) -> Dict:
    """Make a request to the Databricks REST API, handling common errors."""
    if not all([DATABRICKS_HOST, DATABRICKS_TOKEN]):
        # This case is critical configuration error, raising is appropriate
        raise ValueError("Missing required Databricks API credentials in .env file")
    
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    url = f"https://{DATABRICKS_HOST}/api/2.0/{endpoint}"
    
    try:
        if method.upper() == "GET":
            response = requests.get(url, headers=headers)
        elif method.upper() == "POST":
            response = requests.post(url, headers=headers, json=data)
        else:
            # Internal programming error
            raise ValueError(f"Unsupported HTTP method: {method}")
        
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        return response.json()
    
    except req_exc.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err} - Response: {http_err.response.text}")
        # Re-raise as the caller (MCP tool) should handle it
        raise
    except req_exc.ConnectionError as conn_err:
        logging.error(f"Connection error occurred: {conn_err}")
        raise
    except req_exc.Timeout as timeout_err:
        logging.error(f"Request timed out: {timeout_err}")
        raise
    except req_exc.RequestException as req_err:
        # Catch any other requests-related errors
        logging.error(f"An error occurred during the API request: {req_err}")
        raise
    except ValueError as val_err: # Catch the ValueError from unsupported method
        logging.error(f"API Request internal error: {val_err}")
        raise
    # No fallback 'except Exception' here - let unexpected errors propagate if necessary
    # or add one if you want to catch absolutely everything originating *within* this function

@mcp.resource("schema://tables")
def get_schema() -> str:
    """Provide the list of tables in the Databricks SQL warehouse as a resource"""
    try:
        conn = get_databricks_connection() # Use the shared connection
        cursor = conn.cursor()
        tables = cursor.tables().fetchall()
        
        table_info = []
        for table in tables:
            table_info.append(f"Database: {table.TABLE_CAT}, Schema: {table.TABLE_SCHEM}, Table: {table.TABLE_NAME}")
        
        cursor.close()
        return "\n".join(table_info)
    except ValueError as e: # Catch connection config errors
        return f"Configuration Error: {str(e)}"
    except db_exc.Error as db_error: # Catch errors during DB operations
        logging.error(f"Databricks SQL Error in get_schema: {db_error}")
        return f"Database Error: {str(db_error)}"
    except Exception as e: # Fallback for unexpected errors
        logging.error(f"Unexpected Error in get_schema: {e}", exc_info=True)
        return f"An unexpected error occurred: {str(e)}"

@mcp.tool()
def run_sql_query(sql: str) -> str:
    """Execute SQL queries on Databricks SQL warehouse"""
    try:
        conn = get_databricks_connection() # Use the shared connection
        cursor = conn.cursor()
        result = cursor.execute(sql)
        
        if result.description:
            columns = [col[0] for col in result.description]
            rows = result.fetchall()
            cursor.close()
            
            if not rows:
                return "Query executed successfully. No results returned."
            
            # Format as markdown table (potential errors here unlikely but possible)
            try:
                table = "| " + " | ".join(columns) + " |\n"
                table += "| " + " | ".join(["---" for _ in columns]) + " |\n"
                for row in rows:
                    table += "| " + " | ".join([str(cell) for cell in row]) + " |\n"
                return table
            except Exception as format_exc:
                 logging.error(f"Error formatting SQL results: {format_exc}", exc_info=True)
                 return f"Error formatting results: {str(format_exc)}"
        else:
            cursor.close()
            return "Query executed successfully. No results returned."
    except ValueError as e: # Catch connection config errors
        return f"Configuration Error: {str(e)}"
    except db_exc.Error as db_error: # Catch errors during DB operations (connection, syntax, permissions)
        logging.error(f"Databricks SQL Error in run_sql_query: {db_error}")
        # Provide a slightly more user-friendly message for common SQL issues
        return f"SQL Execution Error: {str(db_error)}"
    except Exception as e: # Fallback for unexpected errors
        logging.error(f"Unexpected Error in run_sql_query: {e}", exc_info=True)
        return f"An unexpected error occurred: {str(e)}"

@mcp.tool()
def list_jobs() -> str:
    """List all Databricks jobs"""
    try:
        response = databricks_api_request("jobs/list")
        
        # Error handling for unexpected API response structure
        if not isinstance(response, dict):
             logging.error(f"Unexpected API response type for list_jobs: {type(response)}")
             return "Error: Received unexpected API response format."

        jobs = response.get("jobs") # Use .get() for safer access
        if jobs is None:
            # Distinguish between no jobs and error retrieving jobs
            if "error_code" in response:
                 logging.error(f"API error in list_jobs response: {response}")
                 return f"API Error: {response.get('message', 'Unknown error')}"
            else:
                 return "No jobs found."
        if not isinstance(jobs, list):
             logging.error(f"Unexpected 'jobs' format in list_jobs response: {type(jobs)}")
             return "Error: Received unexpected API response format for jobs list."

        # Format as markdown table
        table_rows = ["| Job ID | Job Name | Created By |", "| ------ | -------- | ---------- |"]
        for job in jobs:
            if not isinstance(job, dict):
                 logging.warning(f"Skipping invalid job item in list_jobs: {job}")
                 continue # Skip malformed job entries
            job_id = job.get("job_id", "N/A")
            job_name = job.get("settings", {}).get("name", "N/A") # Nested get
            creator = job.get("creator_user_name", "N/A") # Corrected field name based on API docs? Check this.
            
            table_rows.append(f"| {job_id} | {job_name} | {creator} |")
        
        return "\n".join(table_rows)
    # Catch specific exceptions raised by databricks_api_request
    except ValueError as e: # Catch config or internal errors
        return f"Configuration/Internal Error: {str(e)}"
    except req_exc.HTTPError as http_err:
         return f"API Request Failed: {http_err.response.status_code} {http_err.response.reason}"
    except req_exc.RequestException as req_err:
        return f"API Connection/Request Error: {str(req_err)}"
    except (KeyError, TypeError, AttributeError) as data_err:
        # Catch errors processing the response data
        logging.error(f"Error processing API response data in list_jobs: {data_err}", exc_info=True)
        return f"Error processing API response: {str(data_err)}"
    except Exception as e: # Fallback
        logging.error(f"Unexpected Error in list_jobs: {e}", exc_info=True)
        return f"An unexpected error occurred: {str(e)}"

@mcp.tool()
def get_job_status(job_id: int) -> str:
    """Get the status of a specific Databricks job"""
    try:
        # Add basic input validation
        if not isinstance(job_id, int) or job_id <= 0:
            return "Error: Invalid Job ID provided."
            
        response = databricks_api_request("jobs/runs/list", data={"job_id": job_id}) # Can raise exceptions
        
        # Error handling for unexpected API response structure
        if not isinstance(response, dict):
             logging.error(f"Unexpected API response type for get_job_status: {type(response)}")
             return "Error: Received unexpected API response format."
             
        runs = response.get("runs")
        if runs is None:
             if "error_code" in response:
                 logging.error(f"API error in get_job_status response: {response}")
                 return f"API Error: {response.get('message', 'Unknown error')}"
             else:
                return f"No runs found for job ID {job_id}."
        if not isinstance(runs, list):
             logging.error(f"Unexpected 'runs' format in get_job_status response: {type(runs)}")
             return "Error: Received unexpected API response format for runs list."

        if not runs: # Check if list is empty after verifying it's a list
            return f"No runs found for job ID {job_id}."
            
        # Format as markdown table
        import datetime # Import moved inside for scope
        table_rows = ["| Run ID | State | Start Time | End Time | Duration |", "| ------ | ----- | ---------- | -------- | -------- |"]
        
        for run in runs:
            if not isinstance(run, dict):
                logging.warning(f"Skipping invalid run item in get_job_status: {run}")
                continue
            
            run_id = run.get("run_id", "N/A")
            # Safer access to nested state
            state_info = run.get("state", {})
            state = state_info.get("life_cycle_state", "N/A") # life_cycle_state is often more useful
            result_state = state_info.get("result_state")
            if result_state:
                state += f" ({result_state})"
            
            start_time = run.get("start_time")
            end_time = run.get("end_time")
            duration_ms = run.get("execution_duration") # Use execution_duration if available

            duration = "N/A"
            if duration_ms is not None and duration_ms > 0:
                duration = f"{duration_ms / 1000:.2f}s"
            elif start_time and end_time:
                # Fallback calculation if execution_duration missing
                 duration = f"{(end_time - start_time) / 1000:.2f}s"

            start_time_str = datetime.datetime.fromtimestamp(start_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if start_time else "N/A"
            end_time_str = datetime.datetime.fromtimestamp(end_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if end_time else "N/A"
            
            table_rows.append(f"| {run_id} | {state} | {start_time_str} | {end_time_str} | {duration} |")
        
        return "\n".join(table_rows)
    # Catch specific exceptions raised by databricks_api_request
    except ValueError as e: # Catch config or internal errors (incl. invalid job_id)
        return f"Configuration/Input Error: {str(e)}"
    except req_exc.HTTPError as http_err:
         return f"API Request Failed: {http_err.response.status_code} {http_err.response.reason}"
    except req_exc.RequestException as req_err:
        return f"API Connection/Request Error: {str(req_err)}"
    except (KeyError, TypeError, AttributeError) as data_err:
        # Catch errors processing the response data
        logging.error(f"Error processing API response data in get_job_status: {data_err}", exc_info=True)
        return f"Error processing API response: {str(data_err)}"
    except Exception as e: # Fallback
        logging.error(f"Unexpected Error in get_job_status: {e}", exc_info=True)
        return f"An unexpected error occurred: {str(e)}"

@mcp.tool()
def get_job_details(job_id: int) -> str:
    """Get detailed information about a specific Databricks job"""
    try:
         # Add basic input validation
        if not isinstance(job_id, int) or job_id <= 0:
            return "Error: Invalid Job ID provided."
            
        response = databricks_api_request(f"jobs/get?job_id={job_id}", method="GET") # Can raise exceptions

        # Error handling for unexpected API response structure
        if not isinstance(response, dict):
             logging.error(f"Unexpected API response type for get_job_details: {type(response)}")
             return "Error: Received unexpected API response format."

        # Safer access using .get()
        settings = response.get("settings", {})
        job_name = settings.get("name", "N/A")
        created_time = response.get("created_time")
        creator_user_name = response.get('creator_user_name', 'N/A')
        tasks = settings.get("tasks", [])
        
        # Convert timestamp to readable format
        import datetime # Import moved inside for scope
        created_time_str = datetime.datetime.fromtimestamp(created_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if created_time else "N/A"
        
        result_lines = [f"## Job Details: {job_name}\n"]
        result_lines.append(f"- **Job ID:** {job_id}")
        result_lines.append(f"- **Created:** {created_time_str}")
        result_lines.append(f"- **Creator:** {creator_user_name}\n")
        
        if isinstance(tasks, list) and tasks: # Check tasks is a non-empty list
            result_lines.append("### Tasks:\n")
            result_lines.append("| Task Key | Task Type | Description |")
            result_lines.append("| -------- | --------- | ----------- |")
            
            for task in tasks:
                if not isinstance(task, dict):
                    logging.warning(f"Skipping invalid task item in get_job_details: {task}")
                    continue
                    
                task_key = task.get("task_key", "N/A")
                # Simplified task type extraction (assuming one _task key)
                task_type = next((k.replace('_task', '') for k in task if k.endswith('_task')), "N/A")
                description = task.get("description", "N/A")
                
                result_lines.append(f"| {task_key} | {task_type} | {description} |")
        else:
            result_lines.append("No tasks defined for this job.")
        
        return "\n".join(result_lines)
    # Catch specific exceptions raised by databricks_api_request
    except ValueError as e: # Catch config or internal errors (incl. invalid job_id)
        return f"Configuration/Input Error: {str(e)}"
    except req_exc.HTTPError as http_err:
         # Specifically check for 404 for job not found
         if http_err.response.status_code == 404:
              return f"Error: Job ID {job_id} not found."
         return f"API Request Failed: {http_err.response.status_code} {http_err.response.reason}"
    except req_exc.RequestException as req_err:
        return f"API Connection/Request Error: {str(req_err)}"
    except (KeyError, TypeError, AttributeError) as data_err:
        # Catch errors processing the response data
        logging.error(f"Error processing API response data in get_job_details: {data_err}", exc_info=True)
        return f"Error processing API response: {str(data_err)}"
    except Exception as e: # Fallback
        logging.error(f"Unexpected Error in get_job_details: {e}", exc_info=True)
        return f"An unexpected error occurred: {str(e)}"

if __name__ == "__main__":
    # Setup basic logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    mcp.run()