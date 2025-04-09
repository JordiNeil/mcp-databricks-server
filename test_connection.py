#!/usr/bin/env python
"""
Databricks Connection Test Script
This script tests the connection to Databricks using credentials from the .env file.
It attempts to connect to both the SQL warehouse and the Databricks API.
"""

import os
from dotenv import load_dotenv
import sys
import datetime
import base64

# Load environment variables
load_dotenv()

# Get Databricks credentials from environment variables
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

def check_env_vars():
    """Check if all required environment variables are set"""
    missing = []
    if not DATABRICKS_HOST:
        missing.append("DATABRICKS_HOST")
    if not DATABRICKS_TOKEN:
        missing.append("DATABRICKS_TOKEN")
    if not DATABRICKS_HTTP_PATH:
        missing.append("DATABRICKS_HTTP_PATH")
    
    if missing:
        print("❌ Missing required environment variables:")
        for var in missing:
            print(f"  - {var}")
        print("\nPlease check your .env file and make sure all required variables are set.")
        return False
    
    print("✅ All required environment variables are set")
    return True

def test_databricks_api():
    """Test connection to Databricks API"""
    import requests
    
    print("\nTesting Databricks API connection...")
    
    try:
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json"
        }
        
        url = f"https://{DATABRICKS_HOST}/api/2.0/clusters/list-node-types"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            print("✅ Successfully connected to Databricks API")
            return True
        else:
            print(f"❌ Failed to connect to Databricks API: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error connecting to Databricks API: {str(e)}")
        return False

def test_clusters_api():
    """Test Clusters API functionality"""
    import requests
    
    print("\nTesting Clusters API...")
    
    try:
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json"
        }
        
        # Test clusters/list endpoint
        url = f"https://{DATABRICKS_HOST}/api/2.0/clusters/list"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            if "clusters" in data:
                print("✅ Successfully accessed Clusters API (list)")
                
                # Also test get_cluster_details if clusters exist
                if data.get("clusters"):
                    cluster_id = data["clusters"][0]["cluster_id"]
                    detail_url = f"https://{DATABRICKS_HOST}/api/2.0/clusters/get?cluster_id={cluster_id}"
                    detail_response = requests.get(detail_url, headers=headers)
                    
                    if detail_response.status_code == 200:
                        print("✅ Successfully accessed Clusters API (details)")
                    else:
                        print(f"❌ Failed to access Clusters API (details): {detail_response.status_code}")
                
                return True
            else:
                print("❌ Response missing expected 'clusters' field")
                return False
        else:
            print(f"❌ Failed to access Clusters API: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error testing Clusters API: {str(e)}")
        return False

def test_instance_pools_api():
    """Test Instance Pools API functionality"""
    import requests
    
    print("\nTesting Instance Pools API...")
    
    try:
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json"
        }
        
        # Test instance-pools/list endpoint
        url = f"https://{DATABRICKS_HOST}/api/2.0/instance-pools/list"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            if "instance_pools" in data:
                print("✅ Successfully accessed Instance Pools API (list)")
                
                # Get available node types to use for creating a pool
                node_types_url = f"https://{DATABRICKS_HOST}/api/2.0/clusters/list-node-types"
                node_types_response = requests.get(node_types_url, headers=headers)
                
                if node_types_response.status_code == 200 and "node_types" in node_types_response.json():
                    node_types = node_types_response.json()["node_types"]
                    if node_types:
                        # Get the first node type ID
                        node_type_id = node_types[0]["node_type_id"]
                        
                        # Test create instance pool
                        pool_name = f"Test Pool {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
                        create_data = {
                            "instance_pool_name": pool_name,
                            "node_type_id": node_type_id,
                            "min_idle_instances": 0,
                            "max_capacity": 2,
                            "idle_instance_autotermination_minutes": 10
                        }
                        
                        create_url = f"https://{DATABRICKS_HOST}/api/2.0/instance-pools/create"
                        create_response = requests.post(create_url, headers=headers, json=create_data)
                        
                        if create_response.status_code == 200 and "instance_pool_id" in create_response.json():
                            print("✅ Successfully accessed Instance Pools API (create)")
                            
                            # Cleanup: Delete the test pool we just created
                            pool_id = create_response.json()["instance_pool_id"]
                            delete_url = f"https://{DATABRICKS_HOST}/api/2.0/instance-pools/delete"
                            delete_data = {"instance_pool_id": pool_id}
                            delete_response = requests.post(delete_url, headers=headers, json=delete_data)
                            
                            if delete_response.status_code == 200:
                                print("✅ Successfully cleaned up test instance pool")
                            else:
                                print(f"⚠️ Could not delete test instance pool: {delete_response.status_code}")
                        else:
                            print(f"❌ Failed to access Instance Pools API (create): {create_response.status_code} - {create_response.text}")
                    else:
                        print("⚠️ No node types available for testing instance pool creation")
                else:
                    print(f"⚠️ Could not get node types for testing: {node_types_response.status_code}")
                
                return True
            else:
                print("❌ Response missing expected 'instance_pools' field")
                return False
        else:
            print(f"❌ Failed to access Instance Pools API: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error testing Instance Pools API: {str(e)}")
        return False

def test_unity_catalog_api():
    """Test Unity Catalog API functionality"""
    import requests
    
    print("\nTesting Unity Catalog API...")
    
    try:
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json"
        }
        
        # Test catalogs list endpoint
        url = f"https://{DATABRICKS_HOST}/api/2.0/unity-catalog/catalogs"
        response = requests.get(url, headers=headers)
        
        # Check if workspace supports Unity Catalog
        if response.status_code == 404 or (response.status_code == 400 and "Unity Catalog is not enabled" in response.text):
            print("⚠️ Unity Catalog is not enabled in this workspace - skipping tests")
            return True  # Skip test but don't mark as failure
        
        if response.status_code == 200:
            # Note: This may return no catalogs, but it should have a "catalogs" key
            data = response.json()
            if "catalogs" in data:
                print("✅ Successfully accessed Unity Catalog API (list catalogs)")
                
                # Test catalog creation (only if user has permission)
                catalog_name = f"test_catalog_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
                create_data = {
                    "name": catalog_name,
                    "comment": "Test catalog for API testing - will be deleted"
                }
                
                create_url = f"https://{DATABRICKS_HOST}/api/2.0/unity-catalog/catalogs"
                create_response = requests.post(create_url, headers=headers, json=create_data)
                
                if create_response.status_code == 200:
                    print("✅ Successfully accessed Unity Catalog API (create catalog)")
                    
                    # Test schema creation in this catalog
                    schema_name = f"test_schema_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
                    schema_data = {
                        "name": schema_name,
                        "catalog_name": catalog_name,
                        "comment": "Test schema for API testing - will be deleted"
                    }
                    
                    schema_url = f"https://{DATABRICKS_HOST}/api/2.0/unity-catalog/schemas"
                    schema_response = requests.post(schema_url, headers=headers, json=schema_data)
                    
                    if schema_response.status_code == 200:
                        print("✅ Successfully accessed Unity Catalog API (create schema)")
                    else:
                        # Not all users can create schemas, so this is not a critical failure
                        print(f"⚠️ Could not create schema (permissions?): {schema_response.status_code}")
                    
                    # Cleanup: Delete the catalog (automatically deletes schemas too)
                    delete_url = f"https://{DATABRICKS_HOST}/api/2.0/unity-catalog/catalogs/{catalog_name}"
                    delete_response = requests.delete(delete_url, headers=headers)
                    
                    if delete_response.status_code in [200, 204]:
                        print("✅ Successfully cleaned up test catalog")
                    else:
                        print(f"⚠️ Could not delete test catalog: {delete_response.status_code}")
                elif create_response.status_code in [403, 401]:
                    # User likely doesn't have permission to create catalogs
                    print("⚠️ Insufficient permissions to create catalogs - skipping create test")
                else:
                    print(f"❌ Failed to access Unity Catalog API (create): {create_response.status_code} - {create_response.text}")
                
                return True
            else:
                print("❌ Response missing expected 'catalogs' field")
                return False
        else:
            print(f"❌ Failed to access Unity Catalog API: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error testing Unity Catalog API: {str(e)}")
        return False

def test_jobs_api():
    """Test Jobs API functionality"""
    import requests
    
    print("\nTesting Jobs API...")
    
    try:
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json"
        }
        
        # Test jobs/list endpoint
        url = f"https://{DATABRICKS_HOST}/api/2.0/jobs/list"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            if "jobs" in data:
                print("✅ Successfully accessed Jobs API (list)")
                
                # Also test job details if jobs exist
                if data.get("jobs"):
                    job_id = data["jobs"][0]["job_id"]
                    
                    # Test get_job_details
                    detail_url = f"https://{DATABRICKS_HOST}/api/2.0/jobs/get?job_id={job_id}"
                    detail_response = requests.get(detail_url, headers=headers)
                    
                    if detail_response.status_code == 200:
                        print("✅ Successfully accessed Jobs API (details)")
                    else:
                        print(f"❌ Failed to access Jobs API (details): {detail_response.status_code}")
                    
                    # Test get_job_status
                    status_url = f"https://{DATABRICKS_HOST}/api/2.0/jobs/runs/list"
                    status_response = requests.get(status_url, headers=headers, params={"job_id": job_id})
                    
                    if status_response.status_code == 200 and "runs" in status_response.json():
                        print("✅ Successfully accessed Jobs API (status)")
                    else:
                        print(f"❌ Failed to access Jobs API (status): {status_response.status_code}")
                    
                    # Test list_job_runs
                    runs_url = f"https://{DATABRICKS_HOST}/api/2.2/jobs/runs/list"
                    runs_response = requests.get(runs_url, headers=headers)
                    
                    if runs_response.status_code == 200 and "runs" in runs_response.json():
                        print("✅ Successfully accessed Jobs API (runs list)")
                    else:
                        print(f"❌ Failed to access Jobs API (runs list): {runs_response.status_code}")
                
                # Test job creation API
                # Find an available cluster to use
                cluster_url = f"https://{DATABRICKS_HOST}/api/2.0/clusters/list"
                cluster_response = requests.get(cluster_url, headers=headers)
                
                if cluster_response.status_code == 200 and cluster_response.json().get("clusters"):
                    # Find the first RUNNING cluster to use
                    clusters = cluster_response.json()["clusters"]
                    running_clusters = [c for c in clusters if c.get("state") == "RUNNING"]
                    
                    if running_clusters:
                        test_cluster_id = running_clusters[0]["cluster_id"]
                        
                        # Create a test job
                        create_url = f"https://{DATABRICKS_HOST}/api/2.0/jobs/create"
                        job_name = f"Test Job {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
                        
                        job_data = {
                            "name": job_name,
                            "settings": {
                                "name": job_name,
                                "existing_cluster_id": test_cluster_id,
                                "notebook_task": {
                                    "notebook_path": "/Shared/test-notebook" if os.path.exists("/Shared/test-notebook") else "/Users/test-notebook"
                                },
                                "max_retries": 0,
                                "timeout_seconds": 300
                            }
                        }
                        
                        create_response = requests.post(create_url, headers=headers, json=job_data)
                        
                        if create_response.status_code == 200 and "job_id" in create_response.json():
                            print("✅ Successfully accessed Jobs API (create)")
                        else:
                            print(f"❌ Failed to access Jobs API (create): {create_response.status_code} - {create_response.text}")
                    else:
                        print("⚠️ Skipping job creation test: No running clusters found")
                else:
                    print("⚠️ Skipping job creation test: Couldn't list clusters")
                
                return True
            else:
                print("❌ Response missing expected 'jobs' field")
                return False
        else:
            print(f"❌ Failed to access Jobs API: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error testing Jobs API: {str(e)}")
        return False

def test_dbfs_api():
    """Test DBFS API functionality"""
    import requests
    import json
    import subprocess
    
    print("\nTesting DBFS API...")
    
    try:
        # Method 1: Direct API call with query parameters (matches our implementation)
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json"
        }
        
        # The DBFS list endpoint requires a path parameter
        url = f"https://{DATABRICKS_HOST}/api/2.0/dbfs/list?path=/"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            if "files" in data:
                print("✅ Successfully accessed DBFS API (list)")
                
                # Also test read_dbfs_file if README.md exists
                if any(file.get("path") == "/databricks-datasets/README.md" for file in data.get("files", [])):
                    read_url = f"https://{DATABRICKS_HOST}/api/2.0/dbfs/read?path=/databricks-datasets/README.md&length=1000"
                    read_response = requests.get(read_url, headers=headers)
                    
                    if read_response.status_code == 200 and "data" in read_response.json():
                        print("✅ Successfully accessed DBFS API (read)")
                    else:
                        print(f"❌ Failed to access DBFS API (read): {read_response.status_code}")
                
                # Test file upload to DBFS
                test_content = "This is a test file uploaded via the DBFS API."
                test_path = "/FileStore/test-upload.txt"
                
                # Step 1: Create handle
                create_url = f"https://{DATABRICKS_HOST}/api/2.0/dbfs/create"
                create_data = {
                    "path": test_path,
                    "overwrite": True
                }
                
                create_response = requests.post(create_url, headers=headers, json=create_data)
                
                if create_response.status_code == 200 and "handle" in create_response.json():
                    handle = create_response.json()["handle"]
                    
                    # Step 2: Add data block
                    add_block_url = f"https://{DATABRICKS_HOST}/api/2.0/dbfs/add-block"
                    encoded_content = base64.b64encode(test_content.encode('utf-8')).decode('utf-8')
                    
                    add_block_data = {
                        "handle": handle,
                        "data": encoded_content
                    }
                    
                    add_block_response = requests.post(add_block_url, headers=headers, json=add_block_data)
                    
                    # Step 3: Close handle
                    if add_block_response.status_code == 200:
                        close_url = f"https://{DATABRICKS_HOST}/api/2.0/dbfs/close"
                        close_data = {
                            "handle": handle
                        }
                        
                        close_response = requests.post(close_url, headers=headers, json=close_data)
                        
                        if close_response.status_code == 200:
                            print("✅ Successfully accessed DBFS API (upload)")
                            
                            # Verify the file was uploaded by reading it back
                            verify_url = f"https://{DATABRICKS_HOST}/api/2.0/dbfs/read?path={test_path}&length=1000"
                            verify_response = requests.get(verify_url, headers=headers)
                            
                            if (verify_response.status_code == 200 and 
                                "data" in verify_response.json() and 
                                test_content == base64.b64decode(verify_response.json()["data"]).decode('utf-8')):
                                print("✅ Successfully verified uploaded content")
                            else:
                                print(f"❌ Failed to verify uploaded content: {verify_response.status_code}")
                        else:
                            print(f"❌ Failed to close handle: {close_response.status_code}")
                    else:
                        print(f"❌ Failed to add data block: {add_block_response.status_code}")
                else:
                    print(f"❌ Failed to create upload handle: {create_response.status_code}")
                
                return True
            else:
                print("❌ Response missing expected 'files' field")
                return False
        else:
            # Method 2: Try with curl as a fallback (this is our current implementation)
            print(f"Direct requests failed, trying curl: {response.status_code}")
            
            cmd = [
                "curl", "-s",
                "-H", f"Authorization: Bearer {DATABRICKS_TOKEN}",
                "-H", "Content-Type: application/json",
                f"https://{DATABRICKS_HOST}/api/2.0/dbfs/list?path=/"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                try:
                    data = json.loads(result.stdout)
                    if "files" in data:
                        print("✅ Successfully accessed DBFS API using curl")
                        return True
                    else:
                        print("❌ Response missing expected 'files' field (curl)")
                        return False
                except json.JSONDecodeError:
                    print(f"❌ Failed to parse curl response as JSON")
                    return False
            else:
                print(f"❌ Curl command failed: {result.stderr}")
                return False
    except Exception as e:
        print(f"❌ Error testing DBFS API: {str(e)}")
        return False

def test_workspace_api():
    """Test Workspace API functionality"""
    import requests
    import base64
    
    print("\nTesting Workspace API...")
    
    try:
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json"
        }
        
        # The workspace list endpoint requires a path parameter
        url = f"https://{DATABRICKS_HOST}/api/2.0/workspace/list"
        data = {"path": "/"}
        response = requests.get(url, headers=headers, params=data)
        
        if response.status_code == 200:
            data = response.json()
            if "objects" in data:
                print("✅ Successfully accessed Workspace API (list)")
                
                # Test notebook import
                test_notebook_path = "/Users/test-import-notebook"
                test_notebook_content = "# Test Notebook\n\nprint('Hello, Databricks!')"
                
                # Encode content
                encoded_content = base64.b64encode(test_notebook_content.encode('utf-8')).decode('utf-8')
                
                import_url = f"https://{DATABRICKS_HOST}/api/2.0/workspace/import"
                import_data = {
                    "path": test_notebook_path,
                    "content": encoded_content,
                    "language": "PYTHON",
                    "format": "SOURCE",
                    "overwrite": True
                }
                
                import_response = requests.post(import_url, headers=headers, json=import_data)
                
                if import_response.status_code == 200:
                    print("✅ Successfully accessed Workspace API (import)")
                    
                    # Test notebook export
                    export_url = f"https://{DATABRICKS_HOST}/api/2.0/workspace/export"
                    export_data = {
                        "path": test_notebook_path,
                        "format": "SOURCE"
                    }
                    
                    export_response = requests.get(export_url, headers=headers, params=export_data)
                    
                    if export_response.status_code == 200 and "content" in export_response.json():
                        exported_content = base64.b64decode(export_response.json()["content"]).decode('utf-8')
                        
                        if test_notebook_content in exported_content:
                            print("✅ Successfully accessed Workspace API (export)")
                        else:
                            print("❌ Exported content does not match imported content")
                    else:
                        print(f"❌ Failed to access Workspace API (export): {export_response.status_code}")
                else:
                    print(f"❌ Failed to access Workspace API (import): {import_response.status_code}")
                
                return True
            else:
                print("❌ Response missing expected 'objects' field")
                return False
        else:
            print(f"❌ Failed to access Workspace API: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error testing Workspace API: {str(e)}")
        return False

def test_sql_connection():
    """Test connection to Databricks SQL warehouse"""
    print("\nTesting Databricks SQL warehouse connection...")
    
    try:
        from databricks.sql import connect
        
        conn = connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS test")
        result = cursor.fetchall()
        
        if result and result[0][0] == 1:
            print("✅ Successfully connected to Databricks SQL warehouse")
            conn.close()
            return True
        else:
            print("❌ Failed to get expected result from SQL warehouse")
            conn.close()
            return False
    except Exception as e:
        print(f"❌ Error connecting to Databricks SQL warehouse: {str(e)}")
        return False

if __name__ == "__main__":
    print("Databricks Connection Test")
    print("=========================\n")
    
    # Check for dependencies
    try:
        import requests
        from databricks.sql import connect
    except ImportError as e:
        print(f"❌ Missing dependency: {str(e)}")
        print("Please run: pip install -r requirements.txt")
        sys.exit(1)
    
    # Run tests
    env_ok = check_env_vars()
    
    if not env_ok:
        sys.exit(1)
    
    api_ok = test_databricks_api()
    sql_ok = test_sql_connection()
    clusters_ok = test_clusters_api()
    jobs_ok = test_jobs_api()
    dbfs_ok = test_dbfs_api()
    workspace_ok = test_workspace_api()
    instance_pools_ok = test_instance_pools_api()
    unity_catalog_ok = test_unity_catalog_api()
    
    # Summary
    print("\nTest Summary")
    print("===========")
    print(f"Environment Variables: {'✅ OK' if env_ok else '❌ Failed'}")
    print(f"Databricks API: {'✅ OK' if api_ok else '❌ Failed'}")
    print(f"Databricks SQL: {'✅ OK' if sql_ok else '❌ Failed'}")
    print(f"Clusters API: {'✅ OK' if clusters_ok else '❌ Failed'}")
    print(f"Jobs API: {'✅ OK' if jobs_ok else '❌ Failed'}")
    print(f"DBFS API: {'✅ OK' if dbfs_ok else '❌ Failed'}")
    print(f"Workspace API: {'✅ OK' if workspace_ok else '❌ Failed'}")
    print(f"Instance Pools API: {'✅ OK' if instance_pools_ok else '❌ Failed'}")
    print(f"Unity Catalog API: {'✅ OK' if unity_catalog_ok else '❌ Failed'}")
    
    if env_ok and api_ok and sql_ok and clusters_ok and jobs_ok and dbfs_ok and workspace_ok and instance_pools_ok and unity_catalog_ok:
        print("\n✅ All tests passed! Your Databricks MCP server should work correctly.")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed. Please check the errors above and fix your configuration.")
        sys.exit(1) 