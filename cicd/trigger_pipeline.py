"""
Databricks Pipeline Trigger Script for CI/CD
This script can be used in GitHub Actions or other CI/CD tools to trigger the pipeline.

Usage:
    python trigger_pipeline.py --action start
    python trigger_pipeline.py --action status
    python trigger_pipeline.py --action stop

Environment Variables Required:
    DATABRICKS_HOST: Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com)
    DATABRICKS_TOKEN: Databricks personal access token
    PIPELINE_ID: The pipeline ID (default: f94b89a8-8688-4a03-b285-e88727b7f403)
"""

import os
import sys
import time
import requests
import argparse
from typing import Dict, Any

# Pipeline Configuration
PIPELINE_ID = "f94b89a8-8688-4a03-b285-e88727b7f403"
PIPELINE_NAME = "Complete-MLOps-Pipeline"


class DatabricksPipelineClient:
    """Client for interacting with Databricks Delta Live Tables API"""
    
    def __init__(self, host: str, token: str, pipeline_id: str):
        self.host = host.rstrip('/')
        self.token = token
        self.pipeline_id = pipeline_id
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        self.base_url = f"{self.host}/api/2.0/pipelines"
    
    def start_update(self, full_refresh: bool = False) -> Dict[str, Any]:
        """Start a pipeline update"""
        url = f"{self.base_url}/{self.pipeline_id}/updates"
        payload = {"full_refresh": full_refresh}
        
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()
    
    def get_update_status(self, update_id: str) -> Dict[str, Any]:
        """Get the status of a pipeline update"""
        url = f"{self.base_url}/{self.pipeline_id}/updates/{update_id}"
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get the current pipeline status"""
        url = f"{self.base_url}/{self.pipeline_id}"
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def stop_pipeline(self) -> Dict[str, Any]:
        """Stop the pipeline"""
        url = f"{self.base_url}/{self.pipeline_id}/stop"
        
        response = requests.post(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def wait_for_completion(self, update_id: str, timeout: int = 3600) -> bool:
        """Wait for pipeline update to complete"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            status = self.get_update_status(update_id)
            state = status.get("update", {}).get("state")
            
            print(f"Pipeline state: {state}")
            
            if state in ["COMPLETED", "FAILED", "CANCELED"]:
                return state == "COMPLETED"
            
            time.sleep(30)  # Check every 30 seconds
        
        print(f"Timeout after {timeout} seconds")
        return False


def main():
    parser = argparse.ArgumentParser(description="Trigger Databricks Pipeline")
    parser.add_argument("--action", choices=["start", "status", "stop", "start-and-wait"], 
                       default="start", help="Action to perform")
    parser.add_argument("--full-refresh", action="store_true", 
                       help="Perform full refresh instead of incremental update")
    args = parser.parse_args()
    
    # Get credentials from environment
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    pipeline_id = os.getenv("PIPELINE_ID", PIPELINE_ID)
    
    if not host or not token:
        print("ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN environment variables must be set")
        sys.exit(1)
    
    # Create client
    client = DatabricksPipelineClient(host, token, pipeline_id)
    
    # Execute action
    if args.action == "start":
        result = client.start_update(full_refresh=args.full_refresh)
        print(f"Pipeline update started: {result}")
    elif args.action == "start-and-wait":
        result = client.start_update(full_refresh=args.full_refresh)
        update_id = result.get("update_id")
        print(f"Pipeline update started: {update_id}")
        success = client.wait_for_completion(update_id)
        sys.exit(0 if success else 1)
    elif args.action == "status":
        result = client.get_pipeline_status()
        print(f"Pipeline status: {result}")
    elif args.action == "stop":
        result = client.stop_pipeline()
        print(f"Pipeline stopped: {result}")


if __name__ == "__main__":
    main()
