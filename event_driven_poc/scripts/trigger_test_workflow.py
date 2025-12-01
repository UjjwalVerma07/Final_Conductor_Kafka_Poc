#!/usr/bin/env python3
"""
Trigger Test Workflow (poc_workflow)
Simple script to trigger the poc_workflow
Automatically registers the workflow if not already registered
"""

import requests
import sys
import os
import json

# Configuration
CONDUCTOR_API_URL = "http://localhost:8080/api"
WORKFLOW_NAME = "poc_workflow"
WORKFLOW_FILE = os.path.join(os.path.dirname(__file__), "..", "workflows", "test_workflow.json")

def is_workflow_registered():
    """Check if the workflow is already registered"""
    url = f"{CONDUCTOR_API_URL}/metadata/workflow/{WORKFLOW_NAME}"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return True
        return False
    except requests.exceptions.RequestException:
        return False

def register_workflow(force_update=False):
    """Register or update the workflow in Conductor"""
    if not os.path.exists(WORKFLOW_FILE):
        print(f"❌ Error: Workflow file not found: {WORKFLOW_FILE}")
        sys.exit(1)
    
    url = f"{CONDUCTOR_API_URL}/metadata/workflow"
    
    try:
        with open(WORKFLOW_FILE, 'r') as f:
            workflow_def = json.load(f)
        
        action = "Updating" if force_update else "Registering"
        print(f"📝 {action} workflow '{WORKFLOW_NAME}' (version {workflow_def.get('version', 'N/A')})...")
        response = requests.post(url, json=workflow_def, timeout=10)
        
        # Handle 409 conflict (version already exists) as success
        if response.status_code == 409:
            print(f"✅ Workflow version {workflow_def.get('version', 'N/A')} already exists - using existing version")
            return True
        
        response.raise_for_status()
        print(f"✅ Workflow {'updated' if force_update else 'registered'} successfully")
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ Error {'updating' if force_update else 'registering'} workflow: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Response: {e.response.text}")
        sys.exit(1)

def trigger_workflow():
    """Trigger the poc_workflow"""
    url = f"{CONDUCTOR_API_URL}/workflow/{WORKFLOW_NAME}"
    payload = {}
    
    try:
        print(f"🚀 Triggering workflow '{WORKFLOW_NAME}'...")
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        workflow_id = response.text.strip().strip('"')
        print(f"✅ Workflow triggered successfully!")
        print(f"📋 Workflow ID: {workflow_id}")
        print(f"🔗 View workflow: {CONDUCTOR_API_URL.replace('/api', '')}/workflow/{workflow_id}")
        return workflow_id
    except requests.exceptions.RequestException as e:
        print(f"❌ Error triggering workflow: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Response: {e.response.text}")
        sys.exit(1)

def main():
    """Main function"""
    print("=" * 60)
    print(f"Test Workflow Manager (poc_workflow)")
    print("=" * 60)
    print(f"Workflow Name: {WORKFLOW_NAME}")
    print(f"Conductor API: {CONDUCTOR_API_URL}")
    print(f"Workflow File: {WORKFLOW_FILE}")
    print("=" * 60)
    print()
    
    # Always register/update to ensure latest version is used
    is_registered = is_workflow_registered()
    
    if is_registered:
        print(f"✅ Workflow '{WORKFLOW_NAME}' is already registered")
        print(f"🔄 Updating to latest version from file...")
        register_workflow(force_update=True)
    else:
        print(f"⚠️  Workflow '{WORKFLOW_NAME}' is not registered")
        print(f"📝 Registering workflow...")
        register_workflow()
    
    print()
    
    # Trigger the workflow
    workflow_id = trigger_workflow()
    
    print()
    print("=" * 60)
    print("✅ Workflow execution started!")
    print("=" * 60)
    print()
    print("💡 Tips:")
    print(f"   - Monitor workflow status: {CONDUCTOR_API_URL}/workflow/{workflow_id}")
    print(f"   - View in UI: {CONDUCTOR_API_URL.replace('/api', '')}/workflow/{workflow_id}")
    print(f"   - Check Kafka topics to see events flowing directly to services")
    print()

if __name__ == "__main__":
    main()

