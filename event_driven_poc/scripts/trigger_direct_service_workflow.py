#!/usr/bin/env python3
"""
Trigger Direct Service Workflow
Simple script to trigger the direct_service_workflow
Automatically registers the workflow if not already registered
"""

import requests
import sys
import os
import json

# Configuration
CONDUCTOR_API_URL = "http://localhost:8080/api"
WORKFLOW_NAME = "direct_service_workflow"
WORKFLOW_FILE = os.path.join(os.path.dirname(__file__), "..", "workflows", "direct_service_workflow.json")

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

def register_workflow():
    """Register the workflow in Conductor"""
    if not os.path.exists(WORKFLOW_FILE):
        print(f"❌ Error: Workflow file not found: {WORKFLOW_FILE}")
        sys.exit(1)
    
    url = f"{CONDUCTOR_API_URL}/metadata/workflow"
    
    try:
        with open(WORKFLOW_FILE, 'r') as f:
            workflow_def = json.load(f)
        
        print(f"📝 Registering workflow '{WORKFLOW_NAME}'...")
        response = requests.post(url, json=workflow_def, timeout=10)
        response.raise_for_status()
        print(f"✅ Workflow registered successfully")
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ Error registering workflow: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Response: {e.response.text}")
        sys.exit(1)

def trigger_workflow():
    """Trigger the direct service workflow"""
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
    print(f"Direct Service Workflow Manager")
    print("=" * 60)
    print(f"Workflow Name: {WORKFLOW_NAME}")
    print(f"Conductor API: {CONDUCTOR_API_URL}")
    print(f"Workflow File: {WORKFLOW_FILE}")
    print("=" * 60)
    print()
    
    # Check if workflow is registered
    if not is_workflow_registered():
        print(f"⚠️  Workflow '{WORKFLOW_NAME}' is not registered")
        register_workflow()
    else:
        print(f"✅ Workflow '{WORKFLOW_NAME}' is already registered")
    
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

