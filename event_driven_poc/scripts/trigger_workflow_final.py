#!/usr/bin/env python3
"""
Trigger Event-Driven Sequential Workflow
Simple script to trigger the event_driven_sequential_workflow
Automatically registers the workflow if not already registered
"""

import requests
import sys
import os
import json

# Configuration
CONDUCTOR_API_URL = "http://localhost:8080/api"
WORKFLOW_NAME = "event_driven_sequential_workflow"
WORKFLOW_FILE = os.path.join(os.path.dirname(__file__), "..", "workflows", "event_driven_sequential_workflow.json")

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
        print(f"Error: Workflow file not found: {WORKFLOW_FILE}")
        sys.exit(1)
    
    url = f"{CONDUCTOR_API_URL}/metadata/workflow"
    
    try:
        with open(WORKFLOW_FILE, 'r') as f:
            workflow_def = json.load(f)
        
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
    """Trigger the event-driven sequential workflow"""
    url = f"{CONDUCTOR_API_URL}/workflow/{WORKFLOW_NAME}"
    payload = {}
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        workflow_id = response.text.strip()
        print(f"✅ Workflow triggered successfully!")
        print(f"📋 Workflow ID: {workflow_id}")
        return workflow_id
    except requests.exceptions.RequestException as e:
        print(f"❌ Error triggering workflow: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Response: {e.response.text}")
        sys.exit(1)

if __name__ == "__main__":
    # Check if workflow is registered
    if not is_workflow_registered():
        print(f"⚠️  Workflow '{WORKFLOW_NAME}' is not registered")
        print(f"📝 Registering workflow...")
        register_workflow()
    else:
        print(f"✅ Workflow '{WORKFLOW_NAME}' is already registered")
    
    # Trigger the workflow
    print(f"🚀 Triggering workflow...")
    trigger_workflow()
