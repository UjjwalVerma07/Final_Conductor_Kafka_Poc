#!/usr/bin/env python3
"""
Trigger Event Handler Test Workflow
Tests Event Handlers with email, phone, and enrichment tasks
"""

import os
import json
import requests
import argparse
from pathlib import Path

# Configuration
CONDUCTOR_API_URL = os.getenv('CONDUCTOR_API_URL', 'http://localhost:8080/api')
WORKFLOW_NAME = "event_handler_test_workflow"
WORKFLOW_FILE = Path(__file__).parent.parent / 'workflows' / 'event_handler_test_workflow.json'

def is_workflow_registered():
    """Check if workflow is already registered"""
    try:
        url = f"{CONDUCTOR_API_URL}/metadata/workflow/{WORKFLOW_NAME}"
        response = requests.get(url, timeout=10)
        return response.status_code == 200
    except Exception:
        return False

def register_workflow(force_update=False):
    """Register the workflow"""
    try:
        with open(WORKFLOW_FILE, 'r') as f:
            workflow_def = json.load(f)
        
        url = f"{CONDUCTOR_API_URL}/metadata/workflow"
        response = requests.post(url, json=workflow_def, timeout=10)
        
        # Handle 409 conflict (version already exists) as success
        if response.status_code == 409:
            print(f"✅ Workflow version {workflow_def.get('version', 'N/A')} already exists - using existing version")
            return True
        
        response.raise_for_status()
        print(f"✅ Workflow {'updated' if force_update else 'registered'} successfully")
        return True
    except Exception as e:
        print(f"❌ Error registering workflow: {e}")
        return False

def trigger_workflow():
    """Trigger the workflow"""
    try:
        url = f"{CONDUCTOR_API_URL}/workflow/{WORKFLOW_NAME}"
        response = requests.post(url, json={}, timeout=10)
        response.raise_for_status()
        workflow_id = response.json().get('workflowId')
        print(f"✅ Workflow triggered successfully!")
        print(f"📋 Workflow ID: {workflow_id}")
        print(f"🔗 View workflow: {CONDUCTOR_API_URL.replace('/api', '')}/workflow/{workflow_id}")
        return workflow_id
    except Exception as e:
        print(f"❌ Error triggering workflow: {e}")
        return None

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Trigger Event Handler Test Workflow')
    parser.add_argument('--update', action='store_true',
                       help='Force re-register/update the workflow even if already registered')
    args = parser.parse_args()
    
    print("=" * 60)
    print(f"Event Handler Test Workflow Manager")
    print("=" * 60)
    print(f"Workflow Name: {WORKFLOW_NAME}")
    print(f"Conductor API: {CONDUCTOR_API_URL}")
    print(f"Workflow File: {WORKFLOW_FILE}")
    print("=" * 60)
    print()
    
    # Check if workflow is registered
    is_registered = is_workflow_registered()
    
    if args.update or not is_registered:
        if args.update and is_registered:
            print(f"🔄 Force updating workflow '{WORKFLOW_NAME}'...")
        elif not is_registered:
            print(f"⚠️  Workflow '{WORKFLOW_NAME}' is not registered")
        register_workflow(force_update=args.update)
    else:
        print(f"✅ Workflow '{WORKFLOW_NAME}' is already registered")
        print(f"💡 Use --update flag to re-register with latest changes")
    
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
    print(f"   - Event Handlers should complete EVENT tasks automatically")
    print()

if __name__ == "__main__":
    main()




