#!/usr/bin/env python3
"""
Trigger Test Workflow 4 (test_workflow4)
Simple script to trigger the test_workflow4
Automatically registers the workflow if not already registered
Service order: Phone → Enrichment → Email
"""

import requests
import sys
import os
import json
import argparse

# Configuration
CONDUCTOR_API_URL = "http://localhost:8080/api"
WORKFLOW_NAME = "test_workflow4"
WORKFLOW_FILE = os.path.join(os.path.dirname(__file__), "..", "workflows", "test_workflow4.json")

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
    """Trigger the test_workflow4"""
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
    parser = argparse.ArgumentParser(description='Trigger Test Workflow 4')
    parser.add_argument('--update', action='store_true',
                       help='Force re-register/update the workflow even if already registered')
    args = parser.parse_args()
    
    print("=" * 60)
    print(f"Test Workflow 4 Manager (test_workflow4)")
    print("Service Order: Phone → Enrichment → Email")
    print("=" * 60)
    print(f"Workflow Name: {WORKFLOW_NAME}")
    print(f"Conductor API: {CONDUCTOR_API_URL}")
    print(f"Workflow File: {WORKFLOW_FILE}")
    print("=" * 60)
    print()
    
    # Always register/update to ensure latest version is used
    is_registered = is_workflow_registered()
    
    if args.update or not is_registered:
        if args.update and is_registered:
            print(f"🔄 Force updating workflow '{WORKFLOW_NAME}'...")
        elif not is_registered:
            print(f"⚠️  Workflow '{WORKFLOW_NAME}' is not registered")
        register_workflow(force_update=True)
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
    print(f"   - Check Kafka topics to see events flowing directly to services")
    print(f"   - Service execution order: Phone → Enrichment → Email")
    print()
    print("📋 Expected Input Flow:")
    print("   - Phone Validation: raw-data / customer_data_sample.csv ✅")
    print("   - Enrichment: phone-validated / phone_validation_${workflow.workflowId}.csv")
    print("   - Email Validation: enriched / enrichment_${workflow.workflowId}.csv")
    print()

if __name__ == "__main__":
    main()




