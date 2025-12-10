#!/usr/bin/env python3
"""
Register Event Handlers for completing async EVENT tasks
Uses Conductor's built-in event processor instead of custom router
"""

import os
import json
import requests
import argparse
from pathlib import Path

# Configuration
CONDUCTOR_API_URL = os.getenv('CONDUCTOR_API_URL', 'http://localhost:8080/api')
EVENT_HANDLERS_DIR = Path(__file__).parent.parent / 'event_handlers'

def register_event_handler(handler_file):
    """Register a single event handler"""
    handler_name = handler_file.stem
    
    try:
        with open(handler_file, 'r') as f:
            handler_def = json.load(f)
        
        url = f"{CONDUCTOR_API_URL}/event"
        
        print(f"📝 Registering event handler: {handler_name}")
        print(f"   Event: {handler_def.get('event', 'N/A')}")
        print(f"   Action: {handler_def.get('actions', [{}])[0].get('action', 'N/A')}")
        
        response = requests.post(url, json=handler_def, timeout=10)
        
        if response.status_code in [200, 201, 204]:
            print(f"✅ Event handler '{handler_name}' registered successfully")
            return True
        elif response.status_code == 409:
            print(f"⚠️  Event handler '{handler_name}' already exists - updating...")
            # Try to update by deleting and re-registering
            delete_url = f"{CONDUCTOR_API_URL}/event/{handler_name}"
            requests.delete(delete_url, timeout=10)
            response = requests.post(url, json=handler_def, timeout=10)
            if response.status_code in [200, 201, 204]:
                print(f"✅ Event handler '{handler_name}' updated successfully")
                return True
            else:
                print(f"❌ Failed to update event handler: {response.status_code}")
                print(f"   Response: {response.text}")
                return False
        else:
            print(f"❌ Failed to register event handler: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error registering event handler '{handler_name}': {e}")
        return False

def list_event_handlers():
    """List all registered event handlers"""
    try:
        url = f"{CONDUCTOR_API_URL}/event"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            handlers = response.json()
            return handlers
        else:
            print(f"❌ Failed to list event handlers: {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ Error listing event handlers: {e}")
        return []

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Register Event Handlers for Conductor')
    parser.add_argument('--list', action='store_true',
                       help='List all registered event handlers')
    parser.add_argument('--handler', type=str,
                       help='Register a specific event handler file')
    args = parser.parse_args()
    
    print("=" * 60)
    print("Conductor Event Handler Manager")
    print("=" * 60)
    print(f"Conductor API: {CONDUCTOR_API_URL}")
    print(f"Event Handlers Directory: {EVENT_HANDLERS_DIR}")
    print("=" * 60)
    print()
    
    if args.list:
        handlers = list_event_handlers()
        if handlers:
            print(f"📋 Found {len(handlers)} registered event handlers:")
            for handler in handlers:
                print(f"   - {handler.get('name', 'N/A')}: {handler.get('event', 'N/A')}")
        else:
            print("📋 No event handlers registered")
        return
    
    if args.handler:
        # Register specific handler
        handler_file = EVENT_HANDLERS_DIR / f"{args.handler}.json"
        if not handler_file.exists():
            print(f"❌ Event handler file not found: {handler_file}")
            return
        register_event_handler(handler_file)
        return
    
    # Register all handlers
    if not EVENT_HANDLERS_DIR.exists():
        print(f"❌ Event handlers directory not found: {EVENT_HANDLERS_DIR}")
        return
    
    handler_files = list(EVENT_HANDLERS_DIR.glob("*.json"))
    
    if not handler_files:
        print(f"⚠️  No event handler files found in {EVENT_HANDLERS_DIR}")
        return
    
    print(f"📝 Found {len(handler_files)} event handler files")
    print()
    
    success_count = 0
    for handler_file in sorted(handler_files):
        if register_event_handler(handler_file):
            success_count += 1
        print()
    
    print("=" * 60)
    print(f"✅ Registered {success_count}/{len(handler_files)} event handlers")
    print("=" * 60)
    print()
    print("💡 Event handlers will now complete EVENT tasks automatically")
    print("   when services publish completion events to conductor-events")
    print()

if __name__ == "__main__":
    main()








