#!/usr/bin/env python3
"""
Trigger Event-Driven Workflows
Triggers event-driven workflows for testing
"""

import os
import json
import requests
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
CONDUCTOR_SERVER_URL = os.getenv('CONDUCTOR_SERVER_URL', 'http://localhost:8080/api')

def trigger_workflow():
    """Trigger event-driven workflow"""
    try:
        workflow_input = {
            "input": {
                "data": "test_data",
                "records": 100
            }
        }
        
        response = requests.post(
            f"{CONDUCTOR_SERVER_URL}/workflow/event_driven_pipeline",
            json=workflow_input
        )
        
        if response.status_code == 200:
            result = response.json()
            workflow_id = result.get('workflowId')
            logger.info(f"Triggered workflow: {workflow_id}")
            return workflow_id
        else:
            logger.error(f"Failed to trigger workflow: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"Error triggering workflow: {e}")
        return None

def main():
    """Trigger event-driven workflow"""
    logger.info("Triggering event-driven workflow...")
    logger.info(f"Conductor: {CONDUCTOR_SERVER_URL}")
    
    # Trigger workflow
    workflow_id = trigger_workflow()
    
    if workflow_id:
        logger.info(f"Workflow triggered successfully: {workflow_id}")
        logger.info(f"Monitor workflow at: {CONDUCTOR_SERVER_URL}/workflow/{workflow_id}")
    else:
        logger.error("Failed to trigger workflow")

if __name__ == '__main__':
    main()
