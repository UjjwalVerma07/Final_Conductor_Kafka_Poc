#!/usr/bin/env python3
"""
Test Sequential Pipeline Workflow
Demonstrates sequential event-driven processing with data flow between microservices
"""

import os
import json
import time
import requests
import logging
from kafka import KafkaConsumer, KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
CONDUCTOR_SERVER_URL = os.getenv('CONDUCTOR_SERVER_URL', 'http://localhost:8080/api')
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')  # Use localhost for host machine access

def check_conductor_health():
    """Check if Conductor server is healthy"""
    try:
        response = requests.get(f"{CONDUCTOR_SERVER_URL.replace('/api', '')}/health", timeout=10)
        if response.status_code == 200:
            logger.info("✅ Conductor server is healthy")
            return True
        else:
            logger.error(f"❌ Conductor server health check failed: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"❌ Cannot connect to Conductor server: {e}")
        return False

def register_event_task_definition():
    """Register the EVENT task definition"""
    try:
        with open('task_definitions/event_task.json', 'r') as f:
            task_def = json.load(f)
        
        response = requests.post(
            f"{CONDUCTOR_SERVER_URL}/metadata/taskdefs",
            json=task_def
        )
        
        if response.status_code == 200:
            logger.info("✅ Registered EVENT task definition")
            return True
        elif response.status_code == 409:
            logger.info("✅ EVENT task definition already exists")
            return True
        else:
            logger.error(f"❌ Failed to register EVENT task definition: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error registering EVENT task definition: {e}")
        return False

def register_kafka_publish_task_definition():
    """Register the KAFKA_PUBLISH task definition"""
    try:
        with open('task_definitions/kafka_publish_task.json', 'r') as f:
            task_def = json.load(f)
        
        response = requests.post(
            f"{CONDUCTOR_SERVER_URL}/metadata/taskdefs",
            json=task_def
        )
        
        if response.status_code == 200:
            logger.info("✅ Registered KAFKA_PUBLISH task definition")
            return True
        elif response.status_code == 409:
            logger.info("✅ KAFKA_PUBLISH task definition already exists")
            return True
        else:
            logger.error(f"❌ Failed to register KAFKA_PUBLISH task definition: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error registering KAFKA_PUBLISH task definition: {e}")
        return False

def register_event_driven_sequential_workflow():
    """Register the event-driven sequential workflow"""
    try:
        with open('workflows/event_driven_sequential_workflow.json', 'r') as f:
            workflow = json.load(f)
        
        response = requests.post(
            f"{CONDUCTOR_SERVER_URL}/metadata/workflow",
            json=workflow
        )
        
        if response.status_code == 200:
            logger.info("✅ Registered event-driven sequential workflow")
            return True
        elif response.status_code == 409:
            logger.info("✅ Event-driven sequential workflow already exists")
            return True
        else:
            logger.error(f"❌ Failed to register workflow: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error registering workflow: {e}")
        return False

def start_event_driven_sequential_workflow():
    """Start the event-driven sequential workflow"""
    try:
        workflow_input = {
            "rawData": "sample_customer_data_with_emails_and_phones"
        }
        
        response = requests.post(
            f"{CONDUCTOR_SERVER_URL}/workflow/event_driven_sequential_workflow",
            json=workflow_input
        )
        
        if response.status_code == 200:
            # Response is just the workflow ID as plain text, not JSON
            workflow_id = response.text.strip()
            logger.info(f"✅ Started event-driven sequential workflow: {workflow_id}")
            return workflow_id
        else:
            logger.error(f"❌ Failed to start workflow: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error starting workflow: {e}")
        return None

def monitor_workflow(workflow_id):
    """Monitor workflow execution"""
    try:
        max_attempts = 60  # 10 minutes max
        attempt = 0
        
        while attempt < max_attempts:
            response = requests.get(f"{CONDUCTOR_SERVER_URL}/workflow/{workflow_id}")
            
            if response.status_code == 200:
                workflow_status = response.json()
                status = workflow_status.get('status', 'UNKNOWN')
                
                logger.info(f"Workflow status: {status}")
                
                if status == 'COMPLETED':
                    logger.info("✅ Sequential pipeline workflow completed successfully!")
                    return workflow_status
                elif status == 'FAILED':
                    logger.error("❌ Workflow failed!")
                    return workflow_status
                elif status in ['RUNNING', 'PAUSED']:
                    time.sleep(10)
                    attempt += 1
                else:
                    logger.warning(f"Unknown workflow status: {status}")
                    time.sleep(10)
                    attempt += 1
            else:
                logger.error(f"Failed to get workflow status: {response.status_code}")
                time.sleep(10)
                attempt += 1
        
        logger.error("❌ Workflow monitoring timed out")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error monitoring workflow: {e}")
        return None

def check_kafka_pipeline_messages():
    """Check Kafka messages to see pipeline processing"""
    try:
        logger.info(f"🔌 Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
        consumer = KafkaConsumer(
            'conductor-events',
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            consumer_timeout_ms=15000,
            request_timeout_ms=10000,
            api_version=(2, 0, 0)
        )
        
        logger.info("📨 Checking Kafka pipeline messages...")
        
        message_count = 0
        pipeline_messages = []
        
        for message in consumer:
            message_count += 1
            event = message.value
            
            if isinstance(event, dict) and event.get('eventType'):
                pipeline_messages.append(event)
                logger.info(f"📨 Pipeline event: {event.get('eventType')} - {event.get('data', {}).get('pipelineStage', 'unknown')}")
        
        if message_count == 0:
            logger.warning("⚠️ No new pipeline messages found in Kafka")
        else:
            logger.info(f"📊 Found {message_count} pipeline messages in Kafka")
            
            # Show pipeline flow
            logger.info("🔄 Pipeline Flow:")
            for msg in pipeline_messages:
                stage = msg.get('data', {}).get('pipelineStage', 'unknown')
                event_type = msg.get('eventType', 'unknown')
                logger.info(f"  {stage}: {event_type}")
        
        consumer.close()
        
    except Exception as e:
        logger.warning(f"⚠️ Cannot connect to Kafka for monitoring: {e}")
        logger.info("💡 This is normal if running from host machine - workflow will still execute")
        logger.info("💡 To monitor Kafka messages, run: docker exec -it kafka-event kafka-console-consumer --bootstrap-server localhost:9092 --topic conductor-events --from-beginning")

def main():
    """Main test function"""
    logger.info("🚀 Starting Event-Driven Sequential Pipeline Test")
    logger.info(f"Conductor: {CONDUCTOR_SERVER_URL}")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
    
    # Step 1: Check Conductor health
    if not check_conductor_health():
        logger.error("❌ Conductor server is not healthy")
        return False
    
    # Step 2: Register task definitions
    if not register_event_task_definition():
        logger.error("❌ Failed to register EVENT task definition")
        return False
    
    if not register_kafka_publish_task_definition():
        logger.error("❌ Failed to register KAFKA_PUBLISH task definition")
        return False
    
    # Step 3: Register workflow
    if not register_event_driven_sequential_workflow():
        logger.error("❌ Failed to register event-driven sequential workflow")
        return False
    
    # Step 4: Start workflow
    workflow_id = start_event_driven_sequential_workflow()
    if not workflow_id:
        logger.error("❌ Failed to start workflow")
        return False
    
    # Step 5: Monitor workflow
    logger.info("👀 Monitoring event-driven sequential pipeline execution...")
    workflow_result = monitor_workflow(workflow_id)
    
    if workflow_result:
        logger.info("✅ Workflow monitoring completed")
    else:
        logger.warning("⚠️ Workflow monitoring timed out, but workflow may still be running")
        logger.info(f"💡 Check workflow status manually: curl 'http://localhost:8080/api/workflow/{workflow_id}'")
    
    # Step 6: Check Kafka messages (optional)
    logger.info("📨 Checking pipeline messages...")
    check_kafka_pipeline_messages()
    
    if workflow_result and workflow_result.get('status') == 'COMPLETED':
        logger.info("🎉 Event-driven sequential pipeline test completed successfully!")
        logger.info("🔄 Pipeline Flow: Start → Email Validation (EVENT) → Phone Validation (EVENT) → Enrichment (EVENT) → Complete")
        return True
    else:
        logger.warning("⚠️ Event-driven sequential pipeline test completed with issues")
        return False

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
 