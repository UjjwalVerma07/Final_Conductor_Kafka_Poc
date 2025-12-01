#!/usr/bin/env python3
"""
Test script for Airflow Adapter Service
Publishes a test event to airflow-trigger-requests topic
"""

import json
import time
from kafka import KafkaProducer

# Configuration
KAFKA_BOOTSTRAP = 'localhost:9092'
TOPIC = 'airflow-trigger-requests'

# Initialize Kafka producer with longer timeout
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    request_timeout_ms=60000  # 60 seconds
)

# Create test event
test_event = {
    "workflowId": f"test-workflow-{int(time.time())}",
    "taskId": "test-airflow-trigger-task",
    "eventType": "airflow_trigger_request",
    "data": {
        "dag_id": "nua-nameparse-process-stage-v02-00-06-tiny",
        "execution_id": "WBNameParse",
        "metadata_url": "s3://958825666686-dpservices-testing-data/conductor-poc/1000861509.WBNameParse.json",
        "pipelineStage": "airflow_processing"
    }
}

print("🚀 Testing Airflow Adapter Service")
print(f"📤 Publishing test event to topic: {TOPIC}")
print(f"📋 Event details:")
print(f"   Workflow ID: {test_event['workflowId']}")
print(f"   Task ID: {test_event['taskId']}")
print(f"   DAG ID: {test_event['data']['dag_id']}")
print(f"   Execution ID: {test_event['data']['execution_id']}")
print(f"   Metadata URL: {test_event['data']['metadata_url']}")
print()

try:
    # Send the event
    future = producer.send(TOPIC, test_event)
    producer.flush()
    
    # Get the result
    record_metadata = future.get(timeout=10)
    
    print("✅ Test event published successfully!")
    print(f"   Topic: {record_metadata.topic}")
    print(f"   Partition: {record_metadata.partition}")
    print(f"   Offset: {record_metadata.offset}")
    print()
    print("📝 Next steps:")
    print("   1. Check airflow-adapter-event container logs:")
    print("      docker logs -f airflow-adapter-event")
    print("   2. The service should:")
    print("      - Receive the event")
    print("      - Trigger the DAG via script")
    print("      - Wait for DAG completion")
    print("      - Publish completion event to conductor-events topic")
    
except Exception as e:
    print(f"❌ Error publishing test event: {e}")
    print()
    print("💡 Troubleshooting:")
    print("   1. Make sure Kafka is running:")
    print("      docker ps | grep kafka")
    print("   2. Make sure airflow-adapter service is running:")
    print("      docker ps | grep airflow-adapter")
    print("   3. Check if the topic exists:")
    print("      docker exec kafka-event kafka-topics.sh --list --bootstrap-server localhost:9092")

