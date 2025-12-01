#!/usr/bin/env python3
"""
Event Router Service
Routes events from Conductor to appropriate microservice topics
"""

import os
import json
import time
import logging
from kafka import KafkaConsumer, KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')

# Initialize Kafka producer
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Service routing configuration
SERVICE_ROUTING = {
    'email_validation': 'email-validation-requests',
    'phone_validation': 'phone-validation-requests', 
    'enrichment': 'enrichment-requests'
}

class EventRouter:
    """Routes events from Conductor to microservice topics"""
    
    def __init__(self):
        self.kafka_producer = kafka_producer
    
    def route_task_event(self, event):
        """Route task event to appropriate microservice topic"""
        try:
            task_type = event.get('data', {}).get('taskType', 'unknown')
            target_topic = SERVICE_ROUTING.get(task_type)
            
            logger.info(f"Attempting to route task type '{task_type}' to topic '{target_topic}'")
            
            if target_topic:
                # Route to microservice topic
                self.kafka_producer.send(target_topic, event)
                self.kafka_producer.flush()
                logger.info(f"✅ Successfully routed {task_type} event to {target_topic}")
            else:
                logger.warning(f"❌ No routing found for task type: {task_type}")
                logger.info(f"Available routing: {SERVICE_ROUTING}")
                
        except Exception as e:
            logger.error(f"❌ Error routing event: {e}", exc_info=True)
    
    def consume_conductor_events(self):
        """Consume events from Conductor and route them"""
        consumer = KafkaConsumer(
            'conductor-events',
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='event-router-group',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000
        )
        
        logger.info("Event Router started - listening for conductor events")
        
        for message in consumer:
            try:
                event = message.value
                
                # Handle both string and dict formats
                if isinstance(event, str):
                    try:
                        event = json.loads(event)
                    except json.JSONDecodeError:
                        logger.warning(f"Received non-JSON string event: {event}")
                        continue
                
                logger.info(f"Received conductor event: {event.get('eventType', 'unknown')}")
                logger.info(f"Event data: {event.get('data', {})}")
                
                # Route task events to microservices
                if event.get('eventType') == 'task.started':
                    logger.info(f"Routing task.started event for task type: {event.get('data', {}).get('taskType', 'unknown')}")
                    self.route_task_event(event)
                else:
                    logger.info(f"Skipping event type: {event.get('eventType', 'unknown')}")
                    
            except Exception as e:
                logger.error(f"Error processing conductor event: {e}", exc_info=True)

def main():
    """Start the Event Router"""
    logger.info("Starting Event Router Service")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
    logger.info(f"Service routing: {SERVICE_ROUTING}")
    
    # Wait for services to be ready
    logger.info("Waiting 10 seconds for services to initialize...")
    time.sleep(10)
    
    # Start event router
    router = EventRouter()
    router.consume_conductor_events()

if __name__ == '__main__':
    main()
