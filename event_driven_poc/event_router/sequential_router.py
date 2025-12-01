#!/usr/bin/env python3
"""
Sequential Event Router Service
Routes events from Conductor to microservices in a sequential pipeline manner
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

# Sequential pipeline routing configuration
PIPELINE_STAGES = {
    'pipeline_started': 'email-validation-requests',
    'email_validation_completed': 'phone-validation-requests',
    'phone_validation_completed': 'enrichment-requests',
    'enrichment_completed': 'pipeline-completion'
}

class SequentialEventRouter:
    """Routes events in a sequential pipeline manner"""
    
    def __init__(self):
        self.kafka_producer = kafka_producer
        self.pipeline_state = {}  # Track pipeline state per workflow
    
    def process_pipeline_event(self, event):
        """Process pipeline event and route to next stage"""
        try:
            event_type = event.get('eventType', 'unknown')
            workflow_id = event.get('workflowId', 'unknown')
            data = event.get('data', {})
            pipeline_stage = data.get('pipelineStage', 'unknown')
            
            logger.info(f"Processing pipeline event: {event_type} for workflow: {workflow_id}")
            logger.info(f"Pipeline stage: {pipeline_stage}")
            
            # Update pipeline state
            if workflow_id not in self.pipeline_state:
                self.pipeline_state[workflow_id] = {
                    'current_stage': 'start',
                    'stages_completed': [],
                    'data_flow': {}
                }
            
            # Route based on event type
            if event_type == 'pipeline_started':
                self.route_to_email_validation(event)
            elif event_type == 'phone_validation_requested':
                self.route_to_phone_validation(event)
            elif event_type == 'enrichment_requested':
                self.route_to_enrichment(event)
            elif event_type == 'pipeline_completed':
                self.route_to_completion(event)
            elif event_type in ['email_validation_completed', 'phone_validation_completed', 'enrichment_completed']:
                # Handle completion events (for future use)
                logger.info(f"Received completion event: {event_type} - no routing needed")
            else:
                logger.warning(f"Unknown event type: {event_type}")
                
        except Exception as e:
            logger.error(f"❌ Error processing pipeline event: {e}", exc_info=True)
    
    def route_to_email_validation(self, event):
        """Route to email validation stage"""
        try:
            workflow_id = event.get('workflowId')
            data = event.get('data', {})
            
            # Create email validation request
            email_request = {
                'workflowId': workflow_id,
                'eventType': 'email_validation_request',
                'data': {
                    'inputData': data.get('inputData', 'raw_data'),
                    'records': data.get('records', 100),
                    'pipelineStage': 'email_validation',
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Send to email validation topic
            self.kafka_producer.send('email-validation-requests', email_request)
            self.kafka_producer.flush()
            
            logger.info(f"✅ Routed to email validation for workflow: {workflow_id}")
            
        except Exception as e:
            logger.error(f"❌ Error routing to email validation: {e}")
    
    def route_to_phone_validation(self, event):
        """Route to phone validation stage"""
        try:
            workflow_id = event.get('workflowId')
            data = event.get('data', {})
            
            # Create phone validation request
            phone_request = {
                'workflowId': workflow_id,
                'eventType': 'phone_validation_request',
                'data': {
                    'inputData': data.get('inputData', 'email_validated_data'),
                    'records': data.get('records', 95),
                    'pipelineStage': 'phone_validation',
                    'previousStage': 'email_validation',
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Send to phone validation topic
            self.kafka_producer.send('phone-validation-requests', phone_request)
            self.kafka_producer.flush()
            
            logger.info(f"✅ Routed to phone validation for workflow: {workflow_id}")
            
        except Exception as e:
            logger.error(f"❌ Error routing to phone validation: {e}")
    
    def route_to_enrichment(self, event):
        """Route to enrichment stage"""
        try:
            workflow_id = event.get('workflowId')
            data = event.get('data', {})
            
            # Create enrichment request
            enrichment_request = {
                'workflowId': workflow_id,
                'eventType': 'enrichment_request',
                'data': {
                    'inputData': data.get('inputData', 'phone_validated_data'),
                    'records': data.get('records', 90),
                    'pipelineStage': 'enrichment',
                    'previousStage': 'phone_validation',
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Send to enrichment topic
            self.kafka_producer.send('enrichment-requests', enrichment_request)
            self.kafka_producer.flush()
            
            logger.info(f"✅ Routed to enrichment for workflow: {workflow_id}")
            
        except Exception as e:
            logger.error(f"❌ Error routing to enrichment: {e}")
    
    def route_to_completion(self, event):
        """Route to pipeline completion"""
        try:
            workflow_id = event.get('workflowId')
            data = event.get('data', {})
            
            # Create completion event
            completion_event = {
                'workflowId': workflow_id,
                'eventType': 'pipeline_completed',
                'data': {
                    'finalData': data.get('inputData', 'enriched_data'),
                    'records': data.get('records', 90),
                    'pipelineStage': 'completed',
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Send to completion topic
            self.kafka_producer.send('pipeline-completion', completion_event)
            self.kafka_producer.flush()
            
            logger.info(f"✅ Pipeline completed for workflow: {workflow_id}")
            
        except Exception as e:
            logger.error(f"❌ Error routing to completion: {e}")
    
    def consume_conductor_events(self):
        """Consume events from Conductor and route them sequentially"""
        consumer = KafkaConsumer(
            'conductor-events',
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: self._safe_deserialize(m),
            auto_offset_reset='earliest',
            group_id='sequential-router-group',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000
        )
        
        logger.info("Sequential Event Router started - listening for conductor events")
        
        for message in consumer:
            try:
                event = message.value
                
                # Skip if event is None or empty
                if not event:
                    logger.warning("Received empty or null event, skipping")
                    continue
                
                # Handle both string and dict formats
                if isinstance(event, str):
                    # Skip simple string messages (like "Hello from Conductor!")
                    if not event.strip().startswith('{'):
                        logger.info(f"Received non-JSON string event (skipping): {event[:50]}...")
                        continue
                    try:
                        event = json.loads(event)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON string: {event[:100]}... Error: {e}")
                        continue
                
                # Ensure event is a dictionary
                if not isinstance(event, dict):
                    logger.warning(f"Received non-dict event: {type(event)} - {str(event)[:100]}...")
                    continue
                
                event_type = event.get('eventType', 'unknown')
                logger.info(f"Received conductor event: {event_type}")
                
                # Process pipeline events
                self.process_pipeline_event(event)
                    
            except Exception as e:
                logger.error(f"Error processing conductor event: {e}", exc_info=True)
    
    def _safe_deserialize(self, message_bytes):
        """Safely deserialize Kafka message, handling both JSON and string formats"""
        try:
            # First try to decode as UTF-8
            message_str = message_bytes.decode('utf-8')
            
            # Remove surrounding quotes if present
            message_str = message_str.strip()
            if message_str.startswith('"') and message_str.endswith('"'):
                message_str = message_str[1:-1]
                # Unescape JSON string
                message_str = message_str.replace('\\"', '"').replace('\\n', '\n').replace('\\t', '\t')
            
            # If it looks like JSON, try to parse it
            if message_str.strip().startswith('{'):
                return json.loads(message_str)
            else:
                # Return as string for non-JSON messages
                return message_str
                
        except UnicodeDecodeError:
            logger.error(f"Failed to decode message as UTF-8: {message_bytes}")
            return None
        except json.JSONDecodeError as e:
            # Return as string if JSON parsing fails
            logger.warning(f"JSON parse failed, treating as string: {e}")
            return message_str
        except Exception as e:
            logger.error(f"Unexpected error deserializing message: {e}")
            return None

def main():
    """Start the Sequential Event Router"""
    logger.info("Starting Sequential Event Router Service")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
    logger.info(f"Pipeline stages: {PIPELINE_STAGES}")
    
    # Wait for services to be ready
    logger.info("Waiting 10 seconds for services to initialize...")
    time.sleep(10)
    
    # Start sequential event router
    router = SequentialEventRouter()
    router.consume_conductor_events()

if __name__ == '__main__':
    main()
