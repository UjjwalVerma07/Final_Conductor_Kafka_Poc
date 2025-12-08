#!/usr/bin/env python3
"""
Enhanced Sequential Event Router Service
Bidirectional event routing: Conductor ↔ Microservices
Routes events from Conductor to microservices and handles results back
This is the main layer as it decouples the Conductor from microservices and handles the result back to the Conductor.
"""

import os
import json
import time
import logging
import threading
from kafka import KafkaConsumer, KafkaProducer
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
CONDUCTOR_API_URL = os.getenv('CONDUCTOR_API_URL', 'http://conductor-server:8080/api')

# Initialize Kafka producers
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Service routing configuration
SERVICE_ROUTING = {
    'pipeline_started': 'email-validation-requests',
    'phone_validation_requested': 'phone-validation-requests',
    'enrichment_requested': 'enrichment-requests',
    'airflow_trigger_requested': 'airflow-trigger-requests',
    'pipeline_completed': 'pipeline-completion'
}

# Results routing configuration
RESULTS_ROUTING = {
    'email-validation-results': 'email_validation_completed',
    'phone-validation-results': 'phone_validation_completed',
    'enrichment-results': 'enrichment_completed'
}

class EnhancedSequentialEventRouter:
    """Enhanced Event Router with bidirectional communication"""

    #This is the constructor for the EnhancedSequential Event Router.
    def __init__(self):
        self.kafka_producer = kafka_producer
        self.pipeline_state = {}  # Track pipeline state per workflow
        self.running = True
        
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
    
    def process_pipeline_event(self, event):
        """Process pipeline event and route to appropriate microservice"""

        """So Pipeline State is basically a dictionary that tracks the state of the pipeline for each workflow. """


        try:
            event_type = event.get('eventType', 'unknown')
            workflow_id = event.get('workflowId', 'unknown')
            data = event.get('data', {})
            pipeline_stage = data.get('pipelineStage', 'unknown')
            
            logger.info(f"Processing pipeline event: {event_type} for workflow: {workflow_id}")
            logger.info(f"Pipeline stage: {pipeline_stage}")
            
            # Update pipeline state
            #This is check if there exist a workflow in the pipeline state. If not, then it will create a new one
            if workflow_id not in self.pipeline_state:
                self.pipeline_state[workflow_id] = {
                    'current_stage': 'start',
                    'stages_completed': [],
                    'data_flow': {},
                    'waiting_for': None
                }
            
            #Then it will route the event ot the appropriate micorservice based on the event type.
            # Route based on event type
            if event_type == 'pipeline_started':
                self.route_to_email_validation(event)
            elif event_type == 'phone_validation_requested':
                self.route_to_phone_validation(event)
            elif event_type == 'enrichment_requested':
                self.route_to_enrichment(event)
            elif event_type == 'airflow_trigger_requested':
                self.route_to_airflow_trigger(event)
            elif event_type == 'pipeline_completed':
                self.route_to_completion(event)
            else:
                logger.warning(f"Unknown event type: {event_type}")
                
        except Exception as e:
            logger.error(f"❌ Error processing pipeline event: {e}", exc_info=True)
    
    def process_result_event(self, event, topic):
        """Process result event from microservice and notify Conductor"""
        """ This event is sent from micorservice to the Event Router process it and send back to the Conductor  """
        try:
            workflow_id = event.get('workflowId', 'unknown')
            task_id = event.get('taskId', 'unknown')
            event_type = event.get('eventType', 'unknown')
            data = event.get('data', {})
            
            logger.info(f"Processing result event: {event_type} for workflow: {workflow_id}, task: {task_id}")
            
            # Map result topic to completion event type
            #This is the mapping of the result topic to the completion event type.
            completion_event_type = RESULTS_ROUTING.get(topic)
            if not completion_event_type:
                logger.warning(f"No completion event mapping for topic: {topic}")
                return
            
            # Create completion event for Conductor
            #This is the completion event that is sent to conductor
            completion_event = {
                'workflowId': workflow_id,
                'taskId': task_id,
                'eventType': completion_event_type,
                'data': {
                    'result': data.get('result', 'success'),
                    'processedRecords': data.get('processedRecords', 0),
                    'failedRecords': data.get('failedRecords', 0),
                    'outputData': data.get('outputData', ''),
                    'pipelineStage': data.get('pipelineStage', 'unknown'),
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Send completion event back to Conductor
            self.send_to_conductor(completion_event)
            
            # Update pipeline state
            if workflow_id in self.pipeline_state:
                self.pipeline_state[workflow_id]['stages_completed'].append(completion_event_type)
                self.pipeline_state[workflow_id]['waiting_for'] = None
                logger.info(f"✅ Updated pipeline state for workflow: {workflow_id}")
            
        except Exception as e:
            logger.error(f"❌ Error processing result event: {e}", exc_info=True)
    
    def send_to_conductor(self, event):
        """Send completion event back to Conductor via conductor-events topic"""
        try:
            self.kafka_producer.send('conductor-events', event)
            self.kafka_producer.flush()
            logger.info(f"✅ Sent completion event to Conductor: {event.get('eventType')} for workflow: {event.get('workflowId')}")
        except Exception as e:
            logger.error(f"❌ Error sending to Conductor: {e}")
    
    def route_to_email_validation(self, event):
        """Route to email validation stage with MinIO file processing"""
        try:
            #This is the workflow id and data that is sent from the Conductor to the Event Router.
            workflow_id = event.get('workflowId')
            data = event.get('data', {})
            
            # Extract MinIO file information
            input_bucket = data.get('input_bucket', 'raw-data')
            input_key = data.get('input_key', 'customer_data_sample.csv')
            output_bucket = data.get('output_bucket', 'email-validated')
            output_key = data.get('output_key', f'email_validated_{workflow_id}.csv')
            
            # Fix null values in file names
            if output_key and 'null' in output_key:
                output_key = f'email_validated_{workflow_id}.csv'
            
            # Debug logging
            logger.info(f"🔍 Email validation routing - workflow_id: {workflow_id}")
            logger.info(f"🔍 Email validation routing - input_key: {input_key}")
            logger.info(f"🔍 Email validation routing - output_key: {output_key}")
            logger.info(f"🔍 Email validation routing - data keys: {list(data.keys())}")
            
            # Create email validation request with MinIO file info
            email_request = {
                'workflowId': workflow_id,
                'taskId': 'email_validation_task',
                'eventType': 'email_validation_request',
                'data': {
                    'input_bucket': input_bucket,
                    'input_key': input_key,
                    'output_bucket': output_bucket,
                    'output_key': output_key,
                    'pipelineStage': 'email_validation',
                    'records': data.get('records', 1000),
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Send to email validation topic
            self.kafka_producer.send('email-validation-requests', email_request)
            self.kafka_producer.flush()
            
            # Update pipeline state
            if workflow_id not in self.pipeline_state:
                self.pipeline_state[workflow_id] = {
                    'stages_completed': [],
                    'waiting_for': 'email_validation_completed',
                    'data_flow': {},
                    'file_flow': {
                        'current_bucket': output_bucket,
                        'current_key': output_key
                    }
                }
            else:
                self.pipeline_state[workflow_id]['waiting_for'] = 'email_validation_completed'
                self.pipeline_state[workflow_id]['file_flow'] = {
                    'current_bucket': output_bucket,
                    'current_key': output_key
                }
            
            logger.info(f"✅ Routed to email validation for workflow: {workflow_id}")
            logger.info(f"📁 Input: {input_bucket}/{input_key}")
            logger.info(f"📁 Output: {output_bucket}/{output_key}")
            
        except Exception as e:
            logger.error(f"❌ Error routing to email validation: {e}")
    
    def route_to_phone_validation(self, event):
        """Route to phone validation stage with MinIO file processing"""
        try:
            #This is the workflow id and data that is sent from the Conductor to the Event Router
            workflow_id = event.get('workflowId')
            data = event.get('data', {})
            
            # Extract MinIO file information
            input_bucket = data.get('input_bucket', 'email-validated')
            input_key = data.get('input_key', f'email_validated_{workflow_id}.csv')
            output_bucket = data.get('output_bucket', 'phone-validated')
            output_key = data.get('output_key', f'phone_validated_{workflow_id}.csv')
            
            # Fix null values in file names
            if input_key and 'null' in input_key:
                input_key = f'email_validated_{workflow_id}.csv'
            if output_key and 'null' in output_key:
                output_key = f'phone_validated_{workflow_id}.csv'
            
            # Debug logging
            logger.info(f"🔍 Phone validation routing - workflow_id: {workflow_id}")
            logger.info(f"🔍 Phone validation routing - input_key: {input_key}")
            logger.info(f"🔍 Phone validation routing - output_key: {output_key}")
            
            # Create phone validation request with MinIO file info
            phone_request = {
                'workflowId': workflow_id,
                'taskId': 'phone_validation_task',
                'eventType': 'phone_validation_request',
                'data': {
                    'input_bucket': input_bucket,
                    'input_key': input_key,
                    'output_bucket': output_bucket,
                    'output_key': output_key,
                    'pipelineStage': 'phone_validation',
                    'previousStage': 'email_validation',
                    'records': data.get('records', 1000),
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Send to phone validation topic
            #This is the actual request that is sent in to the Kafka topic.
            self.kafka_producer.send('phone-validation-requests', phone_request)
            self.kafka_producer.flush()
            
            # Update pipeline state
            #This is the update of the pipeline state for the workflow id.
            if workflow_id in self.pipeline_state:
                self.pipeline_state[workflow_id]['waiting_for'] = 'phone_validation_completed'
                self.pipeline_state[workflow_id]['file_flow'] = {
                    'current_bucket': output_bucket,
                    'current_key': output_key
                }
            
            logger.info(f"✅ Routed to phone validation for workflow: {workflow_id}")
            logger.info(f"📁 Input: {input_bucket}/{input_key}")
            logger.info(f"📁 Output: {output_bucket}/{output_key}")
            
        except Exception as e:
            logger.error(f"❌ Error routing to phone validation: {e}")

    
    def route_to_enrichment(self, event):
        """Route to enrichment stage with MinIO file processing"""
        try:
            #This is the workflow id and data that is sent from the Conductor to the Event Router.
            workflow_id = event.get('workflowId')
            data = event.get('data', {})
            
            # Extract MinIO file information
            input_bucket = data.get('input_bucket', 'phone-validated')
            input_key = data.get('input_key', f'phone_validated_{workflow_id}.csv')
            output_bucket = data.get('output_bucket', 'enriched')
            output_key = data.get('output_key', f'enriched_{workflow_id}.csv')
            
            # Fix null values in file names
            if input_key and 'null' in input_key:
                input_key = f'phone_validated_{workflow_id}.csv'
            if output_key and 'null' in output_key:
                output_key = f'enriched_{workflow_id}.csv'
            
            # Debug logging
            logger.info(f"🔍 Enrichment routing - workflow_id: {workflow_id}")
            logger.info(f"🔍 Enrichment routing - input_key: {input_key}")
            logger.info(f"🔍 Enrichment routing - output_key: {output_key}")
            
            # Create enrichment request with MinIO file info
            enrichment_request = {
                'workflowId': workflow_id,
                'taskId': 'enrichment_task',
                'eventType': 'enrichment_request',
                'data': {
                    'input_bucket': input_bucket,
                    'input_key': input_key,
                    'output_bucket': output_bucket,
                    'output_key': output_key,
                    'pipelineStage': 'enrichment',
                    'previousStage': 'phone_validation',
                    'records': data.get('records', 1000),
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Send to enrichment topic
            self.kafka_producer.send('enrichment-requests', enrichment_request)
            self.kafka_producer.flush()
            
            # Update pipeline state
            if workflow_id in self.pipeline_state:
                self.pipeline_state[workflow_id]['waiting_for'] = 'enrichment_completed'
                self.pipeline_state[workflow_id]['file_flow'] = {
                    'current_bucket': output_bucket,
                    'current_key': output_key
                }
            
            logger.info(f"✅ Routed to enrichment for workflow: {workflow_id}")
            logger.info(f"📁 Input: {input_bucket}/{input_key}")
            logger.info(f"📁 Output: {output_bucket}/{output_key}")
            
        except Exception as e:
            logger.error(f"❌ Error routing to enrichment: {e}")
    
    def route_to_airflow_trigger(self, event):
        """Route to Airflow adapter service to trigger DAG run"""
        try:
            workflow_id = event.get('workflowId')
            data = event.get('data', {})
            
            # Extract Airflow configuration from event data
            dag_id = data.get('dag_id', 'nua-nameparse-process-stage-v02-00-06-tiny')
            execution_id = data.get('execution_id', 'WBNameParse')
            metadata_url = data.get('metadata_url')
            jobid = data.get('jobid') or workflow_id or f"job-{int(time.time())}"
            
            logger.info(f"🔍 Airflow trigger routing - workflow_id: {workflow_id}")
            logger.info(f"🔍 Airflow trigger routing - dag_id: {dag_id}")
            logger.info(f"🔍 Airflow trigger routing - jobid: {jobid}")
            
            # Create Airflow trigger request
            airflow_request = {
                'workflowId': workflow_id,
                'taskId': 'airflow_trigger_task',
                'eventType': 'airflow_trigger_request',
                'data': {
                    'dag_id': dag_id,
                    'execution_id': execution_id,
                    'metadata_url': metadata_url,
                    'jobid': jobid,
                    'pipelineStage': 'airflow_processing',
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Send to airflow trigger requests topic
            self.kafka_producer.send('airflow-trigger-requests', airflow_request)
            self.kafka_producer.flush()
            
            # Update pipeline state
            if workflow_id not in self.pipeline_state:
                self.pipeline_state[workflow_id] = {
                    'stages_completed': [],
                    'waiting_for': 'airflow_dag_completed',
                    'data_flow': {}
                }
            else:
                self.pipeline_state[workflow_id]['waiting_for'] = 'airflow_dag_completed'
            
            logger.info(f"✅ Routed to Airflow adapter for workflow: {workflow_id}")
            logger.info(f"   DAG ID: {dag_id}, Job ID: {jobid}")
            
        except Exception as e:
            logger.error(f"❌ Error routing to Airflow adapter: {e}", exc_info=True)
    
    def route_to_completion(self, event):
        """Route to pipeline completion with MinIO file processing"""
        #This is the workflow id and data that is sent from Conductor to the Event Router.
        try:
            workflow_id = event.get('workflowId')
            data = event.get('data', {})
            
            # Extract final file information
            final_bucket = data.get('final_bucket', 'enriched')
            final_key = data.get('final_key', f'enriched_{workflow_id}.csv')
            
            # Create completion event
            #This is the completion event that is sent to the completion microservice to the Kafka topic.
            completion_event = {
                'workflowId': workflow_id,
                'taskId': 'pipeline_completion_task',
                'eventType': 'pipeline_completed',
                'data': {
                    'final_bucket': final_bucket,
                    'final_key': final_key,
                    'records': data.get('records', 1000),
                    'pipelineStage': 'completed',
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Send to completion topic
            #This is the actual request that is sent into the Kafka topic.
            self.kafka_producer.send('pipeline-completion', completion_event)
            self.kafka_producer.flush()
            
            logger.info(f"✅ Pipeline completed for workflow: {workflow_id}")
            logger.info(f"📁 Final file: {final_bucket}/{final_key}")
            
        except Exception as e:
            logger.error(f"❌ Error routing to completion: {e}")
    
    def consume_conductor_events(self):
        """Consume events from Conductor and route them to microservices"""
        consumer = KafkaConsumer(
            'conductor-events',
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: self._safe_deserialize(m),
            auto_offset_reset='earliest',
            group_id='enhanced-router-conductor-group',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000
        )
        
        logger.info("Enhanced Event Router started - listening for conductor events")
        
        for message in consumer:
            if not self.running:
                break
                
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
    
    def consume_result_events(self):
        """Consume result events from microservices and route them back to Conductor"""
        """Here we are consuming the result events from microservices and routing them back to the Conductor.  """
        consumer = KafkaConsumer(
            'email-validation-results',
            'phone-validation-results', 
            'enrichment-results',
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: self._safe_deserialize(m),
            auto_offset_reset='earliest',
            group_id='enhanced-router-results-group',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000
        )
        
        logger.info("Enhanced Event Router started - listening for microservice results")
        
        for message in consumer:
            if not self.running:
                break
                
            try:
                event = message.value
                topic = message.topic
                
                # Skip if event is None or empty
                if not event:
                    logger.warning("Received empty or null result event, skipping")
                    continue
                
                # Handle both string and dict formats
                if isinstance(event, str):
                    if not event.strip().startswith('{'):
                        logger.info(f"Received non-JSON string result event (skipping): {event[:50]}...")
                        continue
                    try:
                        event = json.loads(event)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON string: {event[:100]}... Error: {e}")
                        continue
                
                # Ensure event is a dictionary
                if not isinstance(event, dict):
                    logger.warning(f"Received non-dict result event: {type(event)} - {str(event)[:100]}...")
                    continue
                
                logger.info(f"Received result event from {topic}: {event.get('eventType', 'unknown')}")
                
                # Process result events
                self.process_result_event(event, topic)
                    
            except Exception as e:
                logger.error(f"Error processing result event: {e}", exc_info=True)
    
    def start(self):
        """Start the enhanced event router with bidirectional communication"""
        """The start() method is the main entry point that initializes and run Enhanced Sequential Event Router Service 
        It sets up the bidirectional communication between the Conductor and microservices."""
        logger.info("Starting Enhanced Sequential Event Router Service")
        logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
        logger.info(f"Conductor API: {CONDUCTOR_API_URL}")
        logger.info(f"Service routing: {SERVICE_ROUTING}")
        logger.info(f"Results routing: {RESULTS_ROUTING}")
        
        # Wait for services to be ready
        logger.info("Waiting 10 seconds for services to initialize...")
        time.sleep(10)
        
        # Start both consumers in separate threads
        # It Runs both the consumers in separate threads.
        #Thread 1: Listens to the conductor-events topics for pipeline events
        #Thread 2: Listens to the result topics for result events from microservices
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit both consumer functions

            conductor_future = executor.submit(self.consume_conductor_events)
            results_future = executor.submit(self.consume_result_events)
            
            try:
                # Wait for both consumers to complete
                conductor_future.result()
                results_future.result()
            except KeyboardInterrupt:
                logger.info("Shutting down Enhanced Event Router...")
                self.running = False
                
                conductor_future.cancel()
                results_future.cancel()

def main():
    """Start the Enhanced Sequential Event Router"""
    router = EnhancedSequentialEventRouter()
    router.start()

if __name__ == '__main__':
    main()
