#!/usr/bin/env python3
"""
Email Validator Service - MinIO File Processing
Processes CSV files from MinIO, validates emails, uploads results
"""

import os
import json
import time
import logging
import pandas as pd
import re
from kafka import KafkaConsumer, KafkaProducer
from minio import Minio
from minio.error import S3Error
import io

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
SERVICE_NAME = os.getenv('SERVICE_NAME', 'email-validator')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'email-validation')

# Initialize Kafka producer
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

class EmailValidatorService:
    """MinIO-based email validation service"""
    
    def __init__(self):
        self.kafka_producer = kafka_producer
        self.minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        """Ensure MinIO bucket exists"""
        try:
            if not self.minio_client.bucket_exists(MINIO_BUCKET):
                self.minio_client.make_bucket(MINIO_BUCKET)
                logger.info(f"Created bucket: {MINIO_BUCKET}")
            else:
                logger.info(f"Bucket exists: {MINIO_BUCKET}")
        except S3Error as e:
            logger.error(f"Error creating bucket: {e}")
    
    def _ensure_output_bucket_exists(self, bucket_name):
        """Ensure output bucket exists"""
        try:
            if not self.minio_client.bucket_exists(bucket_name):
                self.minio_client.make_bucket(bucket_name)
                logger.info(f"Created output bucket: {bucket_name}")
            else:
                logger.info(f"Output bucket exists: {bucket_name}")
        except S3Error as e:
            logger.error(f"Error creating output bucket: {e}")
    
    def download_file(self, bucket, key):
        """Download file from MinIO"""
        try:
            response = self.minio_client.get_object(bucket, key)
            data = response.read()
            response.close()
            response.release_conn()
            logger.info(f"Downloaded file: {bucket}/{key}")
            return data
        except S3Error as e:
            logger.error(f"Error downloading file {bucket}/{key}: {e}")
            raise
    
    def upload_file(self, bucket, key, data):
        """Upload file to MinIO"""
        try:
            data_stream = io.BytesIO(data)
            self.minio_client.put_object(
                bucket, key, data_stream, len(data)
            )
            logger.info(f"Uploaded file: {bucket}/{key}")
        except S3Error as e:
            logger.error(f"Error uploading file {bucket}/{key}: {e}")
            raise
    
    def validate_email(self, email):
        """Validate single email address"""
        #just a simple regex for the email validation can be enhanced as per requirement
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email) is not None
    
    def process_csv_file(self, csv_data):
        """Process CSV file and validate emails"""
        try:
            # Read CSV from bytes
            df = pd.read_csv(io.BytesIO(csv_data))
            logger.info(f"Processing CSV with {len(df)} rows")
            
            # Assume email column is named 'email'
            if 'email' not in df.columns:
                logger.error("No 'email' column found in CSV")
                return None, 0, 0
            
            # Validate emails
            df['email_valid'] = df['email'].apply(self.validate_email)
            
            # Calculate statistics
            total_records = len(df)
            valid_emails = df['email_valid'].sum()
            invalid_emails = total_records - valid_emails
            
            # Filter valid emails for output
            valid_df = df[df['email_valid'] == True].drop('email_valid', axis=1)
            
            # Convert back to CSV
            output_csv = valid_df.to_csv(index=False)
            
            logger.info(f"Email validation completed: {valid_emails}/{total_records} valid")
            return output_csv.encode('utf-8'), valid_emails, invalid_emails
            
        except Exception as e:
            logger.error(f"Error processing CSV: {e}")
            raise
    
    def process_task_event(self, event):
        """Process task event with MinIO file operations"""
        try:
            workflow_id = event.get('workflowId')
            task_id = event.get('taskId')
            data = event.get('data', {})
            
            # Extract MinIO file information
            input_bucket = data.get('input_bucket')
            input_key = data.get('input_key')
            output_bucket = data.get('output_bucket')
            output_key = data.get('output_key')
            
            # Fix null values in output_key by using workflow_id
            logger.info(f"Before fix - output_key: {output_key}")
            if output_key and 'null' in output_key:
                output_key = f'email_validated_{workflow_id}.csv'
                logger.info(f"Fixed output_key to: {output_key}")
            else:
                logger.info(f"No fix needed - output_key: {output_key}")
            
            logger.info(f"Processing email validation for workflow {workflow_id}")
            logger.info(f"Input: {input_bucket}/{input_key}")
            logger.info(f"Output: {output_bucket}/{output_key}")
            logger.info(f"Raw data received: {data}")
            
            # Sleep for 10 seconds to simulate processing time
            logger.info("Sleeping for 10 seconds to simulate processing time...")
            time.sleep(10)
            logger.info("Sleep completed, continuing with processing...")
            
            # Ensure output bucket exists
            self._ensure_output_bucket_exists(output_bucket)
            
            # Download input file
            input_data = self.download_file(input_bucket, input_key)
            
            # Process CSV file
            output_data, valid_count, invalid_count = self.process_csv_file(input_data)
            
            # Upload processed file
            self.upload_file(output_bucket, output_key, output_data)
            
            # Publish result event to the Conductor events topic
            result_event = {
                       "workflowId": workflow_id,
                       "taskId": task_id,
                       "eventType": "email_validation_completed", #Here we are publishing the event Topic by directly specifying the sink 
                       "data": {
                           "input_bucket": input_bucket,
                           "input_key": input_key, 
                           "output_bucket": output_bucket,
                           "output_key": output_key,
                           "result": "success",
                           "processedRecords": int(valid_count),
                           "failedRecords": int(invalid_count),
                           "pipelineStage": "email_validation",
                           "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ')
                       }
                   }
            
            # Publish Failure event also to Conductor events topic
            try:
                future = self.kafka_producer.send(
                    'conductor-events',
                    key=workflow_id.encode('utf-8') if workflow_id else None,
                    value=result_event
                )
                
                # Wait for send to complete and log result
                record_metadata = future.get(timeout=10)
                logger.info(f"Published completion event to conductor-events for workflow {workflow_id}")
                logger.info(f"   Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            except Exception as e:
                logger.error(f"Error publishing event to conductor-events: {e}", exc_info=True)
            
            self.kafka_producer.flush()
            
            logger.info(f"Email validation completed: {valid_count} valid, {invalid_count} invalid")
            
        except Exception as e:
            logger.error(f"Error processing email validation: {e}", exc_info=True)
            
            # Publish failure event
            failure_event = {
                "workflowId": event.get('workflowId', 'unknown'),
                "taskId": event.get('taskId', 'unknown'),
                "eventType": "email_validation_completed", 
                "data": {
                    "result": "failure",
                    "error": str(e),
                    "pipelineStage": "email_validation",
                    "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Publish failure directly to Conductor
            #Here also we are dirctly publishing to the Conductor events topoic 
            try:
                workflow_id = event.get('workflowId', 'unknown')
                future = self.kafka_producer.send(
                    'conductor-events',
                    key=workflow_id.encode('utf-8') if workflow_id != 'unknown' else None,
                    value=failure_event
                )
                record_metadata = future.get(timeout=10)
                logger.info(f"Published failure event to conductor-events for workflow {workflow_id}")
            except Exception as e:
                logger.error(f"Error publishing failure event to conductor-events: {e}", exc_info=True)
            self.kafka_producer.flush()
    
    def consume_task_events(self):
        """Consume task events from Kafka"""
        consumer = KafkaConsumer(
            'email-validation-requests',
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id=f'{SERVICE_NAME}-group'
        )
        
        logger.info(f"{SERVICE_NAME} started - listening for email validation events")
        
        for message in consumer:
            try:
                event = message.value
                
                # Handle both JSON object and string cases
                if isinstance(event, str):
                    try:
                        event = json.loads(event)
                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse JSON string: {event}")
                        continue
                
                event_type = event.get('eventType', 'unknown')
                
                if event_type == 'email_validation_request':
                    logger.info(f"Processing email validation request")
                    self.process_task_event(event)
                else:
                    logger.warning(f"Ignoring event type: {event_type}")
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)

def main():
    """Start the Email Validator Service"""
    logger.info(f"Starting {SERVICE_NAME} Service")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
    logger.info(f"MinIO: {MINIO_ENDPOINT}")
    
    # Wait for services to be ready
    logger.info(" Waiting 10 seconds for services to initialize...")
    time.sleep(10)
    
    # Start service
    service = EmailValidatorService()
    service.consume_task_events()

if __name__ == '__main__':
    main()

