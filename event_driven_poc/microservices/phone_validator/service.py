#!/usr/bin/env python3
"""
Phone Validator Service - MinIO File Processing
Processes CSV files from MinIO, validates phone numbers, uploads results
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
SERVICE_NAME = os.getenv('SERVICE_NAME', 'phone-validator')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'phone-validation')

# Initialize Kafka producer
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

class PhoneValidatorService:
    """MinIO-based phone validation service"""
    
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
                logger.info(f" Created bucket: {MINIO_BUCKET}")
        except S3Error as e:
            logger.error(f" Error creating bucket: {e}")
    
    def _ensure_output_bucket_exists(self, bucket_name):
        """Ensure output bucket exists"""
        try:
            if not self.minio_client.bucket_exists(bucket_name):
                self.minio_client.make_bucket(bucket_name)
                logger.info(f"Created output bucket: {bucket_name}")
            else:
                logger.info(f"Output bucket exists: {bucket_name}")
        except S3Error as e:
            logger.error(f" Error creating output bucket: {e}")
    
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
            logger.error(f" Error downloading file {bucket}/{key}: {e}")
            raise
    
    def upload_file(self, bucket, key, data):
        """Upload file to MinIO"""
        try:
            data_stream = io.BytesIO(data)
            self.minio_client.put_object(
                bucket, key, data_stream, len(data)
            )
            logger.info(f" Uploaded file: {bucket}/{key}")
        except S3Error as e:
            logger.error(f"Error uploading file {bucket}/{key}: {e}")
            raise
    
    def validate_phone(self, phone):
        """Validate phone number (US format)"""
        """Basic Phone Validation Logic - can be enhanced as needed"""
        # Remove all non-digit characters
        digits = re.sub(r'\D', '', str(phone))
        
        # Check if it's a valid US phone number (10 digits)
        if len(digits) == 10:
            return True
        elif len(digits) == 11 and digits.startswith('1'):
            return True
        else:
            return False
    
    def process_csv_file(self, csv_data):
        """Process CSV file and validate phone numbers"""
        try:
            # Read CSV from bytes
            df = pd.read_csv(io.BytesIO(csv_data))
            logger.info(f" Processing CSV with {len(df)} rows")
            
            # Assume phone column is named 'phone'
            if 'phone' not in df.columns:
                logger.error("No 'phone' column found in CSV")
                return None, 0, 0
            
            # Validate phone numbers
            df['phone_valid'] = df['phone'].apply(self.validate_phone)
            
            # Calculate statistics
            total_records = len(df)
            valid_phones = df['phone_valid'].sum()
            invalid_phones = total_records - valid_phones
            
            # Filter valid phones for output
            valid_df = df[df['phone_valid'] == True].drop('phone_valid', axis=1)
            
            # Convert back to CSV
            output_csv = valid_df.to_csv(index=False)
            
            logger.info(f" Phone validation completed: {valid_phones}/{total_records} valid")
            return output_csv.encode('utf-8'), valid_phones, invalid_phones
            
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
            if output_key and 'null' in output_key:
                output_key = f'phone_validated_{workflow_id}.csv'
            
            logger.info(f" Processing phone validation for workflow {workflow_id}")
            logger.info(f" Input: {input_bucket}/{input_key}")
            logger.info(f" Output: {output_bucket}/{output_key}")
            
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
            
            # Publish result event
            result_event = {
                "workflowId": workflow_id,
                "taskId": task_id,
                "eventType": "phone_validation_completed",
                "data": {
                    "input_bucket": input_bucket,
                    "input_key": input_key,
                    "output_bucket": output_bucket,
                    "output_key": output_key,
                    "result": "success",
                    "processedRecords": int(valid_count),
                    "failedRecords": int(invalid_count),
                    "pipelineStage": "phone_validation",
                    "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Publish directly to Conductor
            self.kafka_producer.send('conductor-events', result_event)
            """Publish to this Kafka topic also we will update it later"""
            #self.kafka_producer.send('phone-validation-results', result_event)
            self.kafka_producer.flush()
            
            logger.info(f" Phone validation completed: {valid_count} valid, {invalid_count} invalid")
            
        except Exception as e:
            logger.error(f" Error processing phone validation: {e}", exc_info=True)
            
            # Publish failure event
            failure_event = {
                "workflowId": event.get('workflowId', 'unknown'),
                "taskId": event.get('taskId', 'unknown'),
                "eventType": "phone_validation_completed",
                "data": {
                    "result": "failure",
                    "error": str(e),
                    "pipelineStage": "phone_validation",
                    "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Publish failure directly to Conductor
            #Here also we are directly publishing to the Conductor events topic
            self.kafka_producer.send('conductor-events', failure_event)
            self.kafka_producer.flush()
    
    def consume_task_events(self):
        """Consume task events from Kafka"""
        consumer = KafkaConsumer(
            'phone-validation-requests',
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id=f'{SERVICE_NAME}-group'
        )
        
        logger.info(f"{SERVICE_NAME} started - listening for phone validation events")
        
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
                #And if it is already a json object then we can directly process it 
                event_type = event.get('eventType', 'unknown')
                
                if event_type == 'phone_validation_request':
                    logger.info(f"Processing phone validation request")
                    self.process_task_event(event)
                else:
                    logger.warning(f"Ignoring event type: {event_type}")
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)

def main():
    """Start the Phone Validator Service"""
    logger.info(f" Starting {SERVICE_NAME} Service")
    logger.info(f" Kafka: {KAFKA_BOOTSTRAP}")
    logger.info(f"MinIO: {MINIO_ENDPOINT}")
    
    # Wait for services to be ready
    logger.info(" Waiting 10 seconds for services to initialize...")
    time.sleep(10)
    
    # Start service
    service = PhoneValidatorService()
    service.consume_task_events()

if __name__ == '__main__':
    main()