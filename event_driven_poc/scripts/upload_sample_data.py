#!/usr/bin/env python3
"""
Sample Data Upload Script
Uploads sample CSV data to MinIO for testing the event-driven pipeline
"""

import os
import json
import time
import logging
import pandas as pd
import random
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
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
RAW_DATA_BUCKET = 'raw-data'

# Sample data configuration
SAMPLE_RECORDS = 1000
SAMPLE_FILENAME = 'customer_data_sample.csv'

class SampleDataUploader:
    """Upload sample data to MinIO for testing"""
    
    def __init__(self):
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
            if not self.minio_client.bucket_exists(RAW_DATA_BUCKET):
                self.minio_client.make_bucket(RAW_DATA_BUCKET)
                logger.info(f"✅ Created bucket: {RAW_DATA_BUCKET}")
            else:
                logger.info(f"✅ Bucket exists: {RAW_DATA_BUCKET}")
        except S3Error as e:
            logger.error(f"❌ Error creating bucket: {e}")
            raise
    
    def generate_sample_data(self):
        """Generate sample customer data"""
        logger.info(f"📊 Generating {SAMPLE_RECORDS} sample records...")
        
        # Sample data lists
        first_names = [
            'John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 'Jessica',
            'William', 'Ashley', 'James', 'Amanda', 'Christopher', 'Jennifer', 'Daniel',
            'Lisa', 'Matthew', 'Nancy', 'Anthony', 'Karen', 'Mark', 'Betty', 'Donald',
            'Helen', 'Steven', 'Sandra', 'Paul', 'Donna', 'Andrew', 'Carol'
        ]
        
        last_names = [
            'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller',
            'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez',
            'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin'
        ]
        
        cities = [
            'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia',
            'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville',
            'Fort Worth', 'Columbus', 'Charlotte', 'San Francisco', 'Indianapolis',
            'Seattle', 'Denver', 'Washington', 'Boston', 'El Paso', 'Nashville',
            'Detroit', 'Oklahoma City', 'Portland', 'Las Vegas', 'Memphis', 'Louisville'
        ]
        
        # Generate sample data
        data = []
        for i in range(1, SAMPLE_RECORDS + 1):
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            city = random.choice(cities)
            
            # Generate email (some valid, some invalid for testing)
            if random.random() < 0.85:  # 85% valid emails
                email = f"{first_name.lower()}.{last_name.lower()}{i}@example.com"
            else:  # 15% invalid emails
                email = f"invalid_email_{i}"  # Invalid format
            
            # Generate phone (some valid, some invalid for testing)
            if random.random() < 0.80:  # 80% valid phones
                phone = f"{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            else:  # 20% invalid phones
                phone = f"invalid_phone_{i}"  # Invalid format
            
            record = {
                'serial_no': i,
                'name': f"{first_name} {last_name}",
                'email': email,
                'phone': phone,
                'city': city
            }
            data.append(record)
        
        # Create DataFrame
        df = pd.DataFrame(data)
        logger.info(f"✅ Generated {len(df)} sample records")
        return df
    
    def upload_csv_to_minio(self, df, bucket, key):
        """Upload DataFrame as CSV to MinIO"""
        try:
            # Convert DataFrame to CSV
            csv_data = df.to_csv(index=False)
            csv_bytes = csv_data.encode('utf-8')
            
            # Upload to MinIO
            data_stream = io.BytesIO(csv_bytes)
            self.minio_client.put_object(
                bucket, key, data_stream, len(csv_bytes)
            )
            
            logger.info(f"✅ Uploaded CSV to MinIO: {bucket}/{key}")
            logger.info(f"📊 File size: {len(csv_bytes)} bytes")
            
        except S3Error as e:
            logger.error(f"❌ Error uploading CSV to MinIO: {e}")
            raise
    
    def verify_upload(self, bucket, key):
        """Verify the uploaded file"""
        try:
            # Get object info
            stat = self.minio_client.stat_object(bucket, key)
            logger.info(f"✅ File verified: {bucket}/{key}")
            logger.info(f"📊 File size: {stat.size} bytes")
            logger.info(f"📅 Last modified: {stat.last_modified}")
            
            # Download and verify first few rows
            response = self.minio_client.get_object(bucket, key)
            data = response.read()
            response.close()
            response.release_conn()
            
            # Read CSV and show sample
            df = pd.read_csv(io.BytesIO(data))
            logger.info(f"📊 Sample data preview:")
            logger.info(f"   Total records: {len(df)}")
            logger.info(f"   Columns: {list(df.columns)}")
            logger.info(f"   First 3 rows:")
            logger.info(f"   {df.head(3).to_string()}")
            
            return True
            
        except S3Error as e:
            logger.error(f"❌ Error verifying upload: {e}")
            return False
    
    def upload_sample_data(self):
        """Main function to upload sample data"""
        try:
            logger.info("🚀 Starting sample data upload process")
            logger.info(f"🔌 MinIO: {MINIO_ENDPOINT}")
            logger.info(f"📦 Bucket: {RAW_DATA_BUCKET}")
            
            # Generate sample data
            df = self.generate_sample_data()
            
            # Upload to MinIO
            self.upload_csv_to_minio(df, RAW_DATA_BUCKET, SAMPLE_FILENAME)
            
            # Verify upload
            if self.verify_upload(RAW_DATA_BUCKET, SAMPLE_FILENAME):
                logger.info("🎉 Sample data upload completed successfully!")
                logger.info(f"📁 File location: {RAW_DATA_BUCKET}/{SAMPLE_FILENAME}")
                logger.info(f"📊 Records: {SAMPLE_RECORDS}")
                return True
            else:
                logger.error("❌ Sample data upload verification failed")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error in sample data upload: {e}", exc_info=True)
            return False

def main():
    """Main function"""
    logger.info("📊 Sample Data Upload Script")
    logger.info("=" * 50)
    
    # Check MinIO connection
    try:
        uploader = SampleDataUploader()
        logger.info("✅ MinIO connection successful")
    except Exception as e:
        logger.error(f"❌ Cannot connect to MinIO: {e}")
        logger.error("💡 Make sure MinIO is running: docker-compose up -d")
        return False
    
    # Upload sample data
    success = uploader.upload_sample_data()
    
    if success:
        logger.info("🎉 Sample data upload completed successfully!")
        logger.info("💡 You can now test the event-driven pipeline with real data")
        return True
    else:
        logger.error("❌ Sample data upload failed")
        return False

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
