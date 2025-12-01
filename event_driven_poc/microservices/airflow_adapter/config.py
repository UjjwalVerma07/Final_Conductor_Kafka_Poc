"""
Configuration management for Ingestion Service
"""
import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Application configuration""" 
    
    # MinIO Configuration
    #These are the MINIO credential for the MINIO server that is running in the docker-compose.yaml file
    # MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000") 
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000") 
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    MINIO_USE_SSL = os.getenv("MINIO_USE_SSL", "false").lower() == "true"
    
    # AWS S3 Configuration
    #These are the AWS credential for the AWS S3 bucket that is running in the AWS account that is running the event driven poc
    # IMPORTANT: Only use the company-allotted bucket: 958825666686-dpservices-testing-data
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")  # Optional, for temporary credentials
    AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
    
    # Company-allotted bucket - DO NOT CHANGE
    ALLOWED_S3_BUCKET = "958825666686-dpservices-testing-data"
    ALLOWED_S3_PREFIX = "conductor-poc"
    
    S3_BUCKET = os.getenv("S3_BUCKET", ALLOWED_S3_BUCKET)
    S3_PREFIX = os.getenv("S3_PREFIX", ALLOWED_S3_PREFIX)  # Base prefix: conductor-poc
    
    # Conductor Configuration
    #This is the URL of the Conductor server URL that is the running in the docker-compose.yaml file
    CONDUCTOR_SERVER_URL = os.getenv("CONDUCTOR_SERVER_URL", "http://conductor-server:8080/api")
    
    # Local Storage
    #This is the local temporary directory that is used to store the files that are downloaded from the MINIO server.
    LOCAL_TEMP_DIR = os.getenv("LOCAL_TEMP_DIR", "/tmp/ingestion_service")
    
    # Workflow Templates
    #This is the directory that contains the workflow templates that are used to create the workflows 
    WORKFLOW_TEMPLATES_DIR = os.getenv("WORKFLOW_TEMPLATES_DIR", "./templates")
    SERVICE_TEMPLATES_DIR = os.getenv("SERVICE_TEMPLATES_DIR", "./templates/services")
    
    @classmethod
    def validate(cls):
        """Validate required configuration and enforce company-allotted bucket"""
        errors = []
        
        if not cls.AWS_ACCESS_KEY_ID:
            errors.append("AWS_ACCESS_KEY_ID is required")
        if not cls.AWS_SECRET_ACCESS_KEY:
            errors.append("AWS_SECRET_ACCESS_KEY is required")
        if not cls.S3_BUCKET:
            errors.append("S3_BUCKET is required")
        
        # Enforce company-allotted bucket only
        if cls.S3_BUCKET != cls.ALLOWED_S3_BUCKET:
            errors.append(
                f"S3_BUCKET must be '{cls.ALLOWED_S3_BUCKET}' (company-allotted bucket). "
                f"Current value: '{cls.S3_BUCKET}'"
            )
        
        # Enforce conductor-poc prefix
        if cls.S3_PREFIX != cls.ALLOWED_S3_PREFIX:
            errors.append(
                f"S3_PREFIX must be '{cls.ALLOWED_S3_PREFIX}'. "
                f"Current value: '{cls.S3_PREFIX}'"
            )
        
        if errors:
            raise ValueError(f"Configuration errors: {', '.join(errors)}")
        
        return True

