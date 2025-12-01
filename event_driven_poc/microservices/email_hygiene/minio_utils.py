"""
MinIO utility functions for downloading files
"""
from minio import Minio
from minio.error import S3Error
from config import Config
import os
from typing import Optional, Tuple


class MinIOManager:
    """Manages MinIO operations for the ingestion service"""

    """This is the MINIO client that is used to interact with the MINIO server."""
    def __init__(self):
        """Initialize MinIO client"""
        self.client = Minio(
            Config.MINIO_ENDPOINT,
            access_key=Config.MINIO_ACCESS_KEY,
            secret_key=Config.MINIO_SECRET_KEY,
            secure=Config.MINIO_USE_SSL
        )
    
    """This is the function that is used to parse the MINIO URI and extract the bucket and object key."""
    def parse_minio_uri(self, uri: str) -> Tuple[str, str]:
        """
        Parse MinIO URI to extract bucket and object key
        
        Args:
            uri: MinIO URI in format "minio://bucket/path/to/file" or "bucket/path/to/file"
            
        Returns:
            Tuple of (bucket, object_key)
        """
        # Remove minio:// prefix if present
        if uri.startswith("minio://"):
            uri = uri[8:]
        
        # Split into bucket and key
        parts = uri.split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid MinIO URI format: {uri}. Expected format: minio://bucket/path/to/file")
        
        bucket = parts[0]
        object_key = parts[1]
        
        return bucket, object_key

    """This is the function that is used to download the MINIO file to teh local path."""
    def download_file(self, minio_uri: str, local_path: str) -> str:
        """
        Download file from MinIO to local path
        
        Args:
            minio_uri: MinIO URI (e.g., "minio://bucket/path/to/file")
            local_path: Local path where to save the file
            
        Returns:
            Local file path
        """
        try:
            bucket, object_key = self.parse_minio_uri(minio_uri)
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Download file
            self.client.fget_object(bucket, object_key, local_path)
            
            return local_path
        except S3Error as e:
            raise Exception(f"Failed to download {minio_uri} from MinIO: {str(e)}")
        except Exception as e:
            raise Exception(f"Error downloading from MinIO: {str(e)}")
    
    def file_exists(self, minio_uri: str) -> bool:
        """
        Check if file exists in MinIO
        
        Args:
            minio_uri: MinIO URI to check
            
        Returns:
            True if file exists, False otherwise
        """
        """This is the function that is used to check if the file exists in the user or not """
        try:
            bucket, object_key = self.parse_minio_uri(minio_uri)
            self.client.stat_object(bucket, object_key)
            return True
        except S3Error:
            return False
        except Exception:
            return False
    
    def upload_file(self,local_path,bucket,object_key):
        self.client.fput_object(bucket,object_key,local_path)
        return f"minio://{bucket}/{object_key}"

