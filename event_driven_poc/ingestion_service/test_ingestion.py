"""
Test script for ingestion service functionality
"""
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ingestion_service import IngestionService
from config import Config

def test_ingestion():
    """Test the ingestion service"""
    print("Testing Ingestion Service...")
    print(f"S3 Bucket: {Config.S3_BUCKET}")
    print(f"S3 Prefix: {Config.S3_PREFIX}")
    print()
    
    # Example usage (commented out - uncomment when ready to test)
    """
    service = IngestionService()
    
    result = service.ingest_file_and_update_json(
        minio_input_uri="minio://your-bucket/path/to/file.csv",
        source_json_s3_key="conductor-poc/1000861509.WBNameParse.json",
        run_id="test-run-001"
    )
    
    print("Ingestion completed!")
    print(f"Run ID: {result['run_id']}")
    print(f"Input URI: {result['s3_input_uri']}")
    print(f"Output URI: {result['s3_output_uri']}")
    print(f"Updated JSON URI: {result['updated_json_uri']}")
    """
    
    print("✓ Ingestion service module loaded successfully")
    print("\nTo test:")
    print("1. Set AWS credentials in environment")
    print("2. Set MinIO credentials in environment")
    print("3. Uncomment test code above")
    print("4. Run: python test_ingestion.py")

if __name__ == "__main__":
    test_ingestion()

