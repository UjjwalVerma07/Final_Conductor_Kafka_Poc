"""
Simple test script to verify Phase 1 setup
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from config import Config
    print("✓ Config module imported successfully")
    
    # Test configuration (will fail validation but that's expected without env vars)
    print(f"✓ S3 Bucket: {Config.S3_BUCKET}")
    print(f"✓ S3 Prefix: {Config.S3_PREFIX}")
    print(f"✓ Full S3 Path: s3://{Config.S3_BUCKET}/{Config.S3_PREFIX}/")
    print(f"✓ MinIO Endpoint: {Config.MINIO_ENDPOINT}")
    print(f"✓ Conductor URL: {Config.CONDUCTOR_SERVER_URL}")
    print("\n✓ Phase 1 setup complete!")
    print("\nNext steps:")
    print("1. Set up environment variables (AWS credentials, etc.)")
    print("2. Install dependencies: pip install -r requirements.txt")
    print("3. Test the service: python main.py")
    
except Exception as e:
    print(f"✗ Error: {e}")
    sys.exit(1)

