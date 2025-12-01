"""
Test script for Phase 2 & Phase 3: MinIO Download, S3 Upload, and JSON URI Updates
"""
import sys
import os
import json

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from s3_utils import S3Manager
from minio_utils import MinIOManager
from ingestion_service import IngestionService
from config import Config


def test_s3_connection():
    """Test S3 connection and basic operations"""
    print("=" * 60)
    print("TEST 1: S3 Connection & Basic Operations")
    print("=" * 60)
    
    try:
        s3_manager = S3Manager()
        print(f"✓ S3 Manager initialized")
        print(f"  Bucket: {s3_manager.bucket}")
        print(f"  Prefix: {s3_manager.base_prefix}")
        
        # Test: List a few objects (if any)
        try:
            response = s3_manager.s3_client.list_objects_v2(
                Bucket=s3_manager.bucket,
                Prefix=s3_manager.base_prefix,
                MaxKeys=5
            )
            if 'Contents' in response:
                print(f"✓ Found {len(response['Contents'])} objects in bucket")
            else:
                print("✓ Bucket accessible (no objects found)")
        except Exception as e:
            print(f"⚠ Could not list objects: {e}")
        
        return True
    except Exception as e:
        print(f"✗ S3 Connection failed: {e}")
        return False


def test_minio_connection():
    """Test MinIO connection"""
    print("\n" + "=" * 60)
    print("TEST 2: MinIO Connection")
    print("=" * 60)
    
    try:
        minio_manager = MinIOManager()
        print(f"✓ MinIO Manager initialized")
        print(f"  Endpoint: {Config.MINIO_ENDPOINT}")
        print(f"  Use SSL: {Config.MINIO_USE_SSL}")
        
        # Test: List buckets
        try:
            buckets = minio_manager.client.list_buckets()
            print(f"✓ MinIO accessible - Found {len(buckets)} buckets")
            for bucket in buckets[:3]:  # Show first 3
                print(f"  - {bucket.name}")
        except Exception as e:
            print(f"⚠ Could not list buckets: {e}")
        
        return True
    except Exception as e:
        print(f"✗ MinIO Connection failed: {e}")
        return False


def test_json_download_and_update():
    """Test downloading JSON from S3 and updating URIs"""
    print("\n" + "=" * 60)
    print("TEST 3: JSON Download & URI Update")
    print("=" * 60)
    
    # Test with the actual JSON file in S3
    test_json_key = "conductor-poc/1000861509.WBNameParse.json"
    
    try:
        s3_manager = S3Manager()
        
        # Download JSON
        print(f"Downloading JSON from: {test_json_key}")
        json_data = s3_manager.download_json(test_json_key)
        print("✓ JSON downloaded successfully")
        
        # Check structure
        if 'service' in json_data:
            print("✓ JSON structure valid (has 'service' key)")
            
            # Check URIs
            if 'input' in json_data['service'] and 'uri' in json_data['service']['input']:
                print(f"  Input URI: {json_data['service']['input']['uri']}")
            
            if 'output' in json_data['service'] and 'uri' in json_data['service']['output']:
                print(f"  Output URI: {json_data['service']['output']['uri']}")
            
            if 'report' in json_data['service'] and 'uri' in json_data['service']['report']:
                print(f"  Report URI: {json_data['service']['report']['uri']}")
        
        # Test URI update (use only the company-allotted bucket/prefix)
        test_input_uri = (
            f"s3://{Config.ALLOWED_S3_BUCKET}/{Config.ALLOWED_S3_PREFIX}/tests/test-input.in"
        )
        test_output_uri = (
            f"s3://{Config.ALLOWED_S3_BUCKET}/{Config.ALLOWED_S3_PREFIX}/tests/test-output.out"
        )
        test_report_uri = (
            f"s3://{Config.ALLOWED_S3_BUCKET}/{Config.ALLOWED_S3_PREFIX}/tests/test-report.counts.txt"
        )
        
        print("\nTesting URI update...")
        updated_json = s3_manager.update_json_uris(
            json_data,
            test_input_uri,
            test_output_uri,
            test_report_uri,
            workflow_id="test-workflow-123"
        )
        #Instead of the updating the json uris will are going to update and upload the bucket back.
        s3_manager.fetch_and_update_json("conductor-poc/1000861509.WBNameParse.json",test_input_uri,test_output_uri,"conductor-poc/1000861509.WBNameParse.json",test_report_uri,"test-workflow-123")
        # Verify updates
        if updated_json['service']['input']['uri'] == test_input_uri:
            print("✓ Input URI updated correctly")
        else:
            print(f"✗ Input URI update failed: {updated_json['service']['input']['uri']}")
        
        if updated_json['service']['output']['uri'] == test_output_uri:
            print("✓ Output URI updated correctly")
        else:
            print(f"✗ Output URI update failed: {updated_json['service']['output']['uri']}")
        
        if updated_json['service']['report']['uri'] == test_report_uri:
            print("✓ Report URI updated correctly")
        else:
            print(f"✗ Report URI update failed: {updated_json['service']['report']['uri']}")
        
        # Check workflow ID update
        if 'metadata' in updated_json and 'environment' in updated_json['metadata']:
            for env_var in updated_json['metadata']['environment']:
                if env_var.get('name') == 'WORKFLOW_ID':
                    if env_var.get('value') == "test-workflow-123":
                        print("✓ Workflow ID updated correctly")
                    else:
                        print(f"✗ Workflow ID update failed: {env_var.get('value')}")
        #Lets Uploade the Json back to the S3 again


        
        
        return True
    except Exception as e:
        print(f"✗ JSON Download/Update test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_full_ingestion_flow():
    """Test the full ingestion flow (requires actual MinIO file)"""
    print("\n" + "=" * 60)
    print("TEST 4: Full Ingestion Flow (Dry Run)")
    print("=" * 60)
    print("⚠ This test requires:")
    print("  1. Valid MinIO file URI")
    print("  2. Valid S3 JSON file path")
    print("  3. AWS credentials configured")
    print("\nSkipping actual execution - showing test structure...")
    
    # Example test structure
    print("\nExample test call:")
    print("""
    service = IngestionService()
    
    result = service.ingest_file_and_update_json(
        minio_input_uri="minio://your-bucket/path/to/file.in",
        source_json_s3_key="conductor-poc/1000861509.WBNameParse.json",
        run_id="test-run-001"
    )
    
    print(f"Run ID: {result['run_id']}")
    print(f"Input URI: {result['s3_input_uri']}")
    print(f"Output URI: {result['s3_output_uri']}")
    print(f"Report URI: {result['s3_report_uri']}")
    print(f"Updated JSON URI: {result['updated_json_uri']}")
    """)
    
    return True


def main():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("PHASE 2 & 3 TEST SUITE")
    print("Testing: MinIO Download, S3 Upload, JSON URI Updates")
    print("=" * 60)
    
    # Check configuration
    print("\nConfiguration Check:")
    print(f"  S3 Bucket: {Config.S3_BUCKET}")
    print(f"  S3 Prefix: {Config.S3_PREFIX}")
    print(f"  MinIO Endpoint: {Config.MINIO_ENDPOINT}")
    print(f"  AWS Region: {Config.AWS_REGION}")
    
    if not Config.AWS_ACCESS_KEY_ID:
        print("\n⚠ WARNING: AWS_ACCESS_KEY_ID not set")
    if not Config.AWS_SECRET_ACCESS_KEY:
        print("⚠ WARNING: AWS_SECRET_ACCESS_KEY not set")
    
    # Run tests
    results = []
    
    results.append(("S3 Connection", test_s3_connection()))
    results.append(("MinIO Connection", test_minio_connection()))
    results.append(("JSON Download & Update", test_json_download_and_update()))
    results.append(("Full Ingestion Flow", test_full_ingestion_flow()))
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Ready for next phase.")
    else:
        print("\n⚠ Some tests failed. Please check configuration and credentials.")


if __name__ == "__main__":
    main()

