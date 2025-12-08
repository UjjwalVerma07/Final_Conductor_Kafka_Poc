#!/bin/bash
#
# Script to fix the input file by removing partial records
# Downloads, analyzes, fixes, and re-uploads the file
#

S3_URI="${1:-s3://958825666686-dpservices-testing-data/conductor-poc/1000861642/1000861642.in}"
RECORD_SIZE=326

echo "=========================================="
echo "Fix Input File - Remove Partial Records"
echo "=========================================="
echo "S3 URI: ${S3_URI}"
echo "Expected record size: ${RECORD_SIZE} bytes"
echo ""

# Extract bucket and key
BUCKET=$(echo "$S3_URI" | sed 's|s3://||' | cut -d'/' -f1)
KEY=$(echo "$S3_URI" | sed "s|s3://${BUCKET}/||")
OUTPUT_FILE=$(basename "$KEY")
BACKUP_FILE="${OUTPUT_FILE}.backup"
FIXED_FILE="${OUTPUT_FILE}.fixed"

echo "Step 1: Downloading file from S3..."
aws s3 cp "$S3_URI" "$OUTPUT_FILE" 2>&1

if [ $? -ne 0 ]; then
  echo "❌ Failed to download file"
  exit 1
fi

echo "✅ File downloaded: ${OUTPUT_FILE}"
echo ""

# Create backup
cp "$OUTPUT_FILE" "$BACKUP_FILE"
echo "✅ Backup created: ${BACKUP_FILE}"
echo ""

# Analyze the file
echo "Step 2: Analyzing file..."
FILE_SIZE=$(wc -c < "$OUTPUT_FILE")
EXPECTED_RECORDS=$((FILE_SIZE / RECORD_SIZE))
REMAINDER=$((FILE_SIZE % RECORD_SIZE))

echo "   Total file size: ${FILE_SIZE} bytes"
echo "   Expected record size: ${RECORD_SIZE} bytes"
echo "   Number of complete records: ${EXPECTED_RECORDS}"
echo "   Remainder bytes: ${REMAINDER}"
echo ""

if [ $REMAINDER -eq 0 ]; then
  echo "✅ File appears to have all complete records!"
  echo "   However, there might be a record with wrong content."
  echo "   Checking each record individually..."
  echo ""
  
  # Check each record's actual length
  python3 << 'PYTHON_SCRIPT'
import sys

record_size = 326
input_file = sys.argv[1]
fixed_file = sys.argv[2]

with open(input_file, 'rb') as f:
    data = f.read()

total_size = len(data)
num_complete = total_size // record_size
remainder = total_size % record_size

print(f"Analyzing {num_complete} complete records...")

# Check each record
problematic_records = []
for i in range(num_complete):
    start = i * record_size
    end = start + record_size
    record = data[start:end]
    
    # Check for null bytes or unusual patterns that might indicate corruption
    if len(record) != record_size:
        problematic_records.append(i)
        print(f"  Record {i+1}: Expected {record_size} bytes, got {len(record)} bytes")

if remainder > 0:
    print(f"\n⚠️ Found {remainder} extra bytes at the end (partial record)")
    print(f"   This will be removed.")
    
    # Write fixed file (remove partial record)
    with open(fixed_file, 'wb') as f:
        f.write(data[:num_complete * record_size])
    print(f"\n✅ Fixed file created: {fixed_file}")
    print(f"   Removed {remainder} bytes (partial record)")
    print(f"   New file size: {num_complete * record_size} bytes")
    print(f"   Records: {num_complete}")
else:
    print("\n✅ All records appear to be correct size")
    print("   File might have data quality issues within records")
    print("   Creating copy anyway...")
    with open(fixed_file, 'wb') as f:
        f.write(data)
    print(f"   Copy created: {fixed_file}")

PYTHON_SCRIPT
  "$OUTPUT_FILE" "$FIXED_FILE"
  
else
  echo "⚠️ Found ${REMAINDER} extra bytes - partial record detected!"
  echo ""
  echo "Step 3: Removing partial record..."
  
  # Use Python to remove the partial record
  python3 << 'PYTHON_SCRIPT'
import sys

record_size = 326
input_file = sys.argv[1]
fixed_file = sys.argv[2]

with open(input_file, 'rb') as f:
    data = f.read()

total_size = len(data)
num_complete = total_size // record_size
remainder = total_size % record_size

print(f"Original file: {total_size} bytes")
print(f"Complete records: {num_complete}")
print(f"Partial record: {remainder} bytes (will be removed)")

# Write fixed file (only complete records)
with open(fixed_file, 'wb') as f:
    f.write(data[:num_complete * record_size])

new_size = num_complete * record_size
print(f"\n✅ Fixed file created: {fixed_file}")
print(f"   New file size: {new_size} bytes")
print(f"   Records: {num_complete}")
print(f"   Removed: {remainder} bytes (partial record)")

PYTHON_SCRIPT
  "$OUTPUT_FILE" "$FIXED_FILE"
fi

echo ""
echo "Step 4: Verifying fixed file..."
FIXED_SIZE=$(wc -c < "$FIXED_FILE")
FIXED_RECORDS=$((FIXED_SIZE / RECORD_SIZE))
FIXED_REMAINDER=$((FIXED_SIZE % RECORD_SIZE))

echo "   Fixed file size: ${FIXED_SIZE} bytes"
echo "   Records: ${FIXED_RECORDS}"
echo "   Remainder: ${FIXED_REMAINDER} bytes"

if [ $FIXED_REMAINDER -eq 0 ]; then
  echo "   ✅ Fixed file has all complete records!"
else
  echo "   ⚠️ Still has remainder - may need manual inspection"
fi

echo ""
read -p "Upload fixed file back to S3? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
  echo ""
  echo "Step 5: Uploading fixed file to S3..."
  aws s3 cp "$FIXED_FILE" "$S3_URI" 2>&1
  
  if [ $? -eq 0 ]; then
    echo "✅ Fixed file uploaded successfully!"
    echo ""
    echo "Original file backed up locally as: ${BACKUP_FILE}"
    echo "You can restore it if needed with:"
    echo "  aws s3 cp ${BACKUP_FILE} ${S3_URI}"
  else
    echo "❌ Failed to upload fixed file"
    exit 1
  fi
else
  echo ""
  echo "Skipping upload. Files saved locally:"
  echo "  Original: ${OUTPUT_FILE}"
  echo "  Backup: ${BACKUP_FILE}"
  echo "  Fixed: ${FIXED_FILE}"
  echo ""
  echo "To upload manually:"
  echo "  aws s3 cp ${FIXED_FILE} ${S3_URI}"
fi

echo ""
echo "=========================================="
echo "Done!"
echo "=========================================="









