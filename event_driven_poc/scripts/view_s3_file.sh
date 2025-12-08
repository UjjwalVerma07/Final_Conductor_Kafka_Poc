#!/bin/bash
#
# Helper script to download and view S3 file
# Usage: ./view_s3_file.sh <s3-uri>
#

S3_URI="${1:-s3://958825666686-dpservices-testing-data/conductor-poc/1000861642/1000861642.in}"

echo "Downloading S3 file: ${S3_URI}"
echo ""

# Extract bucket and key from S3 URI
BUCKET=$(echo "$S3_URI" | sed 's|s3://||' | cut -d'/' -f1)
KEY=$(echo "$S3_URI" | sed "s|s3://${BUCKET}/||")

echo "Bucket: ${BUCKET}"
echo "Key: ${KEY}"
echo ""

# Download file
OUTPUT_FILE=$(basename "$KEY")
aws s3 cp "$S3_URI" "$OUTPUT_FILE" 2>&1

if [ $? -eq 0 ]; then
  echo ""
  echo "✅ File downloaded: ${OUTPUT_FILE}"
  echo ""
  echo "File size: $(wc -c < "$OUTPUT_FILE") bytes"
  echo "Number of lines: $(wc -l < "$OUTPUT_FILE")"
  echo ""
  echo "First 5 lines (hex dump to see exact bytes):"
  head -5 "$OUTPUT_FILE" | od -c | head -20
  echo ""
  echo "To view full file:"
  echo "  cat ${OUTPUT_FILE}"
  echo "  or"
  echo "  less ${OUTPUT_FILE}"
else
  echo "❌ Failed to download file"
  exit 1
fi






