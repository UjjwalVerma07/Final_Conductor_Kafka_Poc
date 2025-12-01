#!/bin/bash

# Get parameters from environment variables (set by service.py)
JOBID="${JOBID:-1000876411}"
METADATA_URL="${METADATA_URL:-s3://958825666686-dpservices-testing-data/conductor-poc/dp_email_hygiene.json}"
EXECUTION_ID="${EXECUTION_ID:-WBEmailHygiene}"
DAG_ID="${DAG_ID:-nua-emailhygiene-process-stage-v01-01-04-tiny}"
MWAA_ENDPOINT="${MWAA_ENDPOINT:-https://a53c6d7a-ec07-465a-9824-6cc199145a7a-vpce.c75.us-east-1.airflow.amazonaws.com:443}"
MWAA_SESSION_TOKEN="${MWAA_SESSION_TOKEN:-95033acf-cef2-408c-9740-cd997701ce94.-nNO2hXdybJmNCrudLXXa43kfLg}"
 
# Make jobid unique by appending session ID (4 digits)
# Use SESSION_ID from environment if provided (from workflow), otherwise use process ID
if [ -z "${SESSION_ID}" ]; then
    SESSION_ID=$(printf "%04d" $$)
fi
ORIGINAL_JOBID="${JOBID}"
JOBID="${JOBID}-${SESSION_ID}"

# Optional stats_url (can be provided in event data if needed)
# Default stats_url format: scp://abinitio@papdpsetld003l.intra.infousa.com//abi/log/UQU_${jobid}.2771.stats.jsonl
STATS_URL="${STATS_URL:-scp://abinitio@papdpsetld003l.intra.infousa.com//abi/log/UQU_1000876411.32584.stats.jsonl}"

# Log to stderr (so it doesn't interfere with JSON output)
echo "Triggering Airflow DAG: ${DAG_ID}" >&2
echo "Job ID: ${JOBID}" >&2
echo "Metadata URL: ${METADATA_URL}" >&2

# Build the JSON payload
CONF_JSON="{\"jobid\":\"${JOBID}\",\"metadata_url\":\"${METADATA_URL}\",\"execution_id\":\"${EXECUTION_ID}\""

# Add stats_url if provided
if [ -n "${STATS_URL}" ]; then
  CONF_JSON="${CONF_JSON},\"stats_url\":\"${STATS_URL}\""
fi

CONF_JSON="${CONF_JSON}}"

# Trigger the DAG run and capture response
RESPONSE=$(curl -X POST "${MWAA_ENDPOINT}/api/v1/dags/${DAG_ID}/dagRuns" \
     --silent \
     -b "session=${MWAA_SESSION_TOKEN}" \
     -H 'Content-Type: application/json' \
     --data-binary "{
       \"dag_run_id\": \"${JOBID}\",
       \"conf\": ${CONF_JSON}
     }")

EXIT_CODE=$?

# Output only the JSON response to stdout (for parsing by service.py)
echo "${RESPONSE}"

# Return the curl exit code
exit $EXIT_CODE

#All the values are currenlty hardcoded in the script like metadata_url,stats_url and job_id and execution_id.