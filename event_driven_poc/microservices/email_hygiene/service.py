#! /usr/bin/env python

import os
import json
import time
import logging
import subprocess
from kafka.metrics.stats import Min
import requests
import re
from kafka import KafkaConsumer, KafkaProducer
import tempfile
from s3_utils import S3Manager
from minio_utils import MinIOManager

s3_manager=S3Manager()
minio_manager=MinIOManager()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger=logging.getLogger(__name__)


KAFKA_BOOTSTRAP=os.getenv('KAFKA_BOOTSTRAP','localhost:9092')
SERVICE_NAME=os.getenv('SERVICE_NAME','email_hygiene_service')


MWAA_ENDPOINT = os.getenv('MWAA_ENDPOINT', 'https://a53c6d7a-ec07-465a-9824-6cc199145a7a-vpce.c75.us-east-1.airflow.amazonaws.com:443')
MWAA_SESSION_TOKEN = os.getenv('MWAA_SESSION_TOKEN', '')

DAG_POLL_INTERVAL = int(os.getenv('DAG_POLL_INTERVAL', '10')) 
DAG_MAX_WAIT_TIME = int(os.getenv('DAG_MAX_WAIT_TIME', '3600'))  

SCRIPT_PATH=os.path.join(os.path.dirname(__file__),'trigger_email_hygiene.sh')

kafka_producer=KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),

)

class EmailHygieneService:
    """Email Hygiene Microservice Class Triggers Airflow DAGS and Notifies Conductor"""

    def __init__(self):
        self.kafka_producer=kafka_producer
        self.script_path=SCRIPT_PATH

        os.chmod(self.script_path, 0o755) 
        logger.info(f"Email Hygiene Service initialized.")
        logger.info(f"Script:{self.script_path}")
    


    def trigger_dag_run(self, jobid, session_id, metadata_url, execution_id, dag_id=None, stats_url=None):
     
        dag_id= dag_id or 'nua-emailhygiene-process-stage-v01-01-04-tiny'
        max_retries=3
        backoff_seconds=2

        for attempt in range(1,max_retries+1):
            try:
                logger.info(f"Attempt No.{attempt}")
                logger.info(f"Triggering Airflow DAG")
                logger.info(f"DAG ID: {dag_id}")
                logger.info(f"Job ID: {jobid}")
                logger.info(f"Session ID: {session_id}")
                logger.info(f"Final Job ID: {jobid}-{session_id}")
                logger.info(f"Metadata URL: {metadata_url}")
                logger.info(f"Execution ID: {execution_id}")
            
                env = os.environ.copy()
                env.update({
                    'JOBID': jobid,
                    'SESSION_ID': session_id, 
                    'EXECUTION_ID': execution_id,
                    'DAG_ID': dag_id
                })
    
                if MWAA_ENDPOINT:
                    env['MWAA_ENDPOINT'] = MWAA_ENDPOINT
                if MWAA_SESSION_TOKEN:
                    env['MWAA_SESSION_TOKEN'] = MWAA_SESSION_TOKEN
            
         
                logger.info(f"Executing trigger script: {self.script_path}")
                result = subprocess.run(
                    ['/bin/bash', self.script_path],
                    capture_output=True,
                    text=True,
                    timeout=60,
                    env=env
                )
            
    
                if result.stderr:
                    logger.info(f"Script output: {result.stderr.strip()}")

                if result.returncode == 0:
    
                    try:
             
                        stdout_clean = result.stdout.strip()
                        logger.debug(f"Script JSON response: {stdout_clean}")
                    
                        response_data = json.loads(stdout_clean)
              
                        logger.info(f"Full API response: {json.dumps(response_data, indent=2)}")
                    
                        api_status = response_data.get('status')
                        if api_status is not None and api_status != 200:
                            error_detail = response_data.get('detail', 'Unknown error')
                            error_title = response_data.get('title', 'Error')
                        
                   
                            if api_status == 409:
                
                                match = re.search(r"DAGRun ID: '([^']+)'", error_detail)
                                if match:
                                    existing_dag_run_id = match.group(1)
                                    logger.warning(f"DAG run already exists (409 Conflict): {existing_dag_run_id}")
                                    logger.warning(f"   Will monitor existing DAG run instead of creating new one")
                                    return True, existing_dag_run_id
                                else:
                                    constructed_dag_run_id = f"{jobid}-{session_id}"
                                    logger.warning(f"DAG run already exists (409 Conflict), using constructed ID: {constructed_dag_run_id}")
                                    return True, constructed_dag_run_id
                        
                   
                            logger.error(f"Airflow API error (status {api_status}): {error_title}")
                            logger.error(f"   Detail: {error_detail}")
                            # return False, None
         
                        if 'dag_run_id' not in response_data:
            
                            error_detail = response_data.get('detail', response_data.get('message', 'Unknown error'))
                            logger.error(f"Airflow API error: No dag_run_id in response")
                            logger.error(f"Response: {error_detail}")
                            # return False, None
                    

                        dag_run_id = response_data.get('dag_run_id')
                    
                        logger.info(f"Airflow DAG triggered successfully via script")
                        logger.info(f"   DAG Run ID: {dag_run_id}")
                        logger.info(f"   Original Job ID: {jobid}")
                        return True, dag_run_id
                    except json.JSONDecodeError as e:
            
                        logger.error(f"Could not parse script response as JSON: {e}")
                        logger.error(f"   Response was: {result.stdout}")
                        logger.warning(f"Using original jobid as dag_run_id (may not match actual DAG run)")
                        # return True, jobid
                else:
                    error_msg = f"Script execution failed with exit code {result.returncode}"
                    logger.error(f"{error_msg}")
                    logger.error(f"stderr: {result.stderr}")
                    logger.error(f"stdout: {result.stdout}")
                    # return False, None
                
            except subprocess.TimeoutExpired:
                error_msg = "Script execution timed out"
                logger.error(f"{error_msg}")
                # return False, None
            except Exception as e:
                error_msg = f"Error executing script: {e}"
                logger.error(f" {error_msg}")
                # return False, None
            
            time.sleep(backoff_seconds)
            backoff_seconds*=2

        logger.error("Failed to trigger DAG after retries")
        return False,None
    

    def check_dag_run_status(self,dag_id,dag_run_id):

        try:
            endpoint=MWAA_ENDPOINT
            session_token=MWAA_SESSION_TOKEN

            if not session_token:
                logger.error("MWAA_SESSION_TOKEN not configured.")
                return None,"Session token not configured."
            api_url=f"{endpoint}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"

            response=requests.get(
                api_url,
                cookies={'session':session_token},
                headers={'Content-Type':'application/json'},
                timeout=30
            )
            if response.status_code==200:
                dag_run_data=response.json()
                state=dag_run_data.get('state','unknown')
                return state,None
            elif response.status_code==404:
                return None,f"DAG run {dag_run_id} not found."
            else:
                return None, f"API error: {response.status_code} - {response.text}"
                
        except requests.exceptions.RequestException as e:
            return None, f"Network error: {str(e)}"
        except Exception as e:
            return None, f"Error checking status: {str(e)}"
                
    
    def wait_for_dag_completion(self,dag_id,dag_run_id,timeout=None):
        timeout=timeout or DAG_MAX_WAIT_TIME
        start_time=time.time()
        poll_interval=DAG_POLL_INTERVAL

        logger.info(f"Monitoring DAG run: {dag_run_id}")
        logger.info(f"   Poll interval: {poll_interval}s, Max wait: {timeout}s")

        while True:
            elapsed=time.time()-start_time
            if elapsed > timeout:
                logger.error(f"Timout waiting for DAG run to complete ({timeout}s)")
                return False,'timeout',f"DAG run did not complete within {timeout} seconds."
            
            state,error=self.check_dag_run_status(dag_id,dag_run_id)
            if error:
                logger.error(f"Error checking DAG run status: {error}")
                return False,'error',error
            
            if state is None:
                logger.info(f"Could not determin DAG run status")
                time.sleep(poll_interval)
                continue
            logger.info(f"DAG Run Status: {state} (elapsed: {int(elapsed)}s)")

            if state in ['success','failed','skipped','upstream_failed']:
                if state=='success':
                    return True,state,None
                else:
                    logger.error(f"DAG run completed with state: {state}")
                    return False,state,f"DAG run completed with state: {state}" 

            if state in ['queued', 'running', 'up_for_retry', 'up_for_reschedule']:
                time.sleep(poll_interval)
                continue
            
            # Unknown state
            logger.warning(f"Unknown DAG run state: {state}, continuing to monitor...")
            time.sleep(poll_interval)            
    

    def publish_completion_event(self,workflow_id,task_id,dag_id,jobid,status,
                                 error_message=None,metadata_url=None):
   
        try:
  
            event_type="email_hygiene_completed"
            completion_event={
                "workflowId":workflow_id,
                "taskId":task_id,
                "eventType":event_type, 
                "data":{
                    "dag_id":dag_id,
                    "jobid":jobid,
                    "status":status,
                    "result":"success" if status=="success" else "failure",
                    "pipelineStage":"email_hygiene_processing",
                    "timestamp":time.strftime("%Y-%m-%dT%H:%M:%SZ")

                }
            }

            if metadata_url:
                completion_event['data']['metadata_url']=metadata_url
            
    
            if status=="failed" and error_message:
                completion_event['data']['error_message']=error_message



            self.kafka_producer.send('conductor-events',completion_event)
            self.kafka_producer.flush()

            logger.info(f"Published completion event to Conductor for Workflow ID: {workflow_id}, Task ID: {task_id}, Status: {status}")
            logger.info(f" Event Type: {event_type}")
            logger.info(f"DAG: {dag_id}, Job ID: {jobid} , Status: {status}")

        except Exception as e:
            logger.error(f"Error publishing completion event: {e}",exc_info=True)


    def process_task_event(self,event):
     
        try:
            workflow_id=event.get('workflowId')
            task_id=event.get('taskId')
            data=event.get('data',{})

            dag_id=data.get('dagId','nua-emailhygiene-process-stage-v01-01-04-tiny')
            execution_id=data.get('executionId','WBEmailHygiene')

        
            metadata_url=None 
            base_jobid='1000876411' 

            if workflow_id:
          
                session_hash = abs(hash(workflow_id)) % 10000
                session_id = f"{session_hash:04d}" 
            else:
         
                import os
                session_id = f"{os.getpid() % 10000:04d}"
            
            logger.info(f"Processing Email Hygiene request")
            logger.info(f" Workflow ID: {workflow_id}")
            logger.info(f"Task ID: {task_id}")
            logger.info(f"DAG ID: {dag_id}")
            logger.info(f"Base Job ID: {base_jobid}")
            logger.info(f"Session ID: {session_id}")
            logger.info(f"   Final Job ID: {base_jobid}-{session_id}")
            logger.info(f"   Metadata URL: {metadata_url}")
            logger.info(f"   Execution ID: {execution_id}")


            logger.info(f"Step 1: Triggering DAG run via script..")
            trigger_success,dag_run_id=self.trigger_dag_run(
                jobid=base_jobid,
                session_id=session_id,
                metadata_url=metadata_url,
                execution_id=execution_id,
                dag_id=dag_id
            )

            if not trigger_success or not dag_run_id:
                logger.error(f"Failed to trigger DAG run via script.")
                full_job_id=f"{base_jobid}-{session_id}"
                self.publish_completion_event(
                    workflow_id=workflow_id,
                    task_id=task_id,
                    dag_id=dag_id,
                    jobid=full_job_id,
                    status="failed",
                    error_message='Failed to trigger DAG run via script.',
                    metadata_url=metadata_url
                )
                return
            
     

            logger.info(f"Step 2 : Waiting for DAG run to complete...")
            logger.info(f"Monitoring DAG run: {dag_run_id}")
            dag_success,final_state,error_message=self.wait_for_dag_completion(
                dag_id=dag_id,
                dag_run_id=dag_run_id
            )

            logger.info(f"Step 3: Publishing completion event to Conductor...")
            full_job_id=f"{base_jobid}-{session_id}"

            
            metadata_key=f"conductor-poc/dp_email_hygiene.json"
            output_uri=""
            with tempfile.NamedTemporaryFile(delete=False,suffix=".json") as temp_file:
                s3_manager.download_file(metadata_key,temp_file.name)

            with open(temp_file.name,'r') as f:
                metadata_data=json.load(f)
                output_uri=metadata_data.get('service',{}).get('output',{}).get('uri')
            
            if output_uri:
       
                output_key=output_uri.split('/', 3)[-1]
                with tempfile.NamedTemporaryFile(delete=False,suffix=".out") as output_file:
                    s3_manager.download_file(output_key,output_file.name)
                    logger.info(f"OuputFile Downloaded From S3 Now preparing to upload on MINIO")
                    minio_manager.upload_file(output_file.name,"email-hygiene-output","email_hygiene.out")
                    logger.info(f"Uploaded output file to MINIO")
            else:
                logger.error(f"No output URI found in metadata")

            self.publish_completion_event(
                workflow_id=workflow_id,
                task_id=task_id,
                dag_id=dag_id,
                jobid=full_job_id,
                status="success" if dag_success else "failed",
                error_message=error_message,
                metadata_url=metadata_url
            )

            logger.info(f"Email Hygiene task processing completed for Workflow ID: {workflow_id}, Task ID: {task_id}")
            logger.info(f"DAG Run Id : {dag_run_id}")
            logger.info(f"Final Status: {final_state}")
            logger.info(f"Status : {'success' if dag_success else 'failed'}")

        except Exception as e:
            logger.error(f"Error processing Airflow task event: {e}", exc_info=True)
            
            # Publish failure event
            workflow_id = event.get('workflowId', 'unknown')
            task_id = event.get('taskId', 'unknown')
            
            self.publish_completion_event(
                workflow_id=workflow_id,
                task_id=task_id,
                dag_id=event.get('data', {}).get('dag_id', 'unknown'),
                jobid='unknown',
                status='failed',
                error_message=str(e),
                metadata_url=None
            )


    
    def consume_task_events(self):
        """Consume task events from Kafka and process them."""
        consumer=KafkaConsumer(
            'email-hygiene-requests',
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset="latest",
            group_id=f"{SERVICE_NAME}-group"
        )

        logger.info(f"Consuming task events from 'email-hygiene-requests' topic...")
        logger.info(f"{SERVICE_NAME} started - listening for Email Hygiene requests.")
        logger.info(f"Kafka Topic: email-hygiene-requests")

        for message in consumer:
            try:
                event=message.value

                # Handle both JSON object and string cases
                if isinstance(event,str):
                    try:
                        event=json.loads(event)
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON string received: {event}")
                        continue

                event_type=event.get('eventType','email_hygiene_request')
                if event_type == 'email_hygiene_request':
                    self.process_task_event(event)
                else:
                    logger.warning(f"Unknown event type received: {event_type}")
            
            except Exception as e:
                logger.error(f"Error processing message: {e}",exc_info=True)


def main():
    """Main function to start the Email Hygiene Service."""
    logger.info(f"Starting {SERVICE_NAME} Service...")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")

    logger.info("Waiting 10 seconds for services to initialize...")
    time.sleep(10)

    service=EmailHygieneService()
    service.consume_task_events()

if __name__ == "__main__":
    main()