from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, Optional
from ingestion_service import IngestionService
from conductor_api import deployConductorWorkflow, triggerConductorWorkflow
import os
import time
import tempfile
import subprocess
from s3_utils import S3Manager
from config import Config
from dotenv import load_dotenv


load_dotenv()


app = FastAPI(title="Ingestion Service", version="1.1.0")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


CONDUCTOR_URL = os.getenv("CONDUCTOR_URL", "http://conductor-server-event:8080")

ingestion_service = IngestionService()


class WorkflowPayload(BaseModel):
    workflow: Dict[str, Any]
    workflow_id: Optional[str] = None
    trigger_conductor: Optional[bool] = True

def extract_tasks_xml(tasks:list)->dict:

    extracted_xmls={}
    for task in tasks:
        task_type=task.get("type")
        task_name=task.get("name")
        if task_type in ["KAFKA_PUBLISH"]:
            dp_config=(
                task.get("inputParameters")
                    .get("kafka_request")
                    .get("value")
                    .get("data")
                    .get("dp_config")
            )
            if dp_config:
                extracted_xmls[task_name]=dp_config
    return extracted_xmls



import json
from xml.dom.minidom import parseString

def normalize_input_string(raw):

    try:
        return json.loads(raw)  
    except Exception:
        return raw.replace('\\"', '"') 


def handle_newline_logic(xml_string: str) -> str:

    temp = xml_string
    result = ""
    i = 0

    while i < len(temp):
        if temp[i:i+2] == "\\n":
            context = temp[max(0, i-300):i] 
            
            if ("<dps:delim>" in context or
                "<dps:report" in context or
                "</dps:report" in context or
                "<dps:report_assignment" in context or
                "report." in context):
                result += "\\n"  
            else:
                result += "\\n"  
            i+=2
        else:
            result += temp[i]
            i += 1
    return result
   


def pretty_format_xml(xml_str, indent='    '):

    dom = parseString(xml_str)
    pretty = dom.toprettyxml(indent=indent)
    lines = [line for line in pretty.splitlines() if line.strip() != '']
    return '\n'.join(lines)


def convert_escaped_xml_to_pretty(raw_input):

    normalized = normalize_input_string(raw_input).strip()
    normalized = handle_newline_logic(normalized)
    pretty = pretty_format_xml(normalized)
    return pretty


def modify_email_hygiene_report_delimiter(json_data: dict) -> dict:

    if "service" in json_data and "report" in json_data["service"]:
        report = json_data["service"]["report"]
        if "record" in report and isinstance(report["record"], list):
            for record_field in report["record"]:
                if "delimiter" in record_field and record_field["delimiter"] == "\n":
                    record_field["delimiter"] = "\\n"
    return json_data


def xml_to_json(extracted_xmls:dict)->dict:
    uploaded_json={}

    # ALLOWED_S3_BUCKET = "958825666686-dpservices-testing-data"
    # ALLOWED_S3_PREFIX = "conductor-poc"
    s3_manager=S3Manager()
    s3_prefix=Config.S3_PREFIX
    s3_bucket=Config.S3_BUCKET
    
    

    for task_name,xml_string in extracted_xmls.items():
        print(f"Processing the service: {task_name}")

        with tempfile.NamedTemporaryFile(delete=False,suffix=".xml") as xml_file:
            pretty_xml=convert_escaped_xml_to_pretty(xml_string)
            xml_file.write(pretty_xml.encode("utf-8"))
            xml_path=xml_file.name

        
        with tempfile.NamedTemporaryFile(delete=False,suffix=".json") as json_file:
            json_path=json_file.name
        
         #Execution id can be WBNameParse , WBEmailHygiene
         #task->execution_id
        execution_id_mapping={
            "dp_email_hygiene":"WBEmailHygiene",
            "dp_name_parse":"WBNameParse"
        }
        execution_id=execution_id_mapping[task_name]
        #dpservices.bin usage
        cmd=[
            "/app/dpservices.bin",
            "-generate-dps-json",
            "-metadata_url",xml_path,
            "-output_url",json_path,
            "-execution_id",execution_id
        ]
        result=subprocess.run(cmd,capture_output=True,text=True)
        print(f"Result: {result}")
        if result.returncode!=0:
            raise RuntimeError(f"dpservices.bin failed:{result.stderr}")
        print(f"Generated Json for {task_name}:{json_path}")


       
        if task_name == "dp_email_hygiene":
            with open(json_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            modified_json = modify_email_hygiene_report_delimiter(json_data)
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(modified_json, f, indent=2, ensure_ascii=False)
            print(f"Modified report delimiter for {task_name}")
        
        s3_inputkey=f"{s3_prefix}/{task_name}.json"
        s3_manager.upload_file(json_path,s3_inputkey)
        print(f"Uploaded to S3 : {s3_inputkey}")
        uploaded_json[task_name]=s3_inputkey

    return uploaded_json
     
        
@app.post("/workflow/deploy")
async def deploy_workflow(payload: WorkflowPayload):
    try:

        minio_input_uri = "minio://raw-data/1000861642.in"
        print("Using hardcoded MinIO URI:", minio_input_uri)
        
        workflows=payload.workflow
        tasks=workflows.get("tasks",[])
        extracted_xmls=extract_tasks_xml(tasks) 
        print(f"Extracting XMLs for each corresponding tasks : {extracted_xmls}")

        uploaded_jsons=xml_to_json(extracted_xmls)
        print(uploaded_jsons)
   
        service_results,_ = ingestion_service.execute_service_chain(
            workflow_json=payload.workflow,
            minio_input_uri=minio_input_uri,
            workflow_id=payload.workflow_id
        )


        updated_workflow = ingestion_service.update_workflow_template(
            workflow_json=payload.workflow,
            service_results=service_results
        )

      
        if "version" not in updated_workflow:
            updated_workflow["version"] = int(time.time())

        workflow_instance_id = None
        if payload.trigger_conductor:
       
            deployConductorWorkflow(CONDUCTOR_URL, updated_workflow)

  
            workflow_instance_id = triggerConductorWorkflow(
                CONDUCTOR_URL,
                updated_workflow.get("name"),
                inputParams={},
                version=updated_workflow.get("version")
            )

        first_service_name = list(service_results.keys())[0] if service_results else None
        first_result = service_results.get(first_service_name, {})

        return {
            "runId": first_result.get("run_id"),
            "s3_input_uri": first_result.get("s3_input_uri"),
            "s3_output_uri": first_result.get("s3_output_uri"),
            "s3_report_uri": first_result.get("s3_report_uri"),
            "updated_json_uri": first_result.get("updated_json_uri"),
            "updated_json_s3_key": first_result.get("updated_json_s3_key"),
            "workflow_instance_id": workflow_instance_id,
            "message": (
                "Workflow deployed and triggered successfully"
                if workflow_instance_id else "Workflow deployed without triggering Conductor"
            ),
            
        }

    except Exception as e:
        import traceback
        print("Error deploying workflow:", str(e))
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to deploy workflow: {str(e)}"
        )
