from airflow.models import TaskInstance
from etls.aws_etl import get_access_keys, connect_to_s3, create_bucket_if_not_exist, create_folder_if_not_exist, upload_file_to_s3, trigger_glue_job, monitor_glue_job_completion
from utils.constants import AWS_ACCESS_KEYS_SECRET_NAME, AWS_REGION_NAME, S3_BUCKET_NAME, DATA_SOURCE_FOLDER_NAME, GLUE_JOB_NAME, GLUE_JOB_SCRIPT_LOCAL_PATH, GLUE_JOB_SCRIPT_S3_FOLDER


def aws_pipeline(ti:TaskInstance):
    """
    Executes the AWS portion of the pipeline, including S3 interactions and Glue job orchestration.
    """
    # Step 1: Retrieve AWS access keys from Secrets Manager
    access_keys = get_access_keys(AWS_ACCESS_KEYS_SECRET_NAME)

    # Step 2: Connect to S3 and prepare resources
    s3 = connect_to_s3(*access_keys, AWS_REGION_NAME)
    create_bucket_if_not_exist(s3, S3_BUCKET_NAME, AWS_REGION_NAME)
    create_folder_if_not_exist(s3, S3_BUCKET_NAME, DATA_SOURCE_FOLDER_NAME)

    # Step 3: Upload input file and Glue job script to S3
    file_path = ti.xcom_pull(task_ids='bim_pipeline')
    file_name = file_path.split('/')[-1]
    upload_file_to_s3(s3, file_path, S3_BUCKET_NAME, DATA_SOURCE_FOLDER_NAME)
    upload_file_to_s3(s3, GLUE_JOB_SCRIPT_LOCAL_PATH, S3_BUCKET_NAME, GLUE_JOB_SCRIPT_S3_FOLDER)
    
    # Step 4: Trigger Glue job and monitor its completion status
    glue, run_id = trigger_glue_job(s3, S3_BUCKET_NAME, DATA_SOURCE_FOLDER_NAME, file_name, GLUE_JOB_NAME)
    monitor_glue_job_completion(glue, GLUE_JOB_NAME, run_id)
