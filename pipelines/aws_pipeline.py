from airflow.models import TaskInstance
from etls.aws_etl import create_aws_session, create_bucket_if_not_exist, upload_file_to_s3, trigger_glue_job, monitor_glue_job_completion
from utils.constants import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION_NAME, S3_BUCKET_NAME, DATA_SOURCE_FOLDER_NAME, GLUE_JOB_NAME, GLUE_JOB_SCRIPT_LOCAL_PATH, GLUE_JOB_SCRIPT_S3_FOLDER_NAME


def aws_pipeline(ti: TaskInstance) -> None:
    """
    Executes the AWS portion of the pipeline, including S3 interactions and Glue job orchestration.
    """
    # Step 1: Connect to S3 and prepare resources
    session = create_aws_session(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION_NAME)
    create_bucket_if_not_exist(session, S3_BUCKET_NAME, AWS_REGION_NAME)

    # Step 2: Upload input file and Glue job script to S3
    file_path = ti.xcom_pull(task_ids='bim_pipeline')
    file_name = file_path.split('/')[-1]
    upload_file_to_s3(session, file_path, S3_BUCKET_NAME, f'{DATA_SOURCE_FOLDER_NAME}/{file_name}')
    upload_file_to_s3(session, GLUE_JOB_SCRIPT_LOCAL_PATH, S3_BUCKET_NAME, f'{GLUE_JOB_SCRIPT_S3_FOLDER_NAME}/{GLUE_JOB_NAME}.py')
    
    # Step 3: Trigger Glue job and monitor its completion status
    run_id = trigger_glue_job(session, S3_BUCKET_NAME, DATA_SOURCE_FOLDER_NAME, file_name, GLUE_JOB_NAME)
    monitor_glue_job_completion(session, GLUE_JOB_NAME, run_id)