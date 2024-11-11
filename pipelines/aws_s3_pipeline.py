from airflow.models import TaskInstance
from etls.aws_s3_etl import get_secrets, connect_to_s3, create_bucket_if_not_exist, upload_to_s3
from utils.constants import AWS_ACCESS_KEY_SECRET_NAME, AWS_REGION_NAME, AWS_BUCKET_NAME, AWS_FOLDER_NAME


def s3_upload_pipeline(ti:TaskInstance):
    secrets = get_secrets(AWS_ACCESS_KEY_SECRET_NAME)
    s3 = connect_to_s3(*secrets, AWS_REGION_NAME)
    create_bucket_if_not_exist(s3, AWS_BUCKET_NAME, AWS_REGION_NAME)
    local_file_path = ti.xcom_pull(task_ids='revit_extraction')
    upload_to_s3(s3, local_file_path, AWS_BUCKET_NAME, local_file_path.split('/')[-1])
    create_empty_folder(s3, AWS_BUCKET_NAME, AWS_FOLDER_NAME)
