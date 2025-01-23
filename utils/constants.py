import configparser
import os
import logging

parser = configparser.ConfigParser()

try:
    parser.read(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'config.conf'))
except Exception as e:
    logging.exception(f'Error reading configuration file: {e}')
    raise

APS_API_BASE_URL = parser.get('bim', 'aps_api_base_url')
APS_CLIENT_CREDENTIALS_SECRET_NAME = parser.get('bim', 'aps_client_credentials_secret_name')
BIM_360_PROJECT_ID = parser.get('bim', 'bim_360_project_id')
BIM_360_ITEM_ID = parser.get('bim', 'bim_360_item_id')
MODEL_VIEW_NAME = parser.get('bim', 'model_view_name')

AWS_ACCESS_KEY_ID = parser.get('aws', 'aws_access_key_id')
AWS_SECRET_ACCESS_KEY = parser.get('aws', 'aws_secret_access_key')
AWS_REGION_NAME = parser.get('aws', 'aws_region_name')
S3_BUCKET_NAME = parser.get('aws', 's3_bucket_name')
DATA_SOURCE_FOLDER_NAME = parser.get('aws', 'data_source_folder_name')
GLUE_JOB_NAME = parser.get('aws', 'glue_job_name')

OUTPUT_PATH = parser.get('file_paths', 'output_path')
OUTPUT_FILENAME_PREFIX = parser.get('file_paths', 'output_filename_prefix')
GLUE_JOB_SCRIPT_LOCAL_PATH = parser.get('file_paths', 'glue_job_script_local_path')
GLUE_JOB_SCRIPT_S3_FOLDER_NAME = parser.get('file_paths', 'glue_job_script_s3_folder_name')
