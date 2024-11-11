import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'config.conf'))

APS_CLIENT_ID = parser.get('revit', 'aps_client_id')
APS_CLIENT_SECRET = parser.get('revit', 'aps__client_secret')
BIM_360_PROJECT_ID = parser.get('revit', 'bim_360_project_id')
BIM_360_FOLDER_ID = parser.get('revit', 'bim_360_folder_id')
REVIT_FILE_NAME = parser.get('revit', 'revit_file_name')
REVIT_MODEL_GUID = parser.get('revit', 'revit_model_guid')
APS_API_BASE_URL = parser.get('revit', 'aps_api_base_url')

AWS_ACCESS_KEY_SECRET_NAME = parser.get('aws', 'aws_access_key_secret_name')
AWS_REGION_NAME = parser.get('aws', 'aws_region_name')
AWS_BUCKET_NAME = parser.get('aws', 'aws_bucket_name')
AWS_FOLDER_NAME = parser.get('aws', 'aws_folder_name')

INPUT_PATH = parser.get('file_paths', 'input_path')
OUTPUT_PATH = parser.get('file_paths', 'output_path')

DATABASE_HOST = parser.get('database', 'database_host')
DATABASE_NAME = parser.get('database', 'database_name')
DATABASE_PORT = parser.get('database', 'database_port')
DATABASE_USER = parser.get('database', 'database_username')
DATABASE_PASSWORD = parser.get('database', 'database_password')
