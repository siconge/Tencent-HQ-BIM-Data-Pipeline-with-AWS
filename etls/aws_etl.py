import logging
import boto3
from botocore.exceptions import ClientError
from time import sleep


def create_aws_session(aws_access_key_id: str, aws_secret_access_key: str, region_name: str) -> boto3.Session:
    """
    Creates and returns a Boto3 session configured with AWS credentials and region.  
    """
    try:
        session = boto3.session.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        return session
    except ClientError as e:
        logging.exception(f"Error creating AWS session: {e}")
        raise


def create_bucket_if_not_exist(session: boto3.Session, bucket_name: str, region_name: str) -> None:
    try:
        s3 = session.client('s3')
        
        # Check if bucket exists
        response = s3.list_buckets()
        bucket_exists = any(bucket['Name'] == bucket_name for bucket in response['Buckets'])
        if not bucket_exists:
            # Create the bucket if it doesn't exist
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region_name}
            )
            print(f'Bucket "{bucket_name}" created in region "{region_name}"')
        else:
            print(f'Bucket "{bucket_name}" already exists in region "{region_name}"')
        s3.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={'Status': 'Enabled'}
        )
        print(f'Bucket versioning enabled for "{bucket_name}"')
    except ClientError as e:
        logging.exception(f'Error checking or creating bucket "{bucket_name}": {e}')
        raise


# def create_folder_if_not_exist(session: boto3.Session, bucket_name: str, folder_name: str) -> None:
#     try:
#         s3 = session.client('s3')

#         # Check if a folder already exists as long as a single object is fetched (sufficient to check for the existence of a folder)
#         response = s3.list_objects_v2(Bucket=bucket_name, Prefix=f'{folder_name}/', MaxKeys=1)
#         if 'Contents' in response:
#             print(f'Folder "{folder_name}/" already exists in bucket "{bucket_name}"')
        
#         # Add a zero-byte object to create the folder structure
#         else:
#             s3.put_object(Bucket=bucket_name, Key=f'{folder_name}/')
#             print(f'Folder "{folder_name}/" created in bucket "{bucket_name}"')
#     except ClientError as e:
#         logging.exception(f'Error creating folder "{folder_name}/": {e}')
#         raise


def upload_file_to_s3(session: boto3.Session, local_file_path: str, bucket_name: str, file_key: str) -> None:
    try:
        s3 = session.client('s3')
        s3.upload_file(Filename=local_file_path, Bucket=bucket_name, Key=file_key)
        print(f"File uploaded to s3://{bucket_name}/{file_key}")
    
    # Ensure the local file exists before attempting upload to S3
    except FileNotFoundError:
        logging.exception(f"File {local_file_path} not found")
        raise
    except ClientError as e:
        logging.exception(f"Error uploading file: {e}")
        raise


def trigger_glue_job(session: boto3.Session, bucket_name: str, folder_name: str, file_name: str, glue_job_name: str) -> str:
    """
    Triggers an AWS Glue job if valid CSV files exist in the specified folder.
    """
    # Initialize S3 and Glue clients
    s3 = session.client('s3')
    glue = session.client('glue')

    # Define folder prefix and valid file suffix
    prefix = f'{folder_name}/'
    suffix = '.csv'
    
    try:
        # List objects in the S3 bucket
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' not in response:
            logging.exception(f'Folder "{prefix}" does not exist')
            raise

        # Categorize files
        objects_in_folder = response['Contents']
        csv_files = [obj['Key'] for obj in objects_in_folder if obj['Key'].endswith(suffix) and obj['Size'] > 0]
        non_csv_files = [obj['Key'] for obj in objects_in_folder if not obj['Key'].endswith(suffix) and obj['Size'] > 0]

        # Handle non-compatible files
        if non_csv_files:
            logging.exception(f'Non-compatible files found in folder "{prefix}": {non_csv_files}')
            raise

        # Handle no valid CSV files
        if not csv_files:
            logging.exception(f'No valid CSV files found in folder "{prefix}"')
            raise
        
        # Trigger Glue job
        print(f"Triggering Glue job for file: {file_name}")
        response = glue.start_job_run(JobName=glue_job_name)
        run_id = response['JobRunId']
        return run_id
    
    except ClientError as e:
        logging.exception(f'Error triggering Glue ETL job "{glue_job_name}": {e}')
        raise
    except Exception as e:
        logging.exception(f"Unexpected error: {e}")
        raise

 
def monitor_glue_job_completion(session: boto3.Session, glue_job_name: str, run_id: str) -> str:
    """
    Monitors the completion status of an AWS Glue job.
    """
    glue = session.client('glue')
    while True:
        response = glue.get_job_run(JobName=glue_job_name, RunId=run_id)
        status = response['JobRun']['JobRunState']
        end_time = response['JobRun'].get('CompletedOn')
        duration = response['JobRun'].get('ExecutionTime')        
        if status in ['SUCCEEDED']:
            print(f'Glue job "{glue_job_name}" succeeded. Status: {status}, End Time: {end_time}, Duration: {duration} seconds')
            return status
        
        # Raise an error if the job fails or stops to notify Airflow of task failure
        elif status in ['FAILED', 'STOPPED']:
            logging.exception(f'Glue job "{glue_job_name}" failed or stopped. Status: {status}, End Time: {end_time}')
            raise
        print(f"Job still running. Current status: {status}. Retrying in 10 seconds...")
        sleep(10)