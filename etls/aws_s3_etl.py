import boto3, json


# Retrieve AWS credentials from Secrets Manager
def get_secrets(secret_name):
    # Create a Secrets Manager client
    client = boto3.client('secretsmanager')
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        print(f'Error retrieving secret: {e}')
        return None
    secrets = json.loads(response['SecretString'])
    AWS_ACCESS_KEY_ID = secrets['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = secrets['AWS_SECRET_ACCESS_KEY']
    return AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY


def connect_to_s3(key:str, secret:str, region_name:str=region_name):
    try:
        # Initialize an S3 client with credentials
        s3 = boto3.client(
            's3',
            aws_access_key_id=key,
            aws_secret_access_key=secret
            region_name=region_name
        )
        return s3
    except ClientError as e:
        print(f'Error connecting to S3: {e}')


def create_bucket_if_not_exist(s3:boto3.client, bucket_name:str, region_name:str=region_name):
    try:
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
            print(f'Bucket "{bucket_name}" already exists')
        s3.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={'Status': 'Enabled'}
        )
        print(f'Bucket versioning enabled for "{bucket_name}"')
    except ClientError as e:
        print(f'Error checking or creating bucket "{bucket_name}": {e}')


def upload_to_s3(s3:boto3.client, local_file_path:str, bucket_name:str, file_name:str):
    try:
        # Upload the file to the specified S3 bucket
        s3.upload_file(Filename=local_file_path, Bucket=bucket_name, Key=f'raw/{file_name}')
        print(f'File uploaded to s3://{bucket_name}/raw/{file_name}')
    except FileNotFoundError:
        print('File not found')
    except ClientError as e:
        print(f'Error uploading file: {e}')


def create_empty_folder(s3:boto3.client, bucket_name:str, folder_name:str):
    try:
        # Add a zero-byte object to create the folder structure
        s3.put_object(Bucket=bucket_name, Key=f'{folder_name}/')
        print(f'Folder "{folder_name}" created in bucket "{bucket_name}"')
    except ClientError as e:
        print(f'Error creating folder "{folder_name}": {e}')
