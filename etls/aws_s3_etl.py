# pip install s3fs boto3 pandas
import s3fs
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


def connect_to_s3(key, secret):
    try:
        s3 = s3fs.S3FileSystem(anon=0,
                               key=key,
                               secret=secret)
        return s3
    except Exception as e:
        print(e)


def create_bucket_if_not_exist(s3:s3fs.S3FileSystem, bucket:str):
    try:
        if not s3.exists(bucket):
            s3.mkdir(bucket)
            print('Bucket created')
        else:
            print('Bucket already exists')
    except Exception as e:
        print(e)


def upload_to_s3(s3:s3fs.S3FileSystem, file_path:str, bucket:str, s3_file_name:str):
    try:
        s3.put(file_path, bucket+'/raw/'+s3_file_name)
        print('File uploaded to s3')
    except FileNotFoundError:
        print('File not found')