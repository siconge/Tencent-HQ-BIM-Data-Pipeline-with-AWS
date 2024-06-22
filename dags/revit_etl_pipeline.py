import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.aws_s3_pipeline import s3_upload_pipeline
from pipelines.revit_pipeline import revit_pipeline

default_args = {
    'owner': 'Sicong E',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='etl_revit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=0,
    tags=['revit', 'etl', 'pipeline']
)

file_postfix = datetime.now().strftime('%Y%m%d')

# Extract Data from Revit
task_extract = PythonOperator(
    task_id='revit_extraction',
    python_callable=revit_pipeline,
    op_kwargs={'file_name': f'revit_{file_postfix}_unitized_curtain_wall'},
    dag=dag
)

# Upload Data to AWS S3
task_upload = PythonOperator(
    task_id='s3_upload',
    python_callable=s3_upload_pipeline,
    dag=dag
)

task_extract >> task_upload