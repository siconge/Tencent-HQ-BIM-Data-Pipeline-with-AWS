import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.aws_pipeline import aws_pipeline
from pipelines.bim_pipeline import bim_pipeline


def failure_callback(context):
    """
    Sends an email notification upon task failure using Airflow's callback mechanism.
    """
    subject = f"Airflow Task Failed: {context['task_instance_key_str']}"
    body = f"""
    Task failed with the following details:
    Task Instance: {context['task_instance']}
    DAG: {context['dag']}
    Task ID: {context['task_id']}
    Exception: {context['exception']}
    Log URL: {context['task_instance'].log_url}
    """
    send_email(to="your-email@example.com", subject=subject, html_content=body)


default_args = {
    'owner': 'Sicong E',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': failure_callback,  # Attach failure callback globally
}

dag = DAG(
    dag_id='bim_aws_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=0,
    tags=['bim', 'aws', 'etl', 'pipeline']
)


# Task for extracting and processing BIM data from Autodesk BIM 360
task_bim_pipeline = PythonOperator(
    task_id='bim_pipeline',
    python_callable=bim_pipeline,
    dag=dag
)

# Task for loading transformed data into AWS for further analytics
task_aws_pipeline = PythonOperator(
    task_id='aws_pipeline',
    python_callable=aws_pipeline,
    dag=dag
)

# Define task dependencies
task_bim_pipeline >> task_aws_pipeline