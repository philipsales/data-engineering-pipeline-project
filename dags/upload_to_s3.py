import os 
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable, XCom

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (DatasetToS3Operator) 

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(), 
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False,
    'email_on_retry': False,
}

dag = DAG(
    "datasets_to_s3.v2",
    default_args=default_args,
    description='processed Data to S3',
    schedule_interval='0 * * * *')

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

upload_airport_data_to_s3 = DatasetToS3Operator(
        task_id='Upload_airport_data_to_S3',
        dag=dag,
        aws_credentials_id="aws_credentials",
        input_data="/datasets/processed_data/",
        s3_source="s3://udend-dataset",
        file_type="JSON"
)

upload_immigration_data_to_s3 = DatasetToS3Operator(
        task_id='Upload_immigration_data_to_S3',
        dag=dag,
        aws_credentials_id="aws_credentials",
        input_data="/datasets/processed_data/",
        s3_source="s3://udend-dataset",
        file_type="JSON"
)

upload_temperature_data_to_s3 = DatasetToS3Operator(
        task_id='Upload_temperature_data_to_S3',
        dag=dag,
        aws_credentials_id="aws_credentials",
        input_data_path="/datasets/processed_data/",
        s3_folder="temperature",
        s3_source="s3://udend-dataset",
        file_type="JSON"
)

end_operator = DummyOperator(task_id='End_execution', dag=dag)

start_operator >> [
    upload_temperature_data_to_s3 
    ,upload_immigration_data_to_s3
    ,upload_airport_data_to_s3 
    ] >> end_operator