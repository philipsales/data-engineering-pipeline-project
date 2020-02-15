import os 
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable, XCom

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator
                        ,LoadFactOperator
                        ,LoadDimensionOperator
                        ,DataQualityOperator) 


from helpers import SqlQueries

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 12), #for developent only - shorter time
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False,
    'email_on_retry': False,
}

dag = DAG(
    "s3_to_redshift.v13",
    default_args=default_args,
    description='S3 data to Redshift',
    schedule_interval='@monthly')

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_immigration_to_redshift = StageToRedshiftOperator(
    task_id='Stage_immigration_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="_stg_immigration",
    s3_source="s3://udend-datasets/immigration",
    file_type="JSON",
    json_paths="",
    create_table_sql=SqlQueries.stg_immigration_table_create
)

stage_temperature_to_redshift = StageToRedshiftOperator(
    task_id='Stage_temperature_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="_stg_temperature",
    s3_source="s3://udend-datasets/temperature",
    file_type="JSON",
    json_paths="",
    create_table_sql=SqlQueries.stg_temperature_table_create
)

stage_airport_to_redshift = StageToRedshiftOperator(
    task_id='Stage_airport_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="_stg_airport",
    s3_source="s3://udend-datasets/airport",
    file_type="JSON",
    json_paths="",
    create_table_sql=SqlQueries.stg_airport_table_create
)

load_immigration_fact_table = LoadFactOperator(
    task_id='Load_fact_immigration_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="_fc_immigration",
    create_table_sql=SqlQueries.fc_immigration_table_create,
    insert_sql=SqlQueries.fc_immigration_table_insert
)

load_airport_dimension_table = LoadDimensionOperator(
    task_id='Load_dim_airport_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="_dm_airport",
    create_table_sql=SqlQueries.dm_airport_table_create,
    insert_sql=SqlQueries.dm_airport_table_insert,
    truncate_data=True
)

load_immigrant_dimension_table = LoadDimensionOperator(
    task_id='Load_dim_immigrant_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="_dm_immigrant",
    create_table_sql=SqlQueries.dm_immigrant_table_create,
    insert_sql=SqlQueries.dm_immigrant_table_insert,
    truncate_data=True
)

load_temperature_dimension_table = LoadDimensionOperator(
    task_id='Load_dim_temperature_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="_dm_temperature",
    create_table_sql=SqlQueries.dm_temperature_table_create,
    insert_sql=SqlQueries.dm_temperature_table_insert,
    truncate_data=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_dim_time_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="_dm_time",
    create_table_sql=SqlQueries.dm_time_table_create,
    insert_sql=SqlQueries.dm_time_table_insert,
    truncate_data=True
)

run_data_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table="_dim_time",
    pass_value=0,
    sql_stmt=SqlQueries.count_query.format(
        "arrival_date",
        "_dim_time"
    )
)

end_operator = DummyOperator(task_id='End_execution', dag=dag)


#start_operator >> stage_immigration_to_redshift >> end_operator

start_operator >> [
    stage_airport_to_redshift,
    stage_immigration_to_redshift,
    stage_temperature_to_redshift
        ] >> load_immigration_fact_table

load_immigration_fact_table >> [
    load_temperature_dimension_table,
    load_time_dimension_table,
    load_airport_dimension_table,
    load_immigrant_dimension_table
    ]  >> run_data_quality_checks


run_data_quality_checks >> end_operator
