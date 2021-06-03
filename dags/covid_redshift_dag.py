from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from operators.calculate_cases import CalculateNewCasesOperator
from operators.create_tables import CreateTablesOperator
from operators.data_quality import DataQualityOperator
from operators.extract_data import ExtractDataToS3tOperator
from operators.local_to_s3 import LocalToS3Operator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.python_code import RunPythonCodeDataOperator
from operators.stage_redshift import StageToRedshiftOperator
from helpers import SqlQueries

default_args = {
    'owner': 'Leo Arruda',
    'start_date': datetime(2020, 4, 23),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60)
}

dag = DAG('capstone_covid_airflow',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None,
          catchup=False,
          tags=['Load', 'Dataset', 'Redshift']
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_redshift_tables = CreateTablesOperator(
    task_id='Create_tables',
    dag=dag,
    redshift_conn_id="redshift"
)

stage_covid_to_redshift = StageToRedshiftOperator(
    task_id='Stage_covid',
    dag=dag,
    table="staging_covid",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-data-lake",
    s3_key="covid19/staging",
    region="us-west-2",
    extra_params="delimiter ';'"
)

load_covid_cases_fact_table = LoadFactOperator(
    task_id='Load_covid_cases_fact_table',
    dag=dag,
    table='fact_covid_cases',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.covid_cases_insert
)

load_dim_location_table = LoadDimensionOperator(
    task_id='Load_dim_location_table',
    dag=dag,
    table='dim_location',
    redshift_conn_id="redshift",
    truncate_table=True,
    load_sql_stmt=SqlQueries.location_table_insert
)

load_dim_date_table = LoadDimensionOperator(
    task_id='Load_dim_date_table',
    dag=dag,
    table='dim_date',
    redshift_conn_id="redshift",
    truncate_table=True,
    load_sql_stmt=SqlQueries.date_table_insert
)

calculate_new_cases_on_fact = CalculateNewCasesOperator(
    task_id='Calculate_new_cases_on_fact',
    dag=dag,
    redshift_conn_id="redshift",
    calculate_sql_stmt=SqlQueries.calculate_new_cases
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    dq_checks=[
        { 'check_sql': 'SELECT COUNT(*) FROM public.fact_covid_cases WHERE Combined_key IS NULL', 'expected_result': 0 }, 
        { 'check_sql': 'SELECT COUNT(*) FROM public.dim_date WHERE Datekey IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.dim_location WHERE Combined_key IS NULL', 'expected_result': 0 }
    ],
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Tasks ordering
#

start_operator >> create_redshift_tables

create_redshift_tables >> stage_covid_to_redshift

stage_covid_to_redshift >> [load_dim_location_table, load_dim_date_table]

[load_dim_location_table, load_dim_date_table] >> load_covid_cases_fact_table

load_covid_cases_fact_table >> calculate_new_cases_on_fact

calculate_new_cases_on_fact >> run_quality_checks

