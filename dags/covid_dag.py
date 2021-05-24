from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from plugins.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator, CreateTablesOperator)
from plugins.helpers.sql_queries import SqlQueries

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
          schedule_interval='@daily',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_redshift_tables = CreateTablesOperator(
    task_id='Create_tables',
    dag=dag,
    redshift_conn_id="redshift"
)

staging_covid_to_redshift = StageToRedshiftOperator(
    task_id='Staging_covid',
    dag=dag,
    table="staging_covid",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-data-lake",
    s3_key="covid19/staging/",
    region="us-west-2",
    extra_params="delimiter ';'"
)

load_location_dimension_table = LoadDimensionOperator(
    task_id='Load_location_dimension_table',
    dag=dag,
    table='dim_location',
    redshift_conn_id="redshift",
    truncate_table=True,
    load_sql_stmt=SqlQueries.location_table_insert
)

load_date_dimension_table = LoadDimensionOperator(
    task_id='Load_date_dimension_table',
    dag=dag,
    table='dim_date',
    redshift_conn_id="redshift",
    truncate_table=True,
    load_sql_stmt=SqlQueries.date_table_insert
)

load_covid_cases_fact_table = LoadFactOperator(
    task_id='Load_covid_cases_fact_table',
    dag=dag,
    table='fact_covid_cases',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.covid_cases_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    dq_checks=[
        { 'check_sql': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL', 'expected_result': 0 }, 
        { 'check_sql': 'SELECT COUNT(DISTINCT "level") FROM public.songplays', 'expected_result': 2 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.artists WHERE name IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.songs WHERE title IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.users WHERE first_name IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public."time" WHERE weekday IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.songplays sp LEFT OUTER JOIN public.users u ON u.userid = sp.userid WHERE u.userid IS NULL', \
         'expected_result': 0 }
    ],
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Tasks ordering
#

start_operator >> create_redshift_tables

create_redshift_tables >> staging_covid_to_redshift

staging_covid_to_redshift >> [load_date_dimension_table, load_location_dimension_table]

[load_date_dimension_table, load_location_dimension_table] >> load_covid_cases_fact_table

load_covid_cases_fact_table >> end_operator

#run_quality_checks >> end_operator