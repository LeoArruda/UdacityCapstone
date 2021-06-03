
# pylint: disable=missing-function-docstring

"""
### ETL DAG Tutorial Documentation
This ETL DAG is compatible with Airflow 1.10.x (specifically tested with 1.10.12) and is referenced
as part of the documentation that goes along with the Airflow Functional DAG tutorial located
[here](https://airflow.apache.org/tutorial_decorated_flows.html)
"""
# [START tutorial]
# [START import_module]
import os
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from scripts.s3_file_transfer import upload_file
from scripts.download_datasets import download_covid_data
from scripts.early_transformation import create_spark_session, transform_data_schema
from scripts.download_git_utils import GitUtils

from operators.calculate_cases import CalculateNewCasesOperator
from operators.create_tables import CreateTablesOperator
from operators.data_quality import DataQualityOperator
from operators.extract_data import ExtractDataToS3tOperator
from operators.local_to_s3 import LocalToS3Operator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.python_code import RunPythonCodeDataOperator
from operators.stage_redshift import StageToRedshiftOperator

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Leo Arruda',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60)
}
# [END default_args]

# [START instantiate_dag]
with DAG(
    'covid_data_etl_dag',
    default_args=default_args,
    description='ETL COVID DAG',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['ETL', 'Dataset'],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    # [START extract_function]
    def extract(**kwargs):
        # Downloading files from John Hopkins Institute github
        final_date = datetime.datetime.now() - datetime.timedelta(days=1)
        files = download_covid_data(end_date=final_date)
        ######################################
        # Uploading donloaded files to Amazon S3
        for file in files:
            print('Uploading file: {}'.format(file))
            filename = 'data/{}'.format(file)
            destination = 'landing/{}'.format(file)
            bucket_name = 'udacity-data-lake'
            upload_file(file_name=filename, bucket=bucket_name, object_name=destination)
    # [END extract_function]

    # [START transform_function]
    def transform(**kwargs):
        spark = create_spark_session()
        transform_data_schema(spark, input_data='data/', output_data='data/processed/')

    # [END transform_function]

    # [START load_function]
    def load(**kwargs):
        ######################## Correct ###########################
        with os.scandir('out/processed/') as it:
            for entry in it:
                if entry.name.endswith(".csv") and entry.is_file():
                    print(entry.name, entry.path)
                    # print(path_in_str)
                    file=str(entry.name)
                    filename = 'out/processed/{}'.format(file)
                    destination = 'covid19/staging/{}'.format(file)
                    bucket_name = 'udacity-data-lake'
                    upload_file(file_name=filename, bucket=bucket_name, object_name=destination)

    # [END load_function]

    # [START main_flow]
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    extract_task.doc_md = dedent(
        """\
    #### Extract task
    A simple Extract task to get data ready for the rest of the data pipeline.
    In this case, getting data is simulated by reading from a hardcoded JSON string.
    This data is then put into xcom, so that it can be processed by the next task.
    """
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    transform_task.doc_md = dedent(
        """\
    #### Transform task
    A simple Transform task which takes in the collection of order data from xcom
    and computes the total order value.
    This computed value is then put into xcom, so that it can be processed by the next task.
    """
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )
    load_task.doc_md = dedent(
        """\
    #### Load task
    A simple Load task which takes in the result of the Transform task, by reading it
    from xcom and instead of saving it to end user review, just prints it out.
    """
    )

    extract_task >> transform_task >> load_task

# [END main_flow]

# [END tutorial]