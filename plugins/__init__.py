from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin
from operators.calculate_cases import CalculateNewCasesOperator
from operators.create_tables import CreateTablesOperator
from operators.data_quality import DataQualityOperator
from operators.extract_data import ExtractDataToS3tOperator
from operators.local_to_s3 import LocalToS3Operator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.python_code import RunPythonCodeDataOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.spark_code import RunSparkCodeDataOperator
from helpers.sql_queries import SqlQueries
from scripts.download_datasets import download_covid_data
from scripts.download_git_utils import GitUtils
from scripts.early_transformation import create_spark_session, transform_data_schema
from scripts.s3_file_transfer import upload_file


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        CalculateNewCasesOperator,
        CreateTablesOperator,
        DataQualityOperator,
        ExtractDataToS3tOperator,
        LocalToS3Operator,
        LoadFactOperator,
        LoadDimensionOperator,
        RunPythonCodeDataOperator,
        StageToRedshiftOperator,
        RunSparkCodeDataOperator,
    ]

    helpers = [
        SqlQueries
    ]

    scripts = [
        download_covid_data,
        GitUtils,
        create_spark_session,
        transform_data_schema,
        upload_file
    ]
