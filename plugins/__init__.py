from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin
from operators import (CreateTablesOperator, DataQualityOperator, ExtractDataToS3tOperator, LoadDimensionOperator, LoadFactOperator, LocalToS3Operator, RunPythonCodeDataOperator, CalculateNewCasesOperator, StageToRedshiftOperator)
from helpers import SqlQueries
from scripts import (download_covid_data, early_transformation, GitUtils, s3_file_transfer)


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        CreateTablesOperator,
        DataQualityOperator, 
        ExtractDataToS3tOperator,
        LoadDimensionOperator, 
        LoadFactOperator,
        LocalToS3Operator,
        RunPythonCodeDataOperator,
        CalculateNewCasesOperator,
        StageToRedshiftOperator,

    ]
    helpers = [
        SqlQueries
    ]

    scripts = [
        download_covid_data,
        early_transformation,
        GitUtils,
        s3_file_transfer
    ]
