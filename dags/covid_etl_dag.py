
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
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago
from scripts.s3_file_transfer import upload_file
from scripts.download_datasets import download_covid_data

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Leo Arruda',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 0,
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
        final_date = datetime.now() - timedelta(days=1)
        download_covid_data(end_date=final_date)
        ######################################
        # Uploading donloaded files to Amazon S3
        with os.scandir('/Users/leandroarruda/Codes/UdacityCapstone/data/') as it:
            for entry in it:
                if entry.name.endswith(".csv") and entry.is_file():
                    print(entry.name, entry.path)
                    # print(path_in_str)
                    file=str(entry.name)
                    filename = '/Users/leandroarruda/Codes/UdacityCapstone/data/{}'.format(file)
                    destination = 'landing/{}'.format(file)
                    bucket_name = 'udacity-data-lake'
                    upload_file(file_name=filename, bucket=bucket_name, object_name=destination)
    # # [END extract_function]

    # [START transform_function]
    def transform(**kwargs):
        import os
        from pyspark.sql.functions import col, when, concat_ws, to_date
        from pyspark.sql.functions import to_timestamp, lit, date_format, trim, length
        import pyspark.sql.functions as F
        from pyspark.sql.types import StructField, StringType

        #os.environ['JAVA_HOME'] = "/Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home"
        os.environ['PYSPARK_SUBMIT_ARGS'] = """--name job_name --master local --conf spark.dynamicAllocation.enabled=true pyspark-shell""" 

        from pyspark.sql import SparkSession
        spark = SparkSession \
            .builder \
            .appName("Early Transformations") \
            .getOrCreate()

        input_data='/Users/leandroarruda/Codes/UdacityCapstone/data/'
        output_data='/Users/leandroarruda/Codes/UdacityCapstone/data/processed/'

        # get filepath to song data file
        raw_data = input_data + '*.csv'

        # read raw data file
        df = spark.read \
                .option("header",True) \
                .option("inferSchema",True) \
                .csv(raw_data)

        df= df \
            .withColumn("Country_Region", 
            when(df["Country_Region"].contains("China"),"China")
            .otherwise(df["Country_Region"]))

        df= df \
            .withColumn("Country_Region", 
            when(df["Country_Region"].contains("Republic of Korea"),"Korea, South")
            .otherwise(df["Country_Region"]))

        df= df \
            .withColumn("Country_Region", 
            when(df["Country_Region"].contains("Cote d'Ivoir"),"Cote d Ivoir")
            .otherwise(df["Country_Region"]))

        df= df \
            .withColumn("Country_Region", 
            when(df["Country_Region"].contains("Taiwan*"),"Taiwan")
            .otherwise(df["Country_Region"]))

        df= df \
            .withColumn("Combined_Key", 
            when(df["Country_Region"].isNull(),
            concat_ws(", ", df["Province_State"], df["Country_Region"]))
            .otherwise(df["Combined_Key"]))

        df= df \
            .withColumn("Combined_Key", 
            when(df["Combined_Key"].contains("Taiwan*"),"Taiwan")
            .otherwise(df["Combined_Key"]))

        df= df \
            .withColumn("CaseFatalityRatio", (df.Deaths/df.Confirmed)*100)

        if not (StructField("Incidence_Rate",StringType(),True) in df.schema):
            df = df \
                .withColumn("Incidence_Rate", lit(0.0))

        df = df.withColumn('Last_date', F.col('Last_Update').cast("timestamp")) 
        df = df.withColumn('Last_date', F.split("Last_Update", " ").getItem(0))
        df=df.filter(length(df.Last_date)==10)
        df = df \
            .withColumn("Last_up", to_date("Last_date", "yyyy-MM-dd"))

        df = df \
            .withColumn("Year", date_format(col("Last_up"), "yyyy")) \
            .withColumn("Month", date_format(col("Last_up"), "MM")) \
            .withColumn("Day", date_format(col("Last_up"), "dd")) \
            .withColumn("Datekey", date_format(col("Last_Up"), "yyyyMMdd")) \
            .withColumn("newDate", date_format(col("Last_Up"), "yyyy-MM-dd"))

        df = df \
            .withColumn("Combined_Key2", 
                        trim(concat_ws(', ',trim(df['Province_State']),trim(df['Country_Region']))))

        df_final = df.select(
                        col('Datekey'), \
                        col('Year'), \
                        col('Month'), \
                        col('Day'), \
                        col('Province_State').alias('State'), \
                        col('Country_Region').alias('Country'), \
                        col('newDate'), \
                        col('Lat').alias('Latitude'), \
                        col('Long_').alias('Longitude'), \
                        col('Confirmed'), \
                        col('Deaths'), \
                        col('Recovered'), \
                        col('Active'), \
                        col('Incidence_Rate'), \
                        col('CaseFatalityRatio'), \
                        col('Combined_Key2'))

        df_final = df_final.orderBy(['Country','State','Datekey'], ascending=True)
        df_final.write \
            .format("com.databricks.spark.csv") \
            .mode("overwrite") \
            .option("header",False) \
            .option("escape", "") \
            .option("quote", "") \
            .option("emptyValue", "") \
            .option("delimiter", ";") \
            .save(output_data)

    # [END transform_function]

    # [START load_function]
    def load(**kwargs):
        ######################## Correct ###########################
        with os.scandir('/Users/leandroarruda/Codes/UdacityCapstone/data/processed/') as it:
            for entry in it:
                if entry.name.endswith(".csv") and entry.is_file():
                    print(entry.name, entry.path)
                    # print(path_in_str)
                    file=str(entry.name)
                    filename = '/Users/leandroarruda/Codes/UdacityCapstone/data/processed/{}'.format(file)
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

# if __name__ == '__main__':
#     from airflow.utils.state import State
#     dag.clear(dag_run_state=State.NONE)
#     dag.run()