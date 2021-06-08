from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StringType
from pyspark.sql.functions import col, when, concat_ws, to_date
from pyspark.sql.functions import to_timestamp, lit, date_format, trim, length
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType

os.environ['PYSPARK_SUBMIT_ARGS'] = """--name job_name --master local --conf spark.dynamicAllocation.enabled=true pyspark-shell""" 



def create_spark_session():
    """
        Create or retrieve a Spark Session
    """
    # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    spark = SparkSession \
            .builder \
            .appName("Early Transformations") \
            .getOrCreate()
    return spark


def transform_data_schema(input_data, output_data):
    """
        Description: This function transform csv files, with different schemas into a common standard schema.
                     It also standardize country names, convert date format, and calculates missing Incidence_rate
                     occurrences.
        
        Parameters:
            spark       : Spark Session
            input_data  : location of song_data json files with the songs metadata
            output_data : S3 bucket were dimensional tables in parquet format will be stored
    """
    # get filepath to song data file
    raw_data = input_data + '*.csv'

    spark = create_spark_session()

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
