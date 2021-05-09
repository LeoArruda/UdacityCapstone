import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when
from pyspark.sql.functions import day, year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp
from pyspark.sql.functions import monotonically_increasing_id, array_contains, concat_ws, to_date, lit
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType

config = configparser.ConfigParser()
config.read('secrets/secret.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Create or retrieve a Spark Session
    """
    # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:jar:3.2.1") \
        .getOrCreate()
    return spark


def transform_data_schema(spark, input_data, output_data):
    """
        Description: This function transform csv files, with different schemas into a common standard schema.
                     It also standardize country names, convert date format, and calculates missing Incidence_rate
        
        Parameters:
            spark       : Spark Session
            input_data  : location of song_data json files with the songs metadata
            output_data : S3 bucket were dimensional tables in parquet format will be stored
    """
    # get filepath to song data file
    raw_data = input_data + 'data/*.csv'

    # read raw data file
    df = spark.read \
            .option("header",True) \
            .option("inferSchema",True) \
            .csv(raw_data)

    # jjjjj
    df = df \
            .withColumn("Country_Region", when(df["Country_Region"].contains("China"),"China")
            .otherwise(df["Country_Region"]))

    df = df \
            .withColumn("Country_Region", 
            when(df["Country_Region"].contains("Republic of Korea"),"Korea, South")
            .otherwise(df["Country_Region"]))
    
    df = df \
            .withColumn("Combined_Key", 
            when(df["Country_Region"].isNull(),
            concat_ws(", ", df["Province_State"], df["Country_Region"]))
            .otherwise(df["Combined_Key"]))
    
    df = df \
        .withColumn("Country_Region", 
        when(df["Country_Region"].contains("Cote d'Ivoir"),"Cote d Ivoir")
        .otherwise(df["Country_Region"]))
    
    df = df \
            .withColumn("CaseFatalityRatio", (df.Deaths/df.Confirmed)*100)

    if not (StructField("Incidence_Rate",StringType(),True) in df.schema):
        df= df \
            .withColumn("Incidence_Rate", 
            lit(""))
    
    df = df.withColumn("Last_up", to_timestamp("Last_Update", "yyyy-MM-dd HH:mm:ss"))

    df = df \
        .withColumn("Date",
        to_date(col("Last_up"),"yyyy-MM-dd")
                    )
    
    df = df.select(
                 col('Province_State').alias('State'), \
                 col('Country_Region').alias('Country'), \
                 col('Date'), \
                 col('Lat').alias('Latitude'), \
                 col('Long_').alias('Longitude'), \
                 col('Confirmed'), \
                 col('Deaths'), \
                 col('Recovered'), \
                 col('Active'), \
                 col('Incidence_Rate'), \
                 col('CaseFatalityRatio'), \
                 col('Combined_Key'))

    dataset_fields = ['Province_State as State', 'Country_Region as Country', 'Lat as Latitude', 'Long_ as Longitude', 'Confirmed', 'Deaths', 'Recovered', 'Active', 'Incidence_Rate', 'CaseFatalityRatio', 'Combined_Key']

    # extract columns to create songs table
    dataset_table = dataset_table = df.select(dataset_fields)
    
    # write songs table to parquet files partitioned by year and artist
    dataset_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/')

    # define artists fields
    artists_fields = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"]

    # extract columns to create artists table
    artists_table = df.selectExpr(artists_fields).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """
        Description: This function loads log_data from S3 and processes it by extracting the songs and artist tables
        and then again loaded back to S3. Also output from previous function is used in by spark.read.json command
        
        Parameters:
            spark       : Spark Session
            input_data  : location of log_data json files with the events data
            output_data : S3 bucket were dimensional tables in parquet format will be stored
            
    """
    songSchema = StructType([
        StructField("artist_id",StringType()),
        StructField("artist_latitude",DoubleType()),
        StructField("artist_location",StringType()),
        StructField("artist_longitude",DoubleType()),
        StructField("artist_name",StringType()),
        StructField("duration",DoubleType()),
        StructField("num_songs",IntegerType()),
        StructField("title",StringType()),
        StructField("year",IntegerType()),
    ])
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_fields = ["userdId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df.selectExpr(users_fields).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_datetime = udf(date_convert, TimestampType())
    df = df.withColumn("start_time", get_datetime('ts'))
    
    # create datetime column from original timestamp column
    time_table = df.select("start_time").dropDuplicates() \
        .withColumn("hour", hour(col("start_time"))) \
        .withColumn("day", day(col("start_time"))) \
        .withColumn("week", week(col("start_time"))) \
        .withColumn("month", month(col("start_time"))) \
        .withColumn("year", year(col("start_time"))) \
        .withColumn("weekday", date_format(col("start_time"), 'E'))
                   
    # extract columns to create time table
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time/')

    # write time table to parquet files partitioned by year and month
    #time_table.write.parquet(output_data + 'time/')

    # read in song data to use for songplays table
    df_songs = spark.read.parquet(output_data + 'songs/*/*/*')

    df_artists = spark.read.parquet(output_data + 'artists/*')

    songs_logs = df.join(songs_df, (df.song == songs_df.title))
    artists_songs_logs = songs_logs.join(df_artists, (songs_logs.artist == df_artists.name))


    # extract columns from joined song and log datasets to create songplays table 
    songplays = artists_songs_logs.join(
        time_table,
        artists_songs_logs.ts == time_table.start_time, 'left'
    ).drop(artists_songs_logs.year)

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays.select(
        col('start_time').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        col('month').alias('month'),
    ).repartition("year", "month")

    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/')


def main():
    """
        Extract songs and events data from S3, Transform it into dimensional tables format, and Load it back to S3 in Parquet format
    """
    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    output_data = "s3://udacity-leo/data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
