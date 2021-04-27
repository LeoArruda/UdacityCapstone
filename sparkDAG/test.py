# import configparser
# from datetime import datetime
# import os
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import udf, col
# from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


# config = configparser.ConfigParser()
# config.read_file(open('dl.cfg'))

# os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
# os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


# def create_spark_session():
#     spark = SparkSession \
#         .builder \
#         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
#         .getOrCreate()
#     return spark

from gitUtils import GitUtils
import datetime
import csv
from pathlib import Path

myObj = GitUtils

#StartDate = '01-22-2020'
START_DATE = '01-22-2020'
END_DATE = datetime.datetime.now() - datetime.timedelta(days=1)

Date = datetime.datetime.strptime(START_DATE, "%m-%d-%Y")

while Date.date() <= END_DATE.date():
    filename='{date.month:02}-{date.day:02}-{date.year}.csv'.format(date=Date)
    print('Processing file: {}'.format(filename))
    myCSV = myObj.downloadFile(filename=filename)
    data_folder = Path('data/')
    dataset = data_folder / filename
    try:
        csv_file = open(dataset, 'wb')
        csv_file.write(myCSV)
        csv_file.close()
    except Exception as e:
        print(e)
        raise
    Date += datetime.timedelta(days=1)

print(datetime.datetime.now()-END_DATE)