
import configparser
import os
from scripts import fileTransfer
config = configparser.ConfigParser()

config.read('secrets/secret.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS','AWS_SECRET_ACCESS_KEY')

file='03-26-2020.csv'
filename = 'data/{}'.format(file)
destination = 'landing/{}'.format(file)
bucket_name = 'udacity-data-lake'

fileTransfer.upload_file(file_name=filename, bucket=bucket_name, object_name=destination)

