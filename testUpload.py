import configparser
import os
from scripts import s3_file_transfer
from pathlib import Path
config = configparser.ConfigParser()

config.read('secrets/secret.cfg')

#os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS','AWS_ACCESS_KEY_ID')
#moreos.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS','AWS_SECRET_ACCESS_KEY')

# file='03-26-2020.csv'
# filename = 'data/{}'.format(file)
# destination = 'landing/{}'.format(file)
# bucket_name = 'udacity-data-lake'

# s3_file_transfer.upload_file(file_name=filename, bucket=bucket_name, object_name=destination)

with os.scandir('out/processed/') as it:
    for entry in it:
        if entry.name.endswith(".csv") and entry.is_file():
            print(entry.name, entry.path)
            # print(path_in_str)
            file=str(entry.name)
            filename = 'out/processed/{}'.format(file)
            destination = 'covid19/staging/{}'.format(file)
            bucket_name = 'udacity-data-lake'
            s3_file_transfer.upload_file(file_name=filename, bucket=bucket_name, object_name=destination)
