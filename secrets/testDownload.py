from scripts import download_covid_data, s3_file_transfer
import datetime

# Downloading files from John Hopkins Institute github
final_date = datetime.datetime.now() - datetime.timedelta(days=1)
files = download_covid_data.download_covid_data(end_date=final_date)
print(files)

# Uploading donloaded files to Amazon S3
for file in files:
    print('Uploading file: {}'.format(file))
    filename = 'data/{}'.format(file)
    destination = 'landing/{}'.format(file)
    bucket_name = 'udacity-data-lake'
    s3_file_transfer.upload_file(file_name=filename, bucket=bucket_name, object_name=destination)

