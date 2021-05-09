from scripts import downloadCovidData, fileTransfer
import datetime

# Downloading files from John Hopkins Institute github
final_date = datetime.datetime.now() - datetime.timedelta(days=1)
files = downloadCovidData.download_covid_data(end_date=final_date)
print(files)

# Uploading donloaded files to Amazon S3
for file in files:
    print('Uploading file: {}'.format(file))
    filename = 'data/{}'.format(file)
    destination = 'landing/{}'.format(file)
    bucket_name = 'udacity-data-lake'
    fileTransfer.upload_file(file_name=filename, bucket=bucket_name, object_name=destination)

