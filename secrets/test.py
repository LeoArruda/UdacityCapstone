from scripts.gitUtils import GitUtils
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