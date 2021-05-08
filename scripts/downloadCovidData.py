from gitUtils import GitUtils
import datetime
from pathlib import Path



myObj = GitUtils

START_DATE = '03-24-2020'  # Thats the day when the schema changed closer to the final version
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