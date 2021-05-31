import datetime
from pathlib import Path
import logging
import requests


def download_file(url='https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/', filename=None):
        try:
            req = requests.get(url+filename)
            url_content = req.content
            return url_content
        except Exception as e:
            logging.error(e)


def download_covid_data(start_date='03-24-2020', end_date=None):
    """
        Description: This function download COVID-19 data files from John Hopkins University Github
                    and stores into ~/data folder
        
        Parameters:
            start_date  : the initial date to start Format=MM-dd-yyyy.  Default='03-24-2020'  
            end_date    : the final date to download.                   Default=**Last Day**
        
        Returns:
            downloaded_files[] : An array list with all downloaded files
            
    """

    downloaded_files = []

    # '03-24-2020' Thats the day when the schema changed closer to the final version
    # END_DATE = datetime.datetime.now() - datetime.timedelta(days=1)

    if end_date == None:
        end_date = datetime.datetime.now() - datetime.timedelta(days=1)

    date_to_process = datetime.datetime.strptime(start_date, "%m-%d-%Y")

    while date_to_process.date() <= end_date.date():
        filename='{date.month:02}-{date.day:02}-{date.year}.csv'.format(date=date_to_process)
        #print('Processing file: {}'.format(filename))
        jhu_csv = download_file(filename=filename)
        data_folder = Path('data/')
        dataset = data_folder / filename
        try:
            csv_file = open(dataset, 'wb')
            csv_file.write(jhu_csv)
            csv_file.close()
            downloaded_files.append(filename)
        except Exception as e:
            logging.error(e)
            raise
        date_to_process += datetime.timedelta(days=1)
    return(downloaded_files)