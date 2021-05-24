import pygit2 as git
import logging
import requests

class GitUtils:

    def __init__(self):
        self.log = logging.Logger
    
    def sync_repository(self, repository_URL, destinationPath):
        try:
            git.clone_repository(repository_URL, destinationPath)
        except Exception as e:
            print(e)
            
        
    def download_file(self, repository_URL='https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/', filename=None):
        try:
            req = requests.get(repository_URL+filename)
            url_content = req.content
            return url_content
        except Exception as e:
            print(e)
 
        

    
