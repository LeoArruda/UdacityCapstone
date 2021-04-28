import pygit2 as git
import logging
import requests

class GitUtils:

    def __init__(self):
        self.log = logging.Logger
    
    def syncRepository(repositoryURL, destinationPath):
        try:
            git.clone_repository(repositoryURL, destinationPath)
        except Exception as e:
            print(e)
            
        
    def downloadFile(URL='https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/', filename=None):
        import urllib.request
        try:
            req = requests.get(URL+filename)
            url_content = req.content
            return url_content
        except Exception as e:
            print(e)
 
        

    
