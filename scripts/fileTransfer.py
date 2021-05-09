import configparser
import os
import logging
import boto3
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import ParamValidationError

config = configparser.ConfigParser()
config.read('secrets/secret.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS','AWS_SECRET_ACCESS_KEY')

def upload_file(file_name, bucket, object_name=None):
    """
        Description: This function uploads a file to an S3 bucket
        
        Parameter:
            file_name: File to upload
            bucket: Bucket to upload to
            object_name: S3 object name. If not specified then file_name is used
        
        Return:
            True if file was uploaded, else False
            
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    except NoCredentialsError as e:
        logging.error(e)
        return False
    except ParamValidationError as e:
        logging.error(e)
        return False
    return True

