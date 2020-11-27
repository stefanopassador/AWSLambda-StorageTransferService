import json
import urllib.parse
import boto3

from datetime import datetime
import googleapiclient.discovery
from random import randint
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="gcp-sa.json"

print('Loading function')

s3 = boto3.client('s3')

def create_transfer_client(description, project_id, source_bucket, access_key, secret_access_key, sink_bucket, year, month, day):
    """Create a one-off transfer from Amazon S3 to Google Cloud Storage."""
    storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1')

    # Edit this template with desired parameters.
    # Specify times below using US Pacific Time Zone.
    transfer_job = {
        'description': description,
        'status': 'ENABLED',
        'projectId': project_id,
        'transferSpec': {
            'awsS3DataSource': {
                'bucketName': source_bucket,
                'awsAccessKey': {
                    'accessKeyId': access_key,
                    'secretAccessKey': secret_access_key
                }
            },
            'gcsDataSink': {
                'bucketName': sink_bucket
            },
            'transferOptions': {
                'deleteObjectsFromSourceAfterTransfer': True
            }
        },
        'schedule': {
            'scheduleStartDate': {
                'year': year,
                'month': month,
                'day': day 
            },
            'scheduleEndDate': {
                'year': year,
                'month': month,
                'day': day 
            }
        }
    }

    result = storagetransfer.transferJobs().create(body=transfer_job).execute()
    print('Returned transferJob: {}'.format(
        json.dumps(result, indent=4)))

def lambda_handler(event, context):
    description = 'Sync data from AWS to GCP'
    project_id = 'storagetransferservice-test'
    source_bucket = 'storage-transfer-service-test-gcp'
    access_key = os.getenv('ACCESS_KEY') 
    secret_access_key = os.getenv('SECRET_ACCESS_KEY') 
    sink_bucket = 'storage-transfer-service-test-destination'
    today = datetime.now()
    year = today.year
    month = today.month
    day = today.day
    create_transfer_client(
        description, 
        project_id, 
        source_bucket, 
        access_key, 
        secret_access_key, 
        sink_bucket, 
        year, 
        month, 
        day)