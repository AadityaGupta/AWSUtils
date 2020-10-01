import tempfile
import tarfile
import io
import boto3
from botocore.config import Config

my_config = Config(
    region_name = 'us-east-2',
    signature_version = 's3v4',
    retries = {
        'max_attempts': 10,
        'mode': 'standard'
    }
)

s3_client = boto3.client('s3', config=my_config)

bucket = '<bucket-name>'
tar_temp_file = tempfile.mktemp()
def iterate_over_s3_bucket_items(bucket):
    '''
    Logic for generator that iterates over all objects in a given s3 bucket
    :param bucket: name of s3 bucket
    :return: dict of metadata for an object
    '''
    
    paginator = s3_client.get_paginator('list_objects_v2')
    bucket_page_iterator = paginator.paginate(Bucket='<bucket-name>')

    for page in bucket_page_iterator:
        if page['KeyCount'] > 0:
            for item in page['Contents']:
                yield item

def upload_to_S3(tarfile,finalkeyname,bucket):
    '''
    Function that uploads the output file extracted from tar file
    :param tarfile:extracted file from tar file;finalkeyname:name of key for uploaded file
    :param bucket: name of s3 bucket
    :return: status of upload as failure or success
    '''
    s3_upload_status = 'success'

    try:
        s3_client.upload_fileobj(
            io.BytesIO(tarfile),
            bucket,
            finalkeyname
        )
    except Exception as e:
        s3_upload_status = 'failure'
        print(e)
    finally:
        return s3_upload_status


def download_tarfile_and_process(download_keyname,upload_keyname):
    '''
    Function that downloads the tarfile from the S3 bucket, extracts the file and calls upload function
    :param download_keyname:Name of the key of source file location
    :param upload_keyname:Name of the key of upload file location
    '''
    
    s3_client.download_file(bucket, download_keyname, tar_temp_file)
    with tarfile.open(tar_temp_file,mode='r:gz') as tardata:
        try:
            file = tardata.extractfile('output').read()
            #print(type(file))
            #finalfilename='reviews.csv'
            print (upload_to_S3(file,upload_keyname,bucket))
        except KeyError:
            print('ERROR: Not found in tar archive'.format(
                    'output'))
    
    
for bucket_item in iterate_over_s3_bucket_items(bucket):
    downloaded_keyname=bucket_item['Key']
    print(downloaded_keyname)
    if 'reviews/output/' in downloaded_keyname and '/output/output.tar.gz' in downloaded_keyname :
        
        download_tarfile_and_process(downloaded_keyname,'gluejobreviews/reviews.csv')
    elif 'tweets/output/' in downloaded_keyname and '/output/output.tar.gz' in downloaded_keyname :

        download_tarfile_and_process(downloaded_keyname,'gluejobtweets/tweets.csv')   
        
        