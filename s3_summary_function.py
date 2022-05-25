import boto3
import pandas as pd
import datetime

session = boto3.Session( 
         aws_access_key_id='<your_access_key_id>', 
         aws_secret_access_key='<your_secret_access_key>')

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def get_prefix(bucket):
    '''
    Extract folder names or prefixes in a S3 bucket

    :param bucket: Name of the S3 bucket
    :ptype bucket: str

    :returns: list of 

    '''
    folders = set()
    for object in s3_resource.Bucket(bucket).objects.all():
        if object.size > 0 and '/' in object.key:
            folders.add(object.key[:object.key.rfind('/')])
    prefixes = list(folders)
    return prefixes

def get_summary(bucket, prefixes):
    s3_summary = pd.DataFrame(columns = ['folder', 'file', 'last_modified_date'])
    for prefix in prefixes:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = response.get("Contents")
        filename = []
        last_modified = []
        for file in files:
            filename.append(file['Key'])
            last_modified.append(file['LastModified'])
        folder_dict = {'filename' : filename, 'last_modified' : last_modified}
        folder_summary = pd.DataFrame.from_dict(folder_dict)
        folder_summary[['folder','file']] = folder_summary.filename.str.rsplit('/', 1).tolist()
        folder_summary['last_modified_date'] = pd.to_datetime(folder_summary['last_modified']).dt.date
        folder_summary = folder_summary[folder_summary.last_modified_date == folder_summary.last_modified_date.max()]
        folder_summary = folder_summary[folder_summary.file == folder_summary.file.max()]
        folder_summary = folder_summary[['folder', 'file', 'last_modified_date']]
        s3_summary['extracted_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
        s3_summary = s3_summary.append(folder_summary, True)
        return s3_summary


prefixes = get_prefix('<BUCKET_NAME>')




