import json
import boto3

def lambda_handler(event,contents):
    
    l_bucket =  'sec.data.05.16.2020.1' 
    s3_cleint = boto3.client('s3')
    l_bucket_files = []
    
    for files in s3_cleint.list_objects(Bucket = l_bucket).get('Contents'):
                  l_bucket_files.append((files['Key']))
    return {
        'statusCode': 200,
        'body': json.dumps(l_bucket_files)
    }
