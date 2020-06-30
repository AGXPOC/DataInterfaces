import boto3
import os
import base64
import json
import sys
from botocore.session import Session
from botocore.config import Config

def lambda_handler(s3_obj_dtls_dict, context):
    
    
    l_bucket_name = s3_obj_dtls_dict['bucket_name']
    l_file_name = s3_obj_dtls_dict['file_name']
    
#---------------------------------------------------------------------------------------------------------------------------    
    projection_list = ''
    i = 0
    for key in s3_obj_dtls_dict['projection']:
        if i == 0:
            projection_list = ' s.'+ key +' '
        else:
            projection_list = projection_list+', s.' + key +' '
         
        i=i+1
     
#---------------------------------------------------------------------------------------------------------------------------  
    filter_query = ''
    
    i = 0
    for rec in s3_obj_dtls_dict['selection']:
        if rec:
                
            if i ==0:
                filter_query = 'WHERE s.'+ rec['col_name'] + ' ' +  rec['operator'] + " '"  + rec['value']+ "'"
            else:
                filter_query = filter_query + ' AND s.'+ rec['col_name'] + ' ' +  rec['operator'] + " '"  + rec['value']+ "'"
                
            i = i+1

#---------------------------------------------------------------------------------------------------------------------------        
    s3_select_query = 'SELECT ' + projection_list +' FROM s3object s ' +  filter_query
    
    
    s3 = boto3.client('s3')
    
    print('S3 query: ',s3_select_query)
    print('Bucket_name: ',l_bucket_name)
    print('File_name: ',l_file_name)
    
    response = s3.select_object_content(Bucket = l_bucket_name,
                                        Key = l_file_name,
                                        ExpressionType = 'SQL',
                                        Expression = s3_select_query,
                                        InputSerialization = {'CSV': {#"FileHeaderInfo": "USE",
                                                                      "AllowQuotedRecordDelimiter" : True,
                                                                       'RecordDelimiter': '\n',
                                                                        'FieldDelimiter': '\t'
                                        }},
                                        OutputSerialization = {'CSV': {
                                            'FieldDelimiter': ',',
                                        }},
                                    ) 
    
    #payload_cpy = response['Payload']
    
    try:
        records = ''
        for event in response['Payload']:
            print('---------------------------------')
            if 'Records' in event:
              

                records = event['Records']['Payload'].decode('utf-8')
                print('Payload fetch complte')
                
            elif 'Stats' in event:
                statsDetails = event['Stats']['Details']
                print("Bytes scanned: ")
                print(statsDetails['BytesScanned'])
                print("Bytes processed: ")
                print(statsDetails['BytesProcessed'])
        
    except:
        e = sys.exc_info()
        print('exception occured:',e)
        
        
    return_file=  {   "statusCode": 200,
              "headers": {
                            "Content-Type": "csv",
                            "Contect-Disposition": "attachment; filename={}".format(l_file_name)
                          },
              "body": records,   
              "isBase64Encoded": True
            }
                                    
                    
    
    return return_file;  
