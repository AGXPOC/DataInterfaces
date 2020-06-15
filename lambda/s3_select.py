import boto3
import os
import base64
import json

def lambda_handler(event, context):
    
    print('event:',event)
    l_bucket_name = event['bucket']
    l_file_name = event['key']
    l_sql_query =   event['sql_query']
    l_return_obj = {1:1}
    
    s3 = boto3.client('s3')
    
   
    
    response = s3.select_object_content(Bucket = l_bucket_name,
                                        Key = l_file_name,
                                        ExpressionType = 'SQL',
                                        Expression = l_sql_query,
                                        InputSerialization = {'CSV': {#"FileHeaderInfo": "NONE"
                                        }},
                                        OutputSerialization = {'CSV': {
                                            'FieldDelimiter': ',',
                                        }},
                                    ) 
    
    #payload_cpy = response['Payload']
    
    for event in response['Payload']:
        print("in for loop")
        if 'Records' in event:
            
            records = event['Records']['Payload'].decode('utf-8')
            #records = event['Records']['Payload']
            
        elif 'Stats' in event:
            statsDetails = event['Stats']['Details']
            print("Bytes scanned: ")
            print(statsDetails['BytesScanned'])
            print("Bytes processed: ")
            print(statsDetails['BytesProcessed'])
            
        
    #json.dumps(base64.b64encode(bytes(records, encoding='utf8'))),   
    #base64.b64encode(bytes(records, encoding='utf8')),
        
    return_file=  {   "statusCode": 200,
              "headers": {
                            "Content-Type": "csv",
                            "Contect-Disposition": "attachment; filename={}".format(l_file_name)
                          },
              "body": records,   
              "isBase64Encoded": True
            }
                                    
                    
    
    return return_file;              

           
		   
'''
{
  "bucket": "sec.data.05.16.2020.1",
  "key": "NYSE.csv",
  "sql_query": "Select s._1,s._2,s._3 from s3object s where s._2 = 'ASP'"
}

'''		   
