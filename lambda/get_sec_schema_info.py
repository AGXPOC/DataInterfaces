import boto3

def lambda_handler(event, context):
    
    
    l_database_name = 's3_sec_database'
    l_table_name = 'sec_'+ event['file_name'].replace('.', '_')
    
    print('Database name:',l_database_name,', table_name',l_table_name)
    
    glue_cleint = boto3.client('glue')
    
    response = glue_cleint.get_table(DatabaseName=l_database_name,
                                     Name=l_table_name
                                      )
                                      
    l_schema_json_object = response['Table']['StorageDescriptor']['Columns']
    
    return l_schema_json_object
	
'''
{
  "file_name": "sub.txt"
}

Glue data base table format: sec_sub_txt
'''	
