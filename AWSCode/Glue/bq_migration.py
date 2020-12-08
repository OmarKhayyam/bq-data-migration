import boto3
import json
import time
import boto3
bucket = 'bqmigration'
prefix = 'data-test-5/'
schemafilebucket=bucket
schemafilepath='metadata/'
rolename='arn:aws:iam::139049702594:role/GlueS3access'
metadatadb='bq_migration_metadata'
startpos=len(prefix)
s3client = boto3.client('s3')
glueclient = boto3.client('glue')
#athenaclient = boto3.client('athena')
response = glueclient.create_database(
DatabaseInput={'Name': metadatadb}
)
f = open("AmazonS3schema.csv", "w")
f.write("databasename|tablename|columnname|type|distinctcountorsum\n")
s3result = s3client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
for o in s3result.get('CommonPrefixes'):
    fullfoldername=o.get('Prefix')
    endpos=len(fullfoldername)-1
    db_name=fullfoldername[startpos:endpos]
    migrated_data_path='s3://'+bucket+'/'+prefix+db_name+'/'
    response = glueclient.create_database(
    DatabaseInput={'Name': db_name}
    )
    response = glueclient.create_crawler(
        Name='bq_migration_crawler'+db_name,
        Role=rolename,
        DatabaseName=db_name,
        Targets={
            'S3Targets': [
                {
                    'Path': migrated_data_path
                },
            ]
        }
        
    )

    response = glueclient.get_crawler(
        Name='bq_migration_crawler'+db_name
    )
    status=response['Crawler']['State']
    while status != 'READY' :
        time.sleep(10)
        response = glueclient.get_crawler(
            Name='bq_migration_crawler'+db_name
        )
        status=response['Crawler']['State']

    response = glueclient.start_crawler(
        Name='bq_migration_crawler'+db_name
    )

    response = glueclient.get_crawler(
        Name='bq_migration_crawler'+db_name
    )
    status=response['Crawler']['State']
    while status != 'READY' :
        time.sleep(10)
        response = glueclient.get_crawler(
            Name='bq_migration_crawler'+db_name
        )
        status=response['Crawler']['State']


    response = glueclient.get_tables(
        DatabaseName=db_name,
        MaxResults=1000
    )
    columnstring=''
    cnt_tables=len(response['TableList'])
    table_iter=0
    while table_iter < cnt_tables :
        tablename=response['TableList'][table_iter]['Name']
        tablestrresponse=glueclient.get_table(
            DatabaseName=db_name,
            Name=tablename
            )
        cnt_columns=len(tablestrresponse['Table']['StorageDescriptor']['Columns'])
        col_iter=0
        while col_iter < cnt_columns:
            columnname=tablestrresponse['Table']['StorageDescriptor']['Columns'][col_iter]['Name']
            columntype=tablestrresponse['Table']['StorageDescriptor']['Columns'][col_iter]['Type']
            distcountorsum='0'
            newcolumnstring=db_name+'|'+tablename+'|'+columnname+'|'+columntype+'|'+distcountorsum+'\n'
            f.write(newcolumnstring)
            col_iter=col_iter+1
        cnt_columns=len(tablestrresponse['Table']['PartitionKeys'])
        col_iter=0
        while col_iter < cnt_columns:
            columnname=tablestrresponse['Table']['PartitionKeys'][col_iter]['Name']
            columntype=tablestrresponse['Table']['PartitionKeys'][col_iter]['Type']
            distcountorsum='0'
            newcolumnstring=db_name+'|'+tablename+'|'+columnname+'|'+columntype+'|'+distcountorsum+'\n'
            f.write(newcolumnstring)
            col_iter=col_iter+1
        table_iter=table_iter+1
f.close()
s3client = boto3.client('s3')
s3client.upload_file("AmazonS3schema.csv", schemafilebucket, schemafilepath+"AmazonS3schema/AmazonS3schema.csv")


response = glueclient.create_crawler(
    Name='bq_check_crawler',
    Role=rolename,
    DatabaseName=metadatadb,
    Targets={
        'S3Targets': [
            {
                'Path': "s3://"+schemafilebucket+"/"+schemafilepath + "AmazonS3schema/"
            },
                        {
                'Path': "s3://"+schemafilebucket+"/"+schemafilepath + "BQSideValidationData/"
            },
                        {
                'Path': "s3://"+schemafilebucket+"/"+schemafilepath + "BQStats/"
            },
                        {
                'Path': "s3://"+schemafilebucket+"/"+schemafilepath + "BQTableRows/"
            },
            
                        {
                'Path': "s3://"+schemafilebucket+"/"+schemafilepath + "Validation_Discrepancies/"
            }
        ]
    }
    
)

response = glueclient.get_crawler(
    Name='bq_check_crawler'
)
status=response['Crawler']['State']
while status != 'READY' :
    time.sleep(10)
    response = glueclient.get_crawler(
		Name='bq_check_crawler'
	)
    status=response['Crawler']['State']
print("Crawler Created successfully")

print("Starting Crawler")
response = glueclient.start_crawler(
    Name='bq_check_crawler'
)
print("Started Crawler")

response = glueclient.get_crawler(
    Name='bq_check_crawler'
)
status=response['Crawler']['State']
while status != 'READY' :
    time.sleep(10)
    response = glueclient.get_crawler(
		Name='bq_check_crawler'
	)
    status=response['Crawler']['State']
print("Crawler Executed successfully")
