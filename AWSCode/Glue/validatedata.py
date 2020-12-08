import boto3
import json
import time
import boto3
bucket = 'bqmigration'
schemafilebucket=bucket
schemafilepath='metadata/'
metadatadb='bq_migration_metadata'
athenaclient = boto3.client('athena')
glueclient = boto3.client('glue')
s3client = boto3.client('s3')
f = open("ValidationDiscrepancies.csv", "w")
f.write("databasename|tablename|columnname|BQcolumntype|BQValue|AWSValue|Dummy\n")
querytxt='select dataset,\"table\",column,type,validationdata from ' + metadatadb + '.bqsidevalidationdata'
athenaresponse = athenaclient.start_query_execution(QueryString=querytxt,QueryExecutionContext={'Database': metadatadb},WorkGroup='primary')
queryexecutionid=athenaresponse['QueryExecutionId']
queryresponse = athenaclient.get_query_execution(QueryExecutionId=queryexecutionid)
querystatus=queryresponse['QueryExecution']['Status']['State']
while querystatus != 'SUCCEEDED' :
    time.sleep(10)
    queryresponse = athenaclient.get_query_execution(QueryExecutionId=queryexecutionid)
    querystatus=queryresponse['QueryExecution']['Status']['State']
queryresults = athenaclient.get_query_results(QueryExecutionId=queryexecutionid,MaxResults=1000)
numrows=len(queryresults['ResultSet']['Rows'])
rowiterator=1
while rowiterator < numrows :
    databasename=queryresults['ResultSet']['Rows'][rowiterator]['Data'][0]['VarCharValue']
    tablename=queryresults['ResultSet']['Rows'][rowiterator]['Data'][1]['VarCharValue']
    columnname=queryresults['ResultSet']['Rows'][rowiterator]['Data'][2]['VarCharValue']
    coltype=queryresults['ResultSet']['Rows'][rowiterator]['Data'][3]['VarCharValue']
    validationvalue=queryresults['ResultSet']['Rows'][rowiterator]['Data'][4]['VarCharValue']
    rowiterator=rowiterator+1
#    print(databasename+'.'+tablename+'.'+columnname+ ' Type:' + coltype+' ValidationValue:'+ validationvalue+'*****')
    if coltype in ('INT64','NUMERIC','FLOAT64'):
        querytxt='select sum('+columnname+') from '+tablename
    else:
        querytxt='select count(distinct('+columnname+')) from '+tablename
    print(querytxt)
    athenaresponse = athenaclient.start_query_execution(QueryString=querytxt,QueryExecutionContext={'Database': databasename},WorkGroup='primary')
    queryexecutionid=athenaresponse['QueryExecutionId']
    queryresponse = athenaclient.get_query_execution(QueryExecutionId=queryexecutionid)
    querystatus=queryresponse['QueryExecution']['Status']['State']
    while (querystatus != 'SUCCEEDED' and querystatus != 'FAILED'):
        time.sleep(10)
        queryresponse = athenaclient.get_query_execution(QueryExecutionId=queryexecutionid)
        querystatus=queryresponse['QueryExecution']['Status']['State']
    if (querystatus=='SUCCEEDED'):
        valqueryresults = athenaclient.get_query_results(QueryExecutionId=queryexecutionid,MaxResults=1000)
        athenavalue=valqueryresults['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
#        print(athenavalue)
#        print(validationvalue)
        if(float(athenavalue)!=float(validationvalue)):
            print('Write to File')
            newcolumnstring=databasename+'|'+tablename+'|'+columnname+'|'+coltype+'|'+validationvalue+'|'+athenavalue+'|0'+'\n'
            f.write(newcolumnstring)
        else:
            print('Dont write to File')
    else:
        print('Skipping row as Athena query failed')
f.close()
s3client = boto3.client('s3')
s3client.upload_file("ValidationDiscrepancies.csv", schemafilebucket, schemafilepath+"Validation_Discrepancies/ValidationDiscrepancies.csv")
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



    
