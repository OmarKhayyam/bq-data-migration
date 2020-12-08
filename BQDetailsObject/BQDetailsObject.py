#!/usr/bin/env python

import random
import csv
import os
import boto3
from botocore.exceptions import ClientError
from re import search
from google.cloud import bigquery

numeric_types = ['INT64','NUMERIC','FLOAT64']
complex_types = ['ARRAY','STRUCT']

class BQDetailsObject:
    def __init__(self):
        self.thisclient = bigquery.Client()
        self.col_list = list()
        self.table_list = list()

    def getAllDatasets(self):
        self.dsls = self.thisclient.list_datasets()        

    def getAllTablesForDataset(self,dataset_id):
        self.tblls = self.thisclient.list_tables(dataset_id)

    def getTable(self,dataset_id,table_id):
        return self.thisclient.get_table(dataset_id+'.'+table_id)

    def isPartitioned(self,dataset_id,table_id):
        tbl = self.thisclient.get_table(dataset_id+'.'+table_id)        
        if tbl.partitioning_type == None and tbl.range_partitioning == None:
            return False
        return True

    def partitionType(self,dataset_id,table_id):
        tbl = self.thisclient.get_table(dataset_id+'.'+table_id)
        if tbl.partitioning_type != None:
            return 'TIME'
        else:
            return 'RANGE'

    def isIngestPartitioned(self,dataset_id,table_id): ## Check if this table is ingest partitioned
        tbl = self.thisclient.get_table(dataset_id+'.'+table_id)
        if tbl.partitioning_type == 'DAY' or tbl.partitioning_type == 'HOUR':
            if tbl.time_partitioning.field == None:
                return True ## This means its partitioned on __PARTITIONTIME
        else:
            return False

    def isIngestPartitionedHourly(self,dataset_id,table_id): ## Check if this table is ingest partitioned
        tbl = self.thisclient.get_table(dataset_id+'.'+table_id)
        print("Partitioning Type: {}".format(tbl.partitioning_type))
        if tbl.partitioning_type == 'HOUR':
            if tbl.time_partitioning.field == None:
                return True ## This means its partitioned on __PARTITIONTIME
        else:
            return False
            
    def detailsOfTimePartition(self,dataset_id,table_id):
        tbl = self.thisclient.get_table(dataset_id+'.'+table_id)
        return tbl.time_partitioning.type_,tbl.time_partitioning.field
        

    ## For details of __NONE__ and __UNPARTITIONED__ partitions, have a look at this link:
    ## https://cloud.google.com/bigquery/docs/managing-partitioned-table-data
    def getTimePartitions(self,table_id,dataset_id):
        partlist = list()
        tbl = self.thisclient.get_table(dataset_id+'.'+table_id)
        fld = tbl.time_partitioning.field
        if fld == None:
            fld = '_PARTITIONDATE'
        qry = 'select ' + fld + ' as pt from `' + dataset_id + '.' + table_id + '` group by ' + fld + ' order by ' + fld
        qj = self.thisclient.query(qry) 
        results = qj.result()
        for elem in results:
            year,month,day = str(elem[0]).split('-')
            partlist.append(year+month+day)
        return partlist

    def getTimePartitionsHourly(self,table_id,dataset_id):
        partlist = list()
        qry = 'select _PARTITIONTIME as pt from `' + dataset_id + '.' + table_id + '` group by _PARTITIONTIME order by _PARTITIONTIME'
        qj = self.thisclient.query(qry)
        results = qj.result()
        for elem in results:
            partlist.append((elem[0].year,elem[0].month,elem[0].day,elem[0].hour))
        return partlist

    def getRangePartitions(self,table_id,dataset_id):
        tbl = self.thisclient.get_table(dataset_id+'.'+table_id)
        return tbl.range_partitioning.range_
         
    def getColumnDetails(self,dataset_id,table_id):
        collist = list()
        qry = 'SELECT * EXCEPT(is_generated, generation_expression, is_stored, is_updatable) FROM ' + dataset_id + '.INFORMATION_SCHEMA.COLUMNS WHERE table_name=' + '"' + table_id + '"' + ' and is_hidden = "NO" and is_system_defined = "NO"'
        qj = self.thisclient.query(qry)
        results = qj.result()
        for row in results:
            collist.append((row[1],row[2],row[3],row[6]))
        return collist

    def getColumnValidationInputDataNumeric(self,tuplist): 
        """
        The input parameter is a list of tuples and comes from the getColumnDetails() method. So
        this a list of columns for a single table at a time.
        For numeric data types, we are interested in the sum of the column and for non-numeric
        in the distinct count. if a column is a floating point number, the output is non-deterministic.
        This is stated in Google documentation itself.
        """ 
        col_tuple = []
        for everycol in tuplist:
            if everycol[3] in numeric_types:
                qry = 'SELECT SUM(' + everycol[2] + ') AS sum FROM ' + everycol[0] + '.' + everycol[1]
                qj = self.thisclient.query(qry)
                results = qj.result()
                for row in results:
                    return everycol[0]+ '.' + everycol[1] + '.' + everycol[2],row[0]


    def getColumnValidationInputDataNonNumeric(self,tuplist): 
        for everycol in tuplist:
            if everycol[3] not in numeric_types and complex_types:
                qry = 'SELECT COUNT(DISTINCT(' + everycol[2] + ')) as howmany FROM ' + everycol[0] + '.' + everycol[1]
                qj = self.thisclient.query(qry)
                results = qj.result()
                for row in results:
                    return everycol[0]+ '.' + everycol[1] + '.' + everycol[2],row[0]

    def _findDataType(self,datapoint,num_type_list):
        for num_type in num_type_list:
            if search(num_type,datapoint[-1]):
                return True
        return False

    def getSumOfNumericArrayOfStructMembers(self,tuplist):
        """
        For vertically adding up first member of corresponding struct members in a column that has an array of structs:
        with data as ( select Metadata as Mets from rns_sample_dataset.rns_db_2) select sum(Mets[offset(0)].geography) from data as d 
        This does the summation of the first element of the array in an array data type column for the first element only.
        """
        for everycol in tuplist:
            if everycol[3].split('<')[0] == 'ARRAY':
                if everycol[3].split('<')[1] == 'STRUCT':
                    for datapoint in everycol[3].split('<')[2].split(','):
                        if self._findDataType(datapoint.split(' '),numeric_types):
                            qry = 'with data as (select ' + everycol[2] + ' as MetData from ' + everycol[0] + '.' + everycol[1] + ') select sum(MetData[offset(0)].' + datapoint.split(' ')[-2] + ') from data as d'
                            qj = self.thisclient.query(qry)
                            results = qj.result()
                            for row in results:
                                return everycol[0] + '.' + everycol[1] + '.' + everycol[2] + '.' + datapoint.split(' ')[-2],row[0] ## We return the sum of that column with the fully qualified name of that column
        

    def getCountOfNonNumericArrayOfStructMembers(self,tuplist):
        """
        Same as previous but just gets the count distinct of the number of entries in this column for this field in this struct for this
        particular index.
        """
        for everycol in tuplist:
            if everycol[3].split('<')[0] == 'ARRAY':
                if everycol[3].split('<')[1] == 'STRUCT':
                    for datapoint in everycol[3].split('<')[2].split(','):
                        if not self._findDataType(datapoint.split(' '),numeric_types):
                            qry = 'with data as (select ' + everycol[2] + ' as MetData from ' + everycol[0] + '.' + everycol[1] + ') select count(distinct(MetData[offset(0)].' + datapoint.split(' ')[-2] + ')) from data as d'
                            qj = self.thisclient.query(qry)
                            results = qj.result()
                            for row in results:
                                return everycol[0] + '.' + everycol[1] + '.' + everycol[2] + '.' + datapoint.split(' ')[-2],row[0] ## We return the sum of that column-distinct with the fully qualified name of that column
        

    def getMaxLengthOfArrayInColumn(self,tuplist):
        """
        Get the length of max length array in a column that has arrays.
        """
        for everycol in tuplist:
            if everycol[3].split('<')[0] == 'ARRAY':
                qry = 'with data as (select ' + everycol[2] + ' as ColumnData from ' + everycol[0] + '.' + everycol[1] + ') select MAX(ARRAY_LENGTH(ColumnData)) from data as d'
                qj = self.thisclient.query(qry)
                results = qj.result()
                for row in results:
                    return everycol[0] + '.' + everycol[1] + '.' + everycol[2],row[0]

    def getSumOfNumericArrayColumn(self,tuplist): 
        for everycol in tuplist:
            if everycol[3].split('<')[0] == 'ARRAY':
                for datapoint in everycol[3].split('<'):
                    if self._findDataType([datapoint],numeric_types):
                        qry = 'with data as (select ' + everycol[2] + ' as ColData from ' + everycol[0] + '.' + everycol[1] + ') select sum(ColData[offset(0)]) from data as d'
                        qj = self.thisclient.query(qry)
                        results = qj.result()
                        for row in results:
                            return everycol[0] + '.' + everycol[1] + '.' + everycol[2],row[0]


    def getCountOfNonNumericArrayColumn(self,tuplist): 
        for everycol in tuplist:
            if everycol[3].split('<')[0] == 'ARRAY':
                for datapoint in everycol[3].split('<'):
                    if not self._findDataType([datapoint],numeric_types):
                        qry = 'with data as (select ' + everycol[2] + ' as ColData from ' + everycol[0] + '.' + everycol[1] + ') select count(distinct(ColData[offset(0)])) from data as d'
                        qj = self.thisclient.query(qry)
                        results = qj.result()
                        for row in results:
                            return everycol[0] + '.' + everycol[1] + '.' + everycol[2],row[0]
        

    def getSumOfNumericStructMembers(self,tuplist):
        for everycol in tuplist:
            if everycol[3].split('<')[0] == 'STRUCT':
                for datapoint in everycol[3].split('<')[1].split(','):
                    if self._findDataType(datapoint.split(' '),numeric_types):
                        qry = 'with data as (select ' + everycol[2] + ' as ColData from ' + everycol[0] + '.' + everycol[1] + ') select sum(ColData' + '.' + datapoint.split(' ')[-2] + ') from data as d'
                        qj = self.thisclient.query(qry)
                        results = qj.result()
                        for row in results:
                            return everycol[0] + '.' + everycol[1] + '.' + everycol[2] + '.' + datapoint.split(' ')[-2],row[0]
        

    def getCountOfNonNumericStructMembers(self,tuplist): 
        for everycol in tuplist:
            if everycol[3].split('<')[0] == 'STRUCT':
                for datapoint in everycol[3].split('<')[1].split(','):
                    if not self._findDataType(datapoint.split(' '),numeric_types):
                        qry = 'with data as (select ' + everycol[2] + ' as ColData from ' + everycol[0] + '.' + everycol[1] + ') select count(distinct(ColData' + '.' + datapoint.split(' ')[-2] + ')) from data as d'
                        qj = self.thisclient.query(qry)
                        results = qj.result()
                        for row in results:
                            return everycol[0] + '.' + everycol[1] + '.' + everycol[2] + '.' + datapoint.split(' ')[-2],row[0]

    def getNumberOfRows(self,table_id):
        """
        Note that when data is being streamed into a table instead of loaded e.g. clickstream data for example, the
        data may not be immediately available in the table, in such a case the table shows zero rows, but the
        streaming buffer section in the table details tab in the BigQuery console will display the number of records
        streamed into the table.
        When this streaming buffer section disappearss you will see the data in the table and then the table will
        display the correct number of rows, as expected.
        """
        return self.thisclient.get_table(table_id).num_rows

    def validationRouter(self,tuplist):
        """
        This function routes calls to the right kind of validation function. This would
        simplify the code on the class client side. Following are the cases for which
        this router will be used:

            getCountOfNonNumericStructMembers() - Non numeric members of structs 
            getSumOfNumericStructMembers() - Numeric members of structs
            getCountOfNonNumericArrayColumn() - Non numeric array column
            getSumOfNumericArrayColumn() - Numeric array column
            getCountOfNonNumericArrayOfStructMembers() - Non numeric members of array of structs
            getSumOfNumericArrayOfStructMembers() - Numeric members of array of structs
            getColumnValidationInputDataNonNumeric - Non numeric data column
            getColumnValidationInputDataNumeric - Numeric data column
        NOTE:
            This method expects a list of tuples, this can also be a list with a single element.
            This tuple is the kind of tuple thats returned by columnDetails() method above.
            ***This method expects a single tuple in a list as a parameter
        """
        for tup in tuplist:
            typstr = tup[3]
            ## Check for the complex array of struct with numeric member
            if search('ARRAY',typstr) != None and search('STRUCT',typstr) != None:
                for typ in numeric_types:
                    if search(typ,typstr):
                        return self.getSumOfNumericArrayOfStructMembers([tup])
            ## Check for the complex array of struct with non-numeric member
            if search('ARRAY',typstr) != None and search('STRUCT',typstr) != None:
                for typ in numeric_types:
                    if not search(typ,typstr):
                        return self.getCountOfNonNumericArrayOfStructMembers([tup])
            ## Check for the complex struct with numeric member
            if search('STRUCT',typstr) != None:
                for typ in numeric_types:
                    if search(typ,typstr):
                        return self.getSumOfNumericStructMembers([tup])
            ## Check for the complex struct with non-numeric member
            if search('STRUCT',typstr) != None:
                for typ in numeric_types:
                    if not search(typ,typstr):
                        return self.getCountOfNonNumericStructMembers([tup])
            ## Check for the numeric member of an array
            if search('ARRAY',typstr) != None:
                for typ in numeric_types:
                    if search(typ,typstr):
                        return self.getSumOfNumericArrayColumn([tup])
            ## Check for the non-numeric member of an array
            if search('ARRAY',typstr) != None:
                for typ in numeric_types:
                    if not search(typ,typstr):
                        return self.getCountOfNonNumericArrayColumn([tup])
            ## Check for the numeric data column in the table
            for typ in numeric_types:
                if search(typ,typstr):
                    return self.getColumnValidationInputDataNumeric([tup])
            ## Check for the non-numeric data column in the table
            for typ in numeric_types:
                if not search(typ,typstr):
                    return self.getColumnValidationInputDataNonNumeric([tup])

    def getValidationSample(self,percent=10): 
        """
        This method gets the final list of the tables and columns we will work on and 
        also the set of columns that will be validated/verified.
        """
        self.getAllDatasets()
        for ds in self.dsls:
            self.getAllTablesForDataset(ds.dataset_id)
            for t in self.tblls:
                self.col_list.extend(self.getColumnDetails(t.dataset_id,t.table_id))
        ## turning this off for testing ##
        #self.valsample = random.sample(self.col_list,len(self.col_list)//percent)
        valcols = [i for i in self.col_list if not (i[3].startswith('STRUCT') and i[3].startswith('ARRAY'))]
        self.valsample = random.sample(valcols,len(self.col_list)//percent)

    def getFullTableListForThisProject(self):
        self.getAllDatasets()
        for ds in self.dsls:
            self.getAllTablesForDataset(ds.dataset_id)
            for t in self.tblls:
                self.table_list.append(t)
                
    def showValidationSample(self):
        return self.valsample

    def showFullColumnList(self):
        return self.col_list

    def getTableStats(self): 
        """
        This is the big one, this gets all that Ninad needs. Creates a consolidated file
        with all the required information.
        """
        arrlength = 0
        self.getFullTableListForThisProject()
        self.getValidationSample()
        print("Writing file: BQStats.csv")
        with open("BQStats.csv","w") as bqstatfile:
            writer = csv.writer(bqstatfile,delimiter='|')
            writer.writerow(["DATASET","TABLE","COLUMN","TYPE","DUMMY"])
            for col in self.col_list:
                writer.writerow([col[0],col[1],col[2],col[3],0])
        print("Finished writing file: BQStats.csv")
        print("Writing file: BQTableRows.csv")
        with open("BQTableRows.csv","w") as bqrowsfile:
            writer = csv.writer(bqrowsfile,delimiter='|')
            writer.writerow(["DATASET","TABLE","ROWS"])
            for t in self.table_list:
                rows = self.getNumberOfRows(t.dataset_id + "." + t.table_id)
                writer.writerow([t.dataset_id,t.table_id,rows])
        print("Finished writing file: BQTableRows.csv")
        print("Writing file: BQSideValidationData.csv")
        with open("BQSideValidationData.csv","w") as bqvalfile:
            writer = csv.writer(bqvalfile,delimiter='|')
            writer.writerow(["DATASET","TABLE","COLUMN","TYPE","VALIDATIONDATA","ARRAYMAXLENGTH"])
            for valcol in self.valsample:
                #if search("ARRAY",valcol[3]) != None:
                #    continue ## We skip if we find an array as we can't validate on AWS side, this would have to be manual
                #if search("STRUCT",valcol[3]) != None:
                #    continue ## We skip if we find an array as we can't validate on AWS side, this would have to be manual
                coldetails,validationdata = self.validationRouter([valcol])
                if search("ARRAY",valcol[3]) != None: ## If this is an array type...
                    tab,arrlength = self.getMaxLengthOfArrayInColumn([valcol])
                coldet = coldetails.split('.')
                writer.writerow([coldet[0],coldet[1],coldet[2],valcol[3],validationdata,arrlength])
        print("Finished writing file: BQSideValidationData.csv")

    def copyMetadataFilesToS3(self,s3bucket):
        """
        This function expects the stat files to be in the PWD.
        Returns true when all the files are uploaded.
        """
        filenames = ["BQSideValidationData.csv","BQTableRows.csv","BQStats.csv"]
        s3client = boto3.client("s3")
        for name in filenames:
            try:
                print("Uploading {} to {}...".format(name,"s3://"+s3bucket+"/metadata/"+name.split('.')[0]+"/"+name))
                response = s3client.upload_file(name, s3bucket, "metadata/"+name.split('.')[0]+"/"+name)
                os.remove(name)
            except ClientError as e:
                print(e)
                return False
        return True
            

    def exportTable(self,table,dataset,destination,location):
        jc = bigquery.job.ExtractJobConfig()
        jc.destination_format = "AVRO"
        extract_job = self.thisclient.extract_table(
            self.getTable(dataset,table).reference,
            destination,
            job_config = jc,
            location=location
        )
        extract_job.result()
        print("Exported {}.{} to {}".format(dataset,table,destination))

if __name__ == "__main__":
    b = BQDetailsObject()
    b.getAllDatasets()
    for ds in b.dsls:
        b.getAllTablesForDataset(ds.dataset_id)
    for t in b.tblls:
        #print(b.getTable(t.dataset_id,t.table_id).table_id)
        if b.isPartitioned(t.dataset_id,t.table_id):
            #print("Partition for {}".format(t.table_id))
            for res in b.getPartitions(t.table_id,t.dataset_id):
                if res[0] == None:
                    continue
                #print(res[0]) 
        print("Exporting {} table to {}".format(t.table_id,'gs://rns_sample_data_output_bucket-1/' + t.dataset_id + '/' + t.table_id))
        b.exportTable(t.table_id,t.dataset_id,'gs://rns_sample_data_output_bucket-1/' + t.dataset_id + '/' + t.table_id,'US') # Hardcoding location for now
        
