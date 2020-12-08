#!/usr/bin/env python

###################################################
#This is a data extractor built specifically      #
#for Google BigQuery. It does the following       #
# 1. It extracts the raw data, as-is from every   #
#    BQ table and stores it in Avro format in GCS.#
# 2. It then gets the details of every BQ table   #
#    and mimics the structure in GCS.             #
# 3. Uses the raw data and partitions it and      #
#    and stores the data in the appropriate       # 
#    partition.                                   #
# Key Feature: Checkpointing                      #
###################################################

import sys
sys.path.insert(1, '../BQDetailsObject')

import json
import gzip
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from BQDetailsObject import BQDetailsObject

class BQDataExtractor:
    def __init__(self):
        self.thisclient = bigquery.Client()
        self.tablesrecord = list()

    def setGCSClient(self):
        self.thisstorageclient = storage.Client()

    def recordTableDetails(self,dataset_id,table_id):
        """
        This records the details of the table we just extracted.
        We create an object, that we then serialize in JSON format.
        We use this file later when we want to mimic the BQ internal
        structure within BQ onto GCS.
        """
        print("Recording on table {}".format(table_id))
        record = dict()
        b = BQDetailsObject()
        record["dataset_id"] = dataset_id
        record["table_id"] = table_id
        record["is_partitioned"] = b.isPartitioned(dataset_id,table_id)
        if record["is_partitioned"]:
            record["partition_type"] = b.partitionType(dataset_id,table_id)
        else:
            record["partition_type"] = "NA"
        if record["partition_type"] == 'TIME':
            typ_,field = b.detailsOfTimePartition(dataset_id,table_id)
            record["time_partition_type"] = typ_
            record["time_partition_field"] = field
        else:
            record["time_partition_type"] = None
            record["time_partition_field"] = None
        if record["partition_type"] == 'TIME':
            if record["time_partition_type"] == "DAY":
                record["partitions"] = b.getTimePartitions(table_id,dataset_id)
            else: ## If this is HOURLY
                record["partitions"] = b.getTimePartitionsHourly(table_id,dataset_id)
        if record["partition_type"] == 'RANGE':
            s = b.getRangePartitions(table_id,dataset_id).start
            e = b.getRangePartitions(table_id,dataset_id).end
            record['partitions'] = list(range(s,e))
            field = (self.thisclient.get_table(dataset_id + "." + table_id)).range_partitioning.field
            record["range_partition_field"] = field
        else:
            record["range_partition_field"] = None
        if record["is_partitioned"] == False:
            record["partitions"] = ['']
        self.tablesrecord.append(record)

    def extractRawData(self,gcslocation,regionlocation='US'):
        """
        gcslocation expects a valid GCS location
        in the same region e.g. gs://<some_name>/<prefix>/
        This exports all the data from a project i.e.
        for all the datasets within this project and all
        the tables within each dataset.
        """
        b = BQDetailsObject()
        b.getAllDatasets()
        if b.dsls != None:
            for ds in b.dsls:
                b.getAllTablesForDataset(ds.dataset_id)
                if b.tblls != None:
                    for tab in b.tblls:
                        job_config = bigquery.job.ExtractJobConfig()
                        job_config.compression = bigquery.Compression.SNAPPY
                        job_config.destination_format = 'AVRO'
                        job_config.print_header = True
                        job_config.use_avro_logical_types = True ## Hope this is OK, check with Ninad
                        extract_job = self.thisclient.extract_table(
                            tab.reference,gcslocation + "raw/" + tab.dataset_id + "/" + tab.table_id + "/" + tab.table_id + "-*",job_id_prefix="BQ-Migration-",location=regionlocation,job_config=job_config,source_type='Table'
                        ) 
                        extract_job.result() ## Wait for the job to complete
                        print("Table {} now extracted.".format(tab.table_id))
                        self.recordTableDetails(tab.dataset_id,tab.table_id)
        json_object = json.dumps(self.tablesrecord)
        with open("BQTableDetails.json", "w") as outfile:
            outfile.write(json_object)
        return True        


    def extractRawDataV2(self,gcslocation,regionlocation='US'): 
        """
        gcslocation expects a valid GCS location
        in the same region e.g. gs://<some_name>/<prefix>/
        This exports all the data from a project i.e.
        for all the datasets within this project and all
        the tables within each dataset.
        """
        b = BQDetailsObject()
        b.getAllDatasets()
        if b.dsls != None:
            for ds in b.dsls:
                b.getAllTablesForDataset(ds.dataset_id)
                if b.tblls != None:
                    for tab in b.tblls:
                        print("Processing table {}...".format(tab.table_id))
                        if b.isIngestPartitioned(tab.dataset_id,tab.table_id) == False:
                            job_config = bigquery.job.ExtractJobConfig()
                            job_config.compression = bigquery.Compression.SNAPPY
                            job_config.destination_format = 'AVRO'
                            job_config.print_header = True
                            job_config.use_avro_logical_types = True ## Hope this is OK, check with Ninad
                            extract_job = self.thisclient.extract_table(
                                tab.reference,gcslocation + "raw/" + tab.dataset_id + "/" + tab.table_id + "/" + tab.table_id + "-*",job_id_prefix="BQ-Migration-",location=regionlocation,job_config=job_config,source_type='Table'
                            ) 
                            extract_job.result() ## Wait for the job to complete
                            print("Table {} now extracted.".format(tab.table_id))
                        else:
                            for part in b.getTimePartitions(tab.table_id,tab.dataset_id):
                                job_config = bigquery.job.ExtractJobConfig()
                                job_config.compression = bigquery.Compression.SNAPPY
                                job_config.destination_format = 'AVRO'
                                job_config.print_header = True
                                job_config.use_avro_logical_types = True ## Hope this is OK, check with Ninad
                                dsref = bigquery.DatasetReference(ds.project,ds.dataset_id)
                                tabref = dsref.table(tab.table_id + "$" +str(part))
                                extract_job = self.thisclient.extract_table(
                                    tabref, gcslocation + "raw/" + tab.dataset_id + "/" + tab.table_id + "/" + str(part[0:4]) + "/" + str(part[4:6]) + "/" + str(part[6:]) + "/" + tab.table_id + "-*",job_id_prefix="BQ-Migration-",location=regionlocation,job_config=job_config,source_type='Table'
                                )
                                extract_job.result() ## Wait for the job to complete
                                print("Table {} and partition {} now extracted.".format(tab.table_id,part))
                        self.recordTableDetails(tab.dataset_id,tab.table_id) ## Bringing this out of the if
        json_object = json.dumps(self.tablesrecord)
        with open("BQTableDetails.json", "w") as outfile:
            outfile.write(json_object)
        return True        
        
    def extractRawDataFromHourlyPartitionedTable(self,gcslocation,dataset_id,table_id,regionlocation='US'): 
        """
        This method is called only for tables that are HOUR partitioned at ingest time __not__ on column.
        This uses the _PARTITIONTIME pseudo column.
        This method writes the raw data itself to GZIP compressed JSON, hence when we move this to the staging area we need
        keep in mind that it is because PyArrow does not support serializing certain repeatable fields.
        We will have to query the table to get to this data, which means it will cost. This is the price of a tech lock in
        """
        b = BQDetailsObject()
        client = bigquery.Client()
        partlist = b.getTimePartitionsHourly(table_id,dataset_id)
        count = 0
        storage_client = storage.Client()
        bucket_name = gcslocation.split('/')[2]
        while count < len(partlist):
            qry = "select * from " + dataset_id + "." + table_id + " where _PARTITIONTIME >= \"" + str(partlist[count][0]) + "-" + str(partlist[count][1]) + "-" + str(partlist[count][2]) + " " + str(partlist[count][3]).rjust(2,'0') + ":00:00" + "\""
            if count + 1 < len(partlist):
                qry = qry + " and _PARTITIONTIME < \"" + str(partlist[count + 1][0]) + "-" + str(partlist[count + 1][1]) + "-" + str(partlist[count + 1][2]) + " " + str(partlist[count + 1][3]).rjust(2,'0') + ":00:00" + "\""
            qryjob = client.query(qry)
            partdf = qryjob.to_dataframe() 
            partdf.to_json("temp.json",orient="records",lines=True)
            fp = open("temp.json","rb")
            tabledata = fp.read()
            bindata = bytearray(tabledata)
            with gzip.open("temp.gz", "wb") as f:
                f.write(bindata)
            bucket = storage_client.bucket(bucket_name)
            destination_blob_name = '/'.join(gcslocation.split('/')[3:]) + "raw/" + dataset_id + "/" + table_id + "/year=" + str(partlist[count][0]) + "/month=" + str(partlist[count][1]).rjust(2,'0') + "/day=" + str(partlist[count][2]).rjust(2,'0') + "/hour=" + str(partlist[count][3]).rjust(2,'0')  + "/" + table_id + "-" + str(count).rjust(11,'0') + ".gz"
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename("temp.gz")
            print("Table {} and partition {} now extracted as GZIP compressed JSON.".format(table_id,partlist[count]))
            count = count + 1

    def extractRawDataFromRangePartitionedTable(self,gcslocation,dataset_id,table_id,regionlocation='US'): ### WIP ###
        b = BQDetailsObject()
        client = bigquery.Client()
        storage_client = storage.Client()
        rg = b.getRangePartitions(table_id,dataset_id)
        partlist = list(range(rg.start,rg.end))
        field = (self.thisclient.get_table(dataset_id + "." + table_id)).range_partitioning.field
        bucket_name = gcslocation.split('/')[2]
        count = 0
        for part in partlist:
            qry = "select * from " + dataset_id + "." + table_id + " where " + field + " = " + str(part)
            qryjob = client.query(qry)
            partdf = qryjob.to_dataframe()
            ## Testing - Removing field column for Range partitioned table
            ## existence of partition column in data throws Glue off
            ## start test code ***###***
            partdf = partdf.drop([field],axis=1)
            ## end test code ***###***
            ## Trying to get JSONLINES o/p per Ninad's request
            partdf.to_json("temp.json",orient="records",lines=True)
            fp = open("temp.json","rb")
            tabledata = fp.read()
            bindata = bytearray(tabledata)
            with gzip.open("temp.gz", "wb") as f:
                f.write(bindata)
            bucket = storage_client.bucket(bucket_name)
            destination_blob_name = '/'.join(gcslocation.split('/')[3:]) + "raw/" + dataset_id + "/" + table_id + "/" + field + "=" + str(part).rjust(2,'0') + "/" + table_id + "-" + str(count).rjust(11,'0') + ".gz"
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename("temp.gz")
            print("Table {} and partition {} now extracted as GZIP compressed JSON.".format(table_id,part))
            count = count + 1


    def extractRawDataV3(self,gcslocation,regionlocation='US'): 
        """
        gcslocation expects a valid GCS location
        in the same region e.g. gs://<some_name>/<prefix>/
        This exports all the data from a project i.e.
        for all the datasets within this project and all
        the tables within each dataset.
        metadataloc : This is a new param that provides metadata location for Hourly partitioned tables using hidden columns
        """
        b = BQDetailsObject()
        b.getAllDatasets()
        if b.dsls != None:
            for ds in b.dsls:
                b.getAllTablesForDataset(ds.dataset_id)
                if b.tblls != None:
                    for tab in b.tblls:
                        print("Processing table {}...".format(tab.table_id))
                        if b.isIngestPartitioned(tab.dataset_id,tab.table_id) == False and b.isPartitioned(tab.dataset_id,tab.table_id) == False:
                            job_config = bigquery.job.ExtractJobConfig()
                            job_config.compression = bigquery.Compression.SNAPPY
                            job_config.destination_format = 'AVRO'
                            job_config.print_header = True
                            job_config.use_avro_logical_types = True ## Hope this is OK, check with Ninad
                            extract_job = self.thisclient.extract_table(
                                tab.reference,gcslocation + "raw/" + tab.dataset_id + "/" + tab.table_id + "/" + tab.table_id + "-*",job_id_prefix="BQ-Migration-",location=regionlocation,job_config=job_config,source_type='Table'
                            ) 
                            extract_job.result() ## Wait for the job to complete
                            print("Table {} now extracted.".format(tab.table_id))
                        elif b.isIngestPartitioned(tab.dataset_id,tab.table_id) == False and b.isPartitioned(tab.dataset_id,tab.table_id) == True:
                            self.extractRawDataFromRangePartitionedTable(gcslocation,tab.dataset_id,tab.table_id)
                        else:
                            ## Have to patch this for considering HOURLY ingestion time partitioning
                            ## BigQuery doesn't allow exporting HOURLY partitioned tables using a decorator.
                            ## We have no option in such cases but to query the table and this can cost you.
                            ## This is a risk that we have to inform the end user about.
                            if b.isIngestPartitionedHourly(tab.dataset_id,tab.table_id) == True:
                                self.extractRawDataFromHourlyPartitionedTable(gcslocation,tab.dataset_id,tab.table_id)
                            else:
                                for part in b.getTimePartitions(tab.table_id,tab.dataset_id):
                                    job_config = bigquery.job.ExtractJobConfig()
                                    job_config.compression = bigquery.Compression.SNAPPY
                                    job_config.destination_format = 'AVRO'
                                    job_config.print_header = True
                                    job_config.use_avro_logical_types = True ## Hope this is OK, check with Ninad
                                    dsref = bigquery.DatasetReference(ds.project,ds.dataset_id)
                                    tabref = dsref.table(tab.table_id + "$" +str(part))
                                    extract_job = self.thisclient.extract_table(
                                        tabref, gcslocation + "raw/" + tab.dataset_id + "/" + tab.table_id + "/year=" + str(part[0:4]) + "/month=" + str(part[4:6]) + "/day=" + str(part[6:]) + "/" + tab.table_id + "-*",job_id_prefix="BQ-Migration-",location=regionlocation,job_config=job_config,source_type='Table'
                                    )
                                    extract_job.result() ## Wait for the job to complete
                                    print("Table {} and partition {} now extracted.".format(tab.table_id,part))
                        self.recordTableDetails(tab.dataset_id,tab.table_id) ## Bringing this out of the if
        json_object = json.dumps(self.tablesrecord)
        with open("BQTableDetails.json", "w") as outfile:
            outfile.write(json_object)
        return True        

    def mimicBQStructureInGCS(self,gcsbucket,prefix):
        """
        Expects the BQTableDetails.json to exist in the PWD
        The GCS bucket has to be similar to that you gave to
        extractRawData() method, can be a different bucket though.
        The prefix is a different location within the destination.
        Prefix should end with a '/' and not start with '/'
        """
        with open('BQTableDetails.json', 'r') as f:
            tabledet = json.load(f)
        #print(tabledet)
        ## Create a local file
        open("__SOURCEFILE","w")    
        storage_client = storage.Client()
        bucket = storage_client.bucket(gcsbucket)
        blob = None
        for everytable in tabledet:
            destination_uri = prefix + everytable["dataset_id"] + "/" + everytable["table_id"] + "/"
            if everytable["is_partitioned"] == False:
                destination_uri = destination_uri + "__SOURCEFILE"
                blob = bucket.blob(destination_uri)      
                blob.upload_from_filename("__SOURCEFILE")
                print("Unpartitioned File {} uploaded to {}.".format("__SOURCEFILE",destination_uri))
            else: # When table is partitioned
                if everytable["partition_type"] == "TIME":
                    if everytable["time_partition_type"] == "DAY":
                        for t in everytable["partitions"]:
                            print("Processing partition {}.".format(t))
                            destination_uri = destination_uri + "year=" + t[:4] + "/month=" + t[4:6] + "/day=" + t[6:] + "/" + "__SOURCEFILE"
                            blob = bucket.blob(destination_uri)
                            blob.upload_from_filename("__SOURCEFILE")
                            print("TIME-DAY Partitioned File {} uploaded to {}.".format("__SOURCEFILE",destination_uri))
                            ## Re-initializing destination_uri
                            destination_uri = prefix + everytable["dataset_id"] + "/" + everytable["table_id"] + "/"
                    else: ## reserved for HOURLY partitioning
                        for t in everytable["partitions"]:
                            print("Processing partition {}.".format(t))
                            destination_uri = destination_uri + "year=" + str(t[0]) + "/month=" + str(t[1]).rjust(2,'0') + "/day=" + str(t[2]).rjust(2,'0') + "/hour=" + str(t[3]).rjust(2,'0') + "/" + "__SOURCEFILE"
                            blob = bucket.blob(destination_uri)
                            blob.upload_from_filename("__SOURCEFILE")
                            print("TIME-DAY-HOUR Partitioned File {} uploaded to {}.".format("__SOURCEFILE",destination_uri))
                            ## Re-initializing destination_uri
                            destination_uri = prefix + everytable["dataset_id"] + "/" + everytable["table_id"] + "/"
                else: ## Range partitioning
                    for t in everytable["partitions"]:
                        print("Processing partition {}.".format(t))
                        destination_uri = destination_uri + everytable["range_partition_field"] + "=" + str(t).rjust(2,'0') + "/" + "__SOURCEFILE"
                        blob = bucket.blob(destination_uri)
                        blob.upload_from_filename("__SOURCEFILE")
                        print("RANGE Partitioned File {} uploaded to {}.".format("__SOURCEFILE",destination_uri))
                        ## Re-initializing destination_uri
                        destination_uri = prefix + everytable["dataset_id"] + "/" + everytable["table_id"] + "/"
        return True

    def extractDataPartitionWise(self,gcsbucket,prefix,rawlocation):
        """
        parameters:
            gcsbucket: same as mimicBQStructureIntoGCS
            prefix: same as mimicBQStructureIntoGCS
            rawlocation: same as extractRawData 

        This uses the BQTableDetails.json file to process every file:
        There are essentially 4 types of partitioned tables:
        1. Unpartitioned tables
        2. Tables partitioned by date
        3. Tables partitioned by hour
        4. Tables partitioned by integers

        1. This function needs to identify tables
        2. This function needs to identify the corresponding partitions
        3. This function needs to create a Dataframe of all the data needed to extract data from every partition in every table.

        Finally, this function needs to checkpoint its progress and use the checkpoint to restart a failed extractions
        
        Must have comprehensive error logging to help with debugging.
        """
        this.setGCSClient()
        with open('BQTableDetails.json', 'r') as f:
            tabledet = json.load(f)
        for tab in tabledet:
            destination_uri = prefix + tab["dataset_id"] + "/" + tab["table_id"] + "/"
            source_uri = rawlocation + "raw/" + tab["dataset_id"] + "/" + tab["table_id"] + "/"
            if tab["is_partitioned"] == False:
                print("Table Name: {} is not partitioned.".format(tab["table_id"]))
                source_bucket = self.thisstorageclient.bucket(gcsbucket)
                source_blob = source_bucket.blob()
                destination_bucket = self.thisstorageclient.bucket(gcsbucket)
                blob_copy = source_bucket.copy_blob(source_blob,destination_bucket,destination_blob_name)
            else:
                print("Table Name: {} is partitioned".format(tab["table_id"]))

        return True
