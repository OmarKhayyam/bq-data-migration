#!/usr/bin/env python

## Create a schema for the table and then create the table.

from google.cloud import bigquery

client = bigquery.Client()

table_id = "innate-entry-286804.rns_sample_dataset.rns_db_5"

schema=[
    bigquery.SchemaField('itemid','STRING',mode='REQUIRED'),
    bigquery.SchemaField('quantity','STRING',mode='REQUIRED'),
    bigquery.SchemaField('userid','STRING',mode='REQUIRED'),
    bigquery.SchemaField('date','DATE',mode='REQUIRED'),
    bigquery.SchemaField('Metadata','RECORD',mode='REPEATED',fields=[
                                                                bigquery.SchemaField('geography','STRING',mode='REQUIRED'),
                                                                bigquery.SchemaField('location','STRING',mode='REQUIRED'),
                                                                bigquery.SchemaField('hourofday','INT64',mode='REQUIRED')
                                                            ]
    )
]

table = bigquery.Table(table_id,schema=schema)
## Creating an ingestion-time partitioned table
table.time_partitioning = bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.DAY,
                            field="date",  # name of column to use for partitioning
                        ) ## Defaults to DAY
table = client.create_table(table)
print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
