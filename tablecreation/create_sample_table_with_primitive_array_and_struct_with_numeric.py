#!/usr/bin/env python

## Create a schema for the table and then create the table.

from google.cloud import bigquery

client = bigquery.Client()

table_id = "innate-entry-286804.rns_sample_dataset.rns_db_6"

schema=[
    bigquery.SchemaField('Students','STRING',mode='REPEATED'),
    bigquery.SchemaField('Grades','INT64',mode='REPEATED'),
    bigquery.SchemaField('StudentDetails','RECORD',mode='REQUIRED',fields=[
                                                                    bigquery.SchemaField('DaysInSchool','INT64',mode='REQUIRED'),
                                                                    bigquery.SchemaField('NumOfStudentsWithEquivalentGrade','INT64',mode='REQUIRED')
                                                                ]
    )
]

table = bigquery.Table(table_id,schema=schema)
table = client.create_table(table)
print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
