#!/usr/bin/env python

from google.cloud import bigquery

client = bigquery.Client()
qj = client.query("""
                    select * from innate-entry-286804.rns_sample_dataset.rns_db_1 limit 1
                """)
results = qj.result()
for res in results:
    print(res['V22'])
