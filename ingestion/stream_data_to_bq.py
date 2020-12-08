#!/usr/bin/env python

## Create a schema for the table and then create the table.

from google.cloud import bigquery
import random
import string
import pycountry
import time


def get_locality():
    high = len(list(pycountry.subdivisions))
    low = 0
    idx = random.randint(low,high)
    return (list(pycountry.subdivisions)[idx]).country_code,(list(pycountry.subdivisions)[idx]).name

id_size = 10 ## Change ID size if bigger is required
sleep_time = 2 ## Change time to wait as required

client = bigquery.Client()

table_id = "innate-entry-286804.rns_sample_dataset.rns_db_4"
table = client.get_table(table_id)

while(1):
    itemid = ''.join(random.choices(string.ascii_uppercase + string.digits, k = id_size))
    userid = ''.join(random.choices(string.ascii_uppercase + string.digits, k = id_size))
    quantity = 2 ## Hardcoding this. for this experiment
    geography,location = get_locality()    
    hourofday = random.randint(0,24)

    row = [(itemid,quantity,userid,[(geography,location,hourofday)])]
    errors = client.insert_rows(table,row) ## Making an API Request
    if errors == []:
        print("A new row has been added.-Details- {} and {}".format(geography,location))
    else:
        print("A new row could not be added.-Details- {} and {}".format(geography,location))
    time.sleep(sleep_time)
