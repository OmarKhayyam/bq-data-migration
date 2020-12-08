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
years = ['2010','2011','2012','2013','2014','2015','2016','2017','2018','2019']
months = ['01','02','03','04','05','06','07','08','09','10','11','12']
days = ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28']

client = bigquery.Client()

table_id = "innate-entry-286804.rns_sample_dataset.rns_db_5"
table = client.get_table(table_id)

while(1):
    itemid = ''.join(random.choices(string.ascii_uppercase + string.digits, k = id_size))
    userid = ''.join(random.choices(string.ascii_uppercase + string.digits, k = id_size))
    quantity = 2 ## Hardcoding this. for this experiment
    geography,location = get_locality()    
    hourofday = random.randint(0,24)
    for day in days:
        for month in months:
            for year in years:
                dt = year + "-" + month + "-" + day
    row = [(itemid,quantity,userid,dt,[(geography,location,hourofday)])]
    errors = client.insert_rows(table,row) ## Making an API Request
    if errors == []:
        print("A new row has been added.-Details- {} and {}".format(geography,location))
    else:
        print("A new row could not be added.-Details- {} and {}".format(geography,location))
    time.sleep(sleep_time)
