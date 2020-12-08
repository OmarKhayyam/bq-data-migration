#!/usr/bin/env python

## Create a schema for the table and then create the table.

from google.cloud import bigquery
import random
import string
import time


sleep_time = 2 ## Change time to wait as required

student_names = ['Jerry','George','Elaine','Kramer','Newman','Banya','Soup Nazi']
student_grades = [20,30,40,50,60,70,80]
student_dis = [300,200,300,100,50,150,100]
equivalent_grades = [20,50,20,40,60,10,5]

client = bigquery.Client()

table_id = "innate-entry-286804.rns_sample_dataset.rns_db_6"
table = client.get_table(table_id)

count = 1

while(1):
    row = [([(student_names[count - 1]),(student_names[count])],[(student_grades[count - 1]),(student_grades[count])],(student_dis[count - 1],equivalent_grades[count - 1]))]
    errors = client.insert_rows(table,row) ## Making an API Request
    if errors == []:
        print("A new row has been added.")
    else:
        print("A new row could not be added.")
    time.sleep(sleep_time)
    if count == 6:
        count = 1
    else:
        count = count + 1
