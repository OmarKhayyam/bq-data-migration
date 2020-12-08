#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDataExtractor')

from BQDataExtractor import BQDataExtractor

bex = BQDataExtractor()
print("Testing extractRawData()...")
result = bex.extractRawData("gs://rns_sample_data_bucket_1/exported_tables/")
if result == True:
    print("Test for extractRawData() succeeded.")
else:
    print("Test for extractRawData() failed.")
