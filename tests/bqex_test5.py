#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDataExtractor')

from BQDataExtractor import BQDataExtractor

bex = BQDataExtractor()
print("Testing extractRawDataV3()...")
result = bex.extractRawDataV3("gs://rns_sample_data_bucket_1/exported_tablesV3/")
if result == True:
    print("Test for extractRawDataV3() succeeded.")
else:
    print("Test for extractRawDataV3() failed.")
