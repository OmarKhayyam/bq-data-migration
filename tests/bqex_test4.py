#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDataExtractor')

from BQDataExtractor import BQDataExtractor

bex = BQDataExtractor()
print("Testing extractRawDataV2()...")
result = bex.extractRawDataV2("gs://rns_sample_data_bucket_1/exported_tablesV2/")
if result == True:
    print("Test for extractRawDataV2() succeeded.")
else:
    print("Test for extractRawDataV2() failed.")
