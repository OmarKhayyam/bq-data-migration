#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDataExtractor')

from BQDataExtractor import BQDataExtractor

bex = BQDataExtractor()
print("Testing mimicBQStructureInGCS()...")
result = bex.mimicBQStructureInGCS("rns_sample_data_bucket_1","BQSTRUCT/")
if result == True:
    print("Test for mimicBQStructureInGCS() succeeded.")
else:
    print("Test for mimicBQStructureInGCS() failed.")
