#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDataExtractor')

from BQDataExtractor import BQDataExtractor

bex = BQDataExtractor()
print("Testing extractDataPartitionWise()...")
result = bex.extractDataPartitionWise()
if result == True:
    print("Test for extractDataPartitionWise() succeeded.")
else:
    print("Test for extractDataPartitionWise() failed.")
