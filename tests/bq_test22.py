#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDetailsObject')

from BQDetailsObject import BQDetailsObject

b = BQDetailsObject()
print("Testing copyMetadataFilesToS3()...")
b.getTableStats()
result = b.copyMetadataFilesToS3('rns-lk')
if result == True:
    print("Test for copyMetadataFilesToS3() succeeded.")
else:
    print("Test for copyMetadataFilesToS3() failed.")
