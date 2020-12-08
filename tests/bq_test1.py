#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDetailsObject')

from BQDetailsObject import BQDetailsObject

b = BQDetailsObject()
print("Testing getAllDatasets()...")
b.getAllDatasets()
count = 0
if b.dsls != None:
    for ds in b.dsls:
        count = count + 1
if count > 0:
    print("Test for getAllDatasets() succeeded.")
