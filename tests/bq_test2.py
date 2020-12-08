#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDetailsObject')

from BQDetailsObject import BQDetailsObject

b = BQDetailsObject()
print("Testing getAllTablesForDataset()...")
b.getAllDatasets()
count = 0
if b.dsls != None:
    for ds in b.dsls:
        b.getAllTablesForDataset(ds.dataset_id)
        if b.tblls != None:
            for tb in b.tblls:
                count = count + 1
            if count > 0:
                print("Test for getAllTablesForDataset() succeeded.")
                break
