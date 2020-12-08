#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDetailsObject')

from BQDetailsObject import BQDetailsObject

b = BQDetailsObject()
print("Testing partitionType()...")
ispart = False
b.getAllDatasets()
for ds in b.dsls:
    b.getAllTablesForDataset(ds.dataset_id)
    for t in b.tblls:
        ispart = b.isPartitioned(t.dataset_id,t.table_id)
        if ispart:
            print("Testing for table {}...- {}".format(t.table_id,b.partitionType(t.dataset_id,t.table_id)))
print("Test for partitionType() succeeded.")
