#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDetailsObject')

from BQDetailsObject import BQDetailsObject

b = BQDetailsObject()
print("Testing isPartitioned()...")
ispart = False
b.getAllDatasets()
for ds in b.dsls:
    b.getAllTablesForDataset(ds.dataset_id)
    for t in b.tblls:
        print("Testing for table {}...".format(t.table_id))
        ispart = b.isPartitioned(t.dataset_id,t.table_id)
        if ispart:
            print("Table is partitioned.")
        else:
            print("Table is not partitioned.")
print("Test for isPartioned() succeeded.")
