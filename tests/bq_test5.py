#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDetailsObject')

from BQDetailsObject import BQDetailsObject

b = BQDetailsObject()
print("Testing detailsOfTimePartition()...")
ispart = False
b.getAllDatasets()
for ds in b.dsls:
    b.getAllTablesForDataset(ds.dataset_id)
    for t in b.tblls:
        ispart = b.isPartitioned(t.dataset_id,t.table_id)
        if ispart:
            if b.partitionType(t.dataset_id,t.table_id) == 'TIME':
                print("Testing for table {}.".format(t.table_id))
                typ,fild = b.detailsOfTimePartition(t.dataset_id,t.table_id)
                print("\t\tType: {} and Field: {}".format(typ,fild))
print("Test for detailsOfTimePartition() succeeded.")
