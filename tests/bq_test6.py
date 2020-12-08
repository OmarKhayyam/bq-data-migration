#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDetailsObject')

from BQDetailsObject import BQDetailsObject

b = BQDetailsObject()
print("Testing TIME and RANGE partitions...")
ispart = False
b.getAllDatasets()
for ds in b.dsls:
    b.getAllTablesForDataset(ds.dataset_id)
    for t in b.tblls:
        ispart = b.isPartitioned(t.dataset_id,t.table_id)
        if ispart:
            if b.partitionType(t.dataset_id,t.table_id) == 'RANGE':
                p = b.getRangePartitions(t.table_id,t.dataset_id)
                s = p.start
                e = p.end
                print(list(range(s,e)))
            else:
                print(b.getTimePartitions(t.table_id,t.dataset_id))
print("Test for TIME and RANGE partitions succeeded.")
