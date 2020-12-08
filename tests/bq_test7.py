#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDetailsObject')

from BQDetailsObject import BQDetailsObject

b = BQDetailsObject()
print("Testing getTimePartitionsHourly()...")
ispart = False
prtlst = list()
b.getAllDatasets()
for ds in b.dsls:
    b.getAllTablesForDataset(ds.dataset_id)
    for t in b.tblls:
        ispart = b.isPartitioned(t.dataset_id,t.table_id)
        if ispart:
            if b.partitionType(t.dataset_id,t.table_id) == 'RANGE':
                #print(b.getRangePartitions(t.table_id,t.dataset_id))
                continue
            else:
                typ,fild = b.detailsOfTimePartition(t.dataset_id,t.table_id)
                if typ == "HOUR":
                    print("Identified Table: {}".format(t.table_id))
                    prtlst = b.getTimePartitionsHourly(t.table_id,t.dataset_id)
print(prtlst)
print("Test for getTimePartitionsHourly() succeeded.")
