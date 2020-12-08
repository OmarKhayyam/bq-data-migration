#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDetailsObject')

from BQDetailsObject import BQDetailsObject

b = BQDetailsObject()
print("Testing getNumberOfRows()...")
b.getAllDatasets()
for ds in b.dsls:
    b.getAllTablesForDataset(ds.dataset_id)
    for t in b.tblls:
        tab = t.table_id
        print("Table Name: {}.{} - Number of Rows: {}".format(t.dataset_id,tab,b.getNumberOfRows(t.dataset_id + '.' + tab)))
print("Test for getNumberOfRows() succeeded.")
