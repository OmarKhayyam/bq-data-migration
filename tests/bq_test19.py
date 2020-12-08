#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDetailsObject')

from BQDetailsObject import BQDetailsObject

b = BQDetailsObject()
print("Testing validationRouter()...")
b.getAllDatasets()
for ds in b.dsls:
    b.getAllTablesForDataset(ds.dataset_id)
    for t in b.tblls:
        dtset = t.dataset_id
        tab = t.table_id
        mytablst = b.getColumnDetails(dtset,tab)        
        for col in mytablst:
            print("Routing tuple {}".format(col))
            print(b.validationRouter([col]))
    break
print("Test for validationRouter() succeeded.")
