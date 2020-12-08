#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDetailsObject')

from BQDetailsObject import BQDetailsObject

b = BQDetailsObject()
print("Testing getColumnValidationInputDataNumeric()...")
b.getAllDatasets()
for ds in b.dsls:
    b.getAllTablesForDataset(ds.dataset_id)
    for t in b.tblls:
        dtset = t.dataset_id
        tab = t.table_id
        mytablst = b.getColumnDetails(dtset,tab)        
        print(b.getColumnValidationInputDataNumeric(mytablst))
        break
    break
print("Test for getColumnValidationInputDataNumeric() succeeded.")
