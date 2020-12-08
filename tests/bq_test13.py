#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDetailsObject')

from BQDetailsObject import BQDetailsObject

b = BQDetailsObject()
print("Testing getCountOfNonNumericStructMembers()...")
b.getAllDatasets()
for ds in b.dsls:
    b.getAllTablesForDataset(ds.dataset_id)
    for t in b.tblls:
        dtset = t.dataset_id
        tab = t.table_id
        if tab == 'rns_db_3':
            mytablst = b.getColumnDetails(dtset,tab)        
            print(b.getCountOfNonNumericStructMembers(mytablst))
print("Test for getCountOfNonNumericStructMembers() succeeded.")
