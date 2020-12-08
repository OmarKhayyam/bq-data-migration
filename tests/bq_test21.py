#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDetailsObject')

from BQDetailsObject import BQDetailsObject

b = BQDetailsObject()
print("Testing getTableStats()...")
b.getTableStats()
print("Test for getTableStats() succeeded.")
