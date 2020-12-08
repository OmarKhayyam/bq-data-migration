#!/usr/bin/env python

import sys
sys.path.insert(1, '../BQDetailsObject')

from BQDetailsObject import BQDetailsObject

b = BQDetailsObject()
print("Testing getValidationSample()...")
b.getValidationSample()
print("***#############################################***")
print("***### SHOWING VALIDATION SAMPLE COLUMN LIST ###***")
print("***#############################################***")
print(b.showValidationSample())
print("***################################***")
print("***### SHOWING FULL COLUMN LIST ###***")
print("***################################***")
print(b.showFullColumnList())
print("Test for getValidationSample() succeeded.")
