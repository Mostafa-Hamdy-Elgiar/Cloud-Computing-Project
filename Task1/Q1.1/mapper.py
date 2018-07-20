#!/usr/bin/python
import sys

for row in sys.stdin:
        row = row.strip()
        row_list = row.split(',')
        Origin = row_list[3]
        print Origin+',' +str(1)
        print Dest+','+str(1)
