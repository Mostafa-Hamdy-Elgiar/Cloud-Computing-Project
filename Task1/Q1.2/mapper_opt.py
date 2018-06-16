#!/usr/bin/python

import sys

for row in sys.stdin:
        row = row.strip()
        row_list = row.split(',')
        Carrier = row_list[1]
        ArrtimeDel = row_list[8]
        print Carrier+',' +str(ArrtimeDel)
