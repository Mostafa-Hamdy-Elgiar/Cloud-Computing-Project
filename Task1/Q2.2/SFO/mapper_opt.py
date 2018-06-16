#!/usr/bin/python

import sys

for row in sys.stdin:
        row = row.strip()
        row_list = row.split(',')
        if row_list[3] == 'SFO' :
                print row_list[4] + ',' + row_list[6]
