#!/usr/bin/python

import sys


for row in sys.stdin:
        row = row.strip()
        row_list = row.split(',')
        if row_list[3] == 'CMI' and row_list[4] == 'ORD':
                print row_list[1] + ',' + row_list[8]
