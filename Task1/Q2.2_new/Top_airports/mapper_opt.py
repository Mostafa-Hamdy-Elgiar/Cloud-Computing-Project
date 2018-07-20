#!/usr/bin/python

import sys


for row in sys.stdin:
        row = row.strip()
        row_list = row.split(',')
        print row_list[3]+','+row_list[4] + ',' + row_list[6]
