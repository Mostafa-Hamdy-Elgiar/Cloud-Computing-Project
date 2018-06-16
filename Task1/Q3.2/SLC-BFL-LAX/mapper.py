#!/usr/bin/python
import sys

for row in sys.stdin:
        row = row.strip()
        row_list = row.split(',')
        Origin = row_list[3]
        if (row_list[3] == 'SLC' and row_list[4] == 'BFL' and row_list[0] == '2008-04-01') or (row_list[3] == 'BFL' and row_list[4] == 'LAX' and row_list[0] == '2008-04-03'):
                print Origin+'-'+row_list[4] +','+row_list[5] +','+row_list[8] +','+row_list[2]
