#!/usr/bin/python
import sys

for row in sys.stdin:
        row = row.strip()
        row_list = row.split(',')
        Origin = row_list[3]
        if (row_list[3] == 'CMI' and row_list[4] == 'ORD' and row_list[0] == '2008-03-04') or (row_list[3] == 'ORD' and row_list[4] == 'LAX' and row_list[0] == '2008-03-06'):
                print Origin+'-'+row_list[4] +','+row_list[5] +','+row_list[8] +','+row_list[2]
