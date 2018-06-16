#!/usr/bin/python
import sys

for row in sys.stdin:
        row = row.strip()
        row_list = row.split(',')
        Origin = row_list[3]
        if (row_list[3] == 'JAX' and row_list[4] == 'DFW' and row_list[0] == '2008-09-09') or (row_list[3] == 'DFW' and row_list[4] == 'CRP' and row_list[0] == '2008-09-11'):
                print Origin+'-'+row_list[4] +','+row_list[5] +','+row_list[8] +','+row_list[2]
