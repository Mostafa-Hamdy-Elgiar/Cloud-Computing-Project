#!/usr/bin/python
import sys

for row in sys.stdin:
        row = row.strip()
        row_list = row.split(',')
        Origin = row_list[3]
        if (row_list[3] == 'LAX' and row_list[4] == 'SFO' and row_list[0] == '2008-07-12') or (row_list[3] == 'SFO' and row_list[4] == 'PHX' and row_list[0] == '2008-07-14'):
                print Origin+'-'+row_list[4] +','+row_list[5] +','+row_list[8] +','+row_list[2]
