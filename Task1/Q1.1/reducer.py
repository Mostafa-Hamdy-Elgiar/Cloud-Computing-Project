#!/usr/bin/python

from operator import itemgetter 
import sys

count = 1
output_dic = {}
for row in sys.stdin:
	row = row.strip()
	airport , count = row.split(',')
	count = int(count)
	output_dic[airport] = output_dic.get(airport , 0) + count

for key, value in sorted(output_dic.iteritems(),key=itemgetter(1) , reverse=True):
	print "%s: %s" % (key , value)
	count += 1
	if count == 11:
		break
