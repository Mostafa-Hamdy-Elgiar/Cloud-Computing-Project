#!/usr/bin/python

from operator import itemgetter
import sys

count = 0
output_dic = {}
for row in sys.stdin:
        row = row.strip()
        Carrier , Dep_Delay = row.split(',')
	if Dep_Delay :
		Dep_Delay = float(Dep_Delay)
		output_dic[Carrier] = output_dic.get(Carrier , 0) + Dep_Delay

for key , value in sorted(output_dic.items(), key=itemgetter(1) , reverse=True):
	print "%s: %s" % (key , value)
	count += 1
	if count == 10:
		break
