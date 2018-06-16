#!/usr/bin/python

from operator import itemgetter
import sys

count = 0
Carrier_count = 0
output_dic = {}
output_dic_2 = {}
final_dic = {}
for row in sys.stdin:
        row = row.strip()
        Dest_airport , Dep_Delay = row.split(',')
	if Dep_Delay :
		Dep_Delay = float(Dep_Delay)
		output_dic[Dest_airport] = output_dic.get(Dest_airport , 0) + Dep_Delay


for key , value in sorted(output_dic.items(), key=itemgetter(1) , reverse=True):
	print "%s: %s" % (key , value)
	count += 1
	if count == 10:
		break
