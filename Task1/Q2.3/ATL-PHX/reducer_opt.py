#!/usr/bin/python

from operator import itemgetter
import sys

count = 0
output_dic = {}
output_dic_count = {}
final_dic = {}
for row in sys.stdin:
        row = row.strip()
        Carrier , Arr_Delay = row.split(',')
	if Arr_Delay :
		Arr_Delay = float(Arr_Delay)
		output_dic[Carrier] = output_dic.get(Carrier , 0) + Arr_Delay
		output_dic_count[Carrier]= output_dic_count.get(Carrier , 0) + 1


for Carrier in output_dic.keys():
	AVG_Arr_Delay = output_dic[Carrier] / output_dic_count[Carrier]
	final_dic[Carrier] = AVG_Arr_Delay

for key , value in sorted(final_dic.items(), key=itemgetter(1) , reverse=True):
	print "%s: %s" % (key , value)
	count += 1
	if count == 10:
		break
