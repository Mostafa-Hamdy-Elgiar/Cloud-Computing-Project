#!/usr/bin/python

from operator import itemgetter
import sys

count = 0
Carrier_count = 0
output_dic_1 = {}
output_dic_2 = {}
final_dic = {}
for row in sys.stdin:
        row = row.strip()
        Carrier , Dep_Delay = row.split(',')
	#print airport
	if Dep_Delay :
		Dep_Delay = float(Dep_Delay)
		#Carrier_count +=1
		output_dic_1[Carrier] = output_dic_1.get(Carrier , 0) + Dep_Delay
		#output_dic_2[Carrier] = output_dic_2.get(Carrier , 0) + Carrier_count

#for Carrier in output_dic_1.keys():
#	AVG_Dep_Delay = output_dic_1[Carrier]/output_dic_2[Carrier]
#	#print "%s: %s" % (Carrier , AVG_Dep_Delay)
#	final_dic[Carrier] = AVG_Dep_Delay

for key , value in sorted(output_dic_1.items(), key=itemgetter(1) , reverse=True):
	print "%s: %s" % (key , value)
	count += 1
	if count == 10:
		break
